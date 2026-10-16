package com.llmgw.observability;

import org.reactivestreams.Subscription;
import org.slf4j.MDC;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * 把 Reactor Context 里的 traceId 同步到 SLF4J MDC，让一次请求的多行日志能串起来。
 *
 * <h3>为什么 WebFlux 下 MDC 会"丢失"</h3>
 * MDC 是 {@link ThreadLocal} 实现的，而 {@code ThreadLocal} 的隐含前提是
 * <b>"一个请求 = 一个线程，从入口到出口不换人"</b>。WebFlux 把这个前提打破了：
 * <ul>
 *   <li>Netty 事件循环只有少量线程，却被成百上千个并发请求复用 ——
 *       在线程上 set 一个值，会被下一个请求的回调读到（<b>串味</b>）；</li>
 *   <li>一次请求的处理会跨越多个算子、可能切换到别的调度器 ——
 *       入口处 set 的 MDC 在后面的回调线程上根本不存在（<b>丢失</b>）。</li>
 * </ul>
 * 于是"在 Filter 里 MDC.put 一下"这个 Servlet 时代的标准动作，在响应式下
 * 要么不生效、要么把别人的 traceId 写进你的日志。两种都比没有更糟：
 * 前者让人以为有链路追踪，后者给出<b>错误的</b>链路。
 *
 * <h3>正确的位置：Reactor Context</h3>
 * Context 是随订阅链传播的（不绑定线程），所以它是响应式下携带请求级数据的正解。
 * 本类做的事情就是把 Context 里的值在<b>每次算子回调前</b>重新刷进 MDC ——
 * 因为回调运行在哪个线程是不确定的，唯一可靠的做法就是每次都用当前的 Context 覆盖。
 *
 * <p>实现方式遵循 reactor-core 官方 {@code MdcContextLifter} 模式：
 * 用一个 {@code CoreSubscriber} 装饰器包住每个算子，在 {@code onSubscribe} /
 * {@code onNext} / {@code onError} / {@code onComplete} 四个入口处同步 MDC。
 *
 * <h3>已知局限（不掩盖）</h3>
 * 同一线程上交错执行两个请求的回调时，MDC 的值会在两次回调之间被互相覆盖 ——
 * 若某段日志恰好在回调之外（例如异步任务的裸 lambda 里）打印，仍可能串味。
 * 彻底的解法是结构化日志（把 traceId 作为字段随日志事件传递，不依赖线程状态），
 * 那是日志框架层面的改造，不在本项目范围内。这里的做法解决了主链路
 * （绝大多数日志都发生在算子回调内）的串联问题。
 */
class MdcContextLifter<T> implements CoreSubscriber<T> {

    private final CoreSubscriber<T> delegate;

    MdcContextLifter(CoreSubscriber<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Context currentContext() {
        return delegate.currentContext();
    }

    @Override
    public void onSubscribe(Subscription s) {
        sync();
        delegate.onSubscribe(s);
    }

    @Override
    public void onNext(T t) {
        sync();
        delegate.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        sync();
        delegate.onError(t);
    }

    @Override
    public void onComplete() {
        sync();
        delegate.onComplete();
    }

    /**
     * 用当前 Context 覆盖 MDC。
     *
     * 没有 traceId 时是 remove 而不是保留旧值：否则会沿用到上一个请求的 traceId，
     * 那是最坏的情况 —— 日志里出现了<b>别人的</b> traceId，比空着更难排查。
     */
    private void sync() {
        String traceId = delegate.currentContext()
                .getOrDefault(TraceIdFilter.CONTEXT_KEY, "");
        if (traceId.isEmpty()) {
            MDC.remove(TraceIdFilter.CONTEXT_KEY);
        } else {
            MDC.put(TraceIdFilter.CONTEXT_KEY, traceId);
        }
    }
}
