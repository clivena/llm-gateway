package com.llmgw.core.resilience;

import com.llmgw.config.GatewayProperties;
import com.llmgw.observability.GatewayMetrics;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * 上游调用的重试与熔断（F1）。
 *
 * <h3>为什么两个算子都要</h3>
 * 它们回答的是两个不同的问题，少一个都不成立：
 * <ul>
 *   <li><b>重试</b>问的是"这一次失败，立刻再试一次会不会好" —— 处理<b>瞬时</b>抖动。
 *       特点是它<b>还在花钱</b>：每次重试都是一次真实的上游调用。</li>
 *   <li><b>熔断</b>问的是"这条链路现在还值得试吗" —— 处理<b>持续</b>故障。
 *       价值恰恰在于<b>不再花钱</b>：上游整体挂了的时候，重试只是把同一个失败
 *       乘以 N，还把恢复期拖得更长。</li>
 * </ul>
 * 只有重试没有熔断 = 故障期自己给自己制造负载；只有熔断没有重试 = 一次抖动就报错。
 *
 * <h3>算子嵌套顺序：熔断在内、重试在外（与 resilience4j 注解的默认顺序一致）</h3>
 * <pre>
 *   call.timeout(…)                       ← 最内：超时也是失败，要进熔断窗口
 *        .transformDeferred(CircuitBreakerOperator.of(cb))
 *        .transformDeferred(RetryOperator.of(retry))
 * </pre>
 * 这个顺序有两处"反过来就错"：
 * <ol>
 *   <li><b>熔断必须能看到每一次尝试</b>。因为重试在<b>外</b>层，每次重试都是重新订阅
 *       内层，熔断器因此每次都拿到一个新的 Subscriber、每次都记一笔 ——
 *       "3 次尝试有 2 次失败"才是滑动窗口该看到的数据。若顺序反过来，
 *       一次逻辑请求无论内部重试几次都只记 1 笔，失败率会被系统性稀释，
 *       熔断器永远也到不了阈值。</li>
 *   <li><b>超时必须在内层</b>。放外层的话，一次 3 次重试的请求总耗时可以到
 *       3 × timeout，而"单次调用超时"和"整个请求超时"是两件事。</li>
 * </ol>
 *
 * <h3>为什么用 {@code transformDeferred} 而不是 {@code transform}</h3>
 * {@code transform} 在<b>装配期</b>就把算子装好，算子实例被所有订阅者共享；
 * {@code transformDeferred} 推迟到<b>订阅期</b>，每次订阅各自装一遍。
 * 重试的每一次都必须是独立的一次订阅，否则 resilience4j 的
 * CircuitBreakerSubscriber 会被多次订阅复用，状态直接错乱。
 *
 * <h3>为什么 Retry 是"每次请求新建"，而 CircuitBreaker 是"每个 provider 共享"</h3>
 * 这是本类里唯一一处刻意的不对称，理由是两者的状态归属不同：
 * <ul>
 *   <li>熔断器的状态（滑动窗口、失败率、开关）<b>属于 provider</b>，必须跨请求共享 ——
 *       一个进程每个请求一个熔断器，等于没有熔断器。</li>
 *   <li>重试的状态只是"这次请求已经试了几次"，<b>属于单次请求</b>；
 *       而且流式路径的重试谓词要读"这条流是否已经吐过字"这种请求级状态，
 *       共享实例做不到。{@code Retry} 只是个配置 + 计数器的轻量持有者，
 *       每个请求造一个的开销远小于它保护的这次 HTTP 调用。</li>
 * </ul>
 */
@Component
public class UpstreamResilience {

    private static final Logger log = LoggerFactory.getLogger(UpstreamResilience.class);

    /**
     * 重试谓词（白名单）。
     *
     * <p><b>为什么是白名单而不是黑名单</b>：黑名单（"除了这些以外都重试"）在遇到
     * 一个新的、没见过的异常类型时会<b>默认去重试它</b> —— 而"看不懂的错误"
     * 恰恰是最不该重试的那种：重试只会把它放大 N 倍，连同 N 倍的账单一起。
     * 白名单遇到未知异常时默认<b>不重试</b>，错也只是少救一次，
     * 而不是多花一次钱。
     */
    private static final Predicate<Throwable> RETRYABLE = UpstreamResilience::retryable;

    private final GatewayProperties properties;
    private final GatewayProperties.Resilience cfg;
    private final GatewayMetrics metrics;
    private final CircuitBreakerRegistry breakers;
    /** 已经为哪些 provider 注册过 Gauge / 订阅过状态迁移事件 —— 保证幂等。 */
    private final Set<String> prepared = ConcurrentHashMap.newKeySet();

    public UpstreamResilience(GatewayProperties properties, GatewayMetrics metrics) {
        this.properties = properties;
        this.metrics = metrics;
        this.cfg = properties.getResilience();
        var cb = cfg.getCircuitBreaker();
        this.breakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                // 按调用次数计的滑窗。选 COUNT_BASED 而不是 TIME_BASED：
                // 网关的调用量随时间波动极大（白天夜里差几个量级），
                // 时间窗在低峰期会"样本不足"，高峰期又反应过快。
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(cb.getSlidingWindowSize())
                .minimumNumberOfCalls(cb.getMinimumNumberOfCalls())
                .failureRateThreshold(cb.getFailureRateThreshold())
                .waitDurationInOpenState(Duration.ofMillis(cb.getWaitDurationInOpenStateMillis()))
                .permittedNumberOfCallsInHalfOpenState(cb.getPermittedNumberOfCallsInHalfOpenState())
                // 自动从 OPEN 翻到 HALF_OPEN。开着它，/admin/circuit 才能在没人打请求时
                // 也如实反映"等待期已过、正在试探"；关掉的话状态只在下次调用时才推进，
                // 面板会一直显示 OPEN，看起来像永远不恢复。
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
    }

    /**
     * 非流式：把一次上游调用包上超时 → 熔断 → 重试，并把"熔断打开"翻译成本项目的异常类型。
     *
     * <p>调用方若要把"空正文"也纳入重试，必须在传进来之前先把它转成
     * {@link UpstreamEmptyResponseException}（见 ChatPipeline），因为重试算子只认错误信号。
     */
    public <T> Mono<T> protect(String provider, Mono<T> call) {
        Mono<T> decorated = call.timeout(Duration.ofMillis(cfg.getTimeoutMillis()));
        if (cfg.getCircuitBreaker().isEnabled()) {
            decorated = decorated.transformDeferred(CircuitBreakerOperator.of(breakerFor(provider)));
        }
        if (cfg.getRetry().isEnabled()) {
            decorated = decorated.transformDeferred(RetryOperator.of(newRetry(provider, RETRYABLE)));
        }
        // 翻译放在最外层：重试谓词要拿到原始的 CallNotPermittedException 才能判断
        // "这个不该重试"，所以它必须在内层就确定下来，不能被提前替换掉。
        return decorated.onErrorMap(CallNotPermittedException.class, e -> circuitOpen(provider, e));
    }

    /**
     * 流式：与非流式同样的嵌套，但重试多一道闸门 —— <b>只有"一个分片都还没吐出去"时才允许重试</b>。
     *
     * <h3>为什么流式必须额外加这道闸门</h3>
     * 重试的语义是"重新订阅上游、从头再放一遍"。非流式没问题：客户端还没拿到任何东西。
     * 但流式一旦已经推出去几个字，重放会让客户端看到<b>两段拼接的答案</b> ——
     * 前一次成功的半截 + 这一次完整的一遍。这种损坏是不可撤销的，
     * 而且看起来像模型在胡说，排查时完全指不到重试头上。
     *
     * <p>这也正是"首片之前才可重试"这条边界的实现位置：首片超时
     * （{@code gateway.resilience.first-chunk-timeout-millis}）定义了窗口的时间上界，
     * 本闸门定义了它的数据上界。
     *
     * @param upstreamProducedSomething 由本方法在<b>重试边界下游</b>写入。
     *       入参用 AtomicBoolean 而不是内部新建，是为了让调用方（ChatPipeline）
     *       在流结束后还能读到"这次到底有没有数据出来过"——
     *       它决定了流断掉时配额该不该退还。
     *       传 {@code true} 之后就不再回退，因此调用方复用它做判断是安全的。
     */
    public <T> Flux<T> protectStream(String provider, Flux<T> call,
                                     AtomicBoolean upstreamProducedSomething) {
        // Flux.timeout(Duration) 的作用域就是**首元素**：后续元素不受它约束。
        // 所以这一行直接就是"首片超时"，不需要自己写计时器 ——
        // 参数语义与算子语义天然对齐，是选它而不是选 Mono 侧 timeout 的原因。
        Flux<T> decorated = call.timeout(Duration.ofMillis(cfg.getFirstChunkTimeoutMillis()));
        if (cfg.getCircuitBreaker().isEnabled()) {
            decorated = decorated.transformDeferred(CircuitBreakerOperator.of(breakerFor(provider)));
        }
        if (cfg.getRetry().isEnabled()) {
            // 闸门读的是"下游是否已经收到过分片"。注意上游产出 ≠ 下游收到：
            // 标记挂在重试边界**之外**，被重试丢掉的那次尝试所产出的分片不会留下痕迹。
            Predicate<Throwable> gated = RETRYABLE.and(e -> !upstreamProducedSomething.get());
            decorated = decorated.transformDeferred(RetryOperator.of(newRetry(provider, gated)));
        }
        return decorated
                // ★ 位置关键：必须在 RetryOperator 之后。
                // 放前面的话，一次"吐了 3 个分片再报错"的尝试也会把标记置上 ——
                // 而那 3 个分片可能已经被随后的重试判定为丢弃，客户端其实什么都没看到。
                .doOnNext(x -> upstreamProducedSomething.set(true))
                .onErrorMap(CallNotPermittedException.class, e -> circuitOpen(provider, e));
    }

    /** 某个 provider 的熔断状态。测试与面板用；F2 的故障转移也要靠它挑下一个候选。 */
    public CircuitBreaker.State stateOf(String provider) {
        return breakerFor(provider).getState();
    }

    /** 各 provider 的熔断状态快照，给 /admin/circuit 用。 */
    public Map<String, Object> states() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("retryEnabled", cfg.getRetry().isEnabled());
        out.put("retryMaxAttempts", cfg.getRetry().getMaxAttempts());
        out.put("circuitBreakerEnabled", cfg.getCircuitBreaker().isEnabled());
        out.put("failureRateThreshold", cfg.getCircuitBreaker().getFailureRateThreshold());
        out.put("minimumNumberOfCalls", cfg.getCircuitBreaker().getMinimumNumberOfCalls());

        Map<String, Object> perProvider = new LinkedHashMap<>();
        // 遍历**配置里的** provider 而不是 breakers.getAllCircuitBreakers()：
        // 熔断器是懒创建的，从没被调用过的 provider 不会出现在 Registry 里 ——
        // 而面板最需要看到的恰恰是"这个 provider 一次请求都没成过"。
        for (GatewayProperties.Provider p : properties.getProviders()) {
            CircuitBreaker b = breakerFor(p.getName());
            CircuitBreaker.Metrics m = b.getMetrics();
            Map<String, Object> one = new LinkedHashMap<>();
            one.put("state", b.getState().name());
            one.put("failureRate", m.getFailureRate());
            one.put("bufferedCalls", m.getNumberOfBufferedCalls());
            one.put("failedCalls", m.getNumberOfFailedCalls());
            one.put("successfulCalls", m.getNumberOfSuccessfulCalls());
            one.put("notPermittedCalls", m.getNumberOfNotPermittedCalls());
            perProvider.put(p.getName(), one);
        }
        out.put("providers", perProvider);
        return out;
    }

    private CircuitBreaker breakerFor(String provider) {
        CircuitBreaker breaker = breakers.circuitBreaker(provider);
        if (prepared.add(provider)) {
            metrics.registerCircuitGauge(provider,
                    () -> breaker.getState() == CircuitBreaker.State.OPEN
                            || breaker.getState() == CircuitBreaker.State.FORCED_OPEN ? 1 : 0);
            // 状态迁移必须打日志。"熔断什么时候打开的、什么时候恢复的"是故障复盘里
            // 第一个要看的时间线，而指标只告诉你"现在是开还是关"、给不出历史。
            breaker.getEventPublisher().onStateTransition(ev -> log.warn(
                    "circuit breaker state transition [provider={}] {} -> {}",
                    provider, ev.getStateTransition().getFromState(),
                    ev.getStateTransition().getToState()));
        }
        return breaker;
    }

    /**
     * 为单次请求造一个 Retry。谓词由调用方给 —— 流式路径要在基础谓词上再叠一道闸门。
     */
    private Retry newRetry(String provider, Predicate<Throwable> predicate) {
        var r = cfg.getRetry();
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(Math.max(1, r.getMaxAttempts()))
                .intervalFunction(IntervalFunction.ofExponentialBackoff(
                        Duration.ofMillis(r.getWaitDurationMillis()),
                        r.getBackoffMultiplier(),
                        Duration.ofMillis(r.getMaxWaitDurationMillis())))
                .retryOnException(predicate)
                // 显式写 false（也是默认值），因为 true 会让重试耗尽后抛出
                // MaxRetriesExceededException 而不是原始异常 —— 那样上层就再也
                // 拿不到 UpstreamEmptyResponseException 身上那份原始响应，
                // F0 的 finish_reason 透传会被这个开关悄悄破坏掉。
                .failAfterMaxAttempts(false)
                .build();

        Retry retry = Retry.of(provider, config);
        retry.getEventPublisher().onRetry(ev -> {
            metrics.onUpstreamRetry(provider);
            log.warn("retrying upstream call [provider={} attempt={}]: {}",
                    provider, ev.getNumberOfRetryAttempts(), describe(ev.getLastThrowable()));
        });
        return retry;
    }

    private UpstreamCircuitOpenException circuitOpen(String provider, CallNotPermittedException cause) {
        metrics.onCircuitRejected(provider);
        log.warn("circuit breaker open, request not sent upstream [provider={}]", provider);
        return new UpstreamCircuitOpenException(provider, cause);
    }

    /**
     * 哪些错误值得"立刻再试一次"。
     *
     * <p>判断标准只有一条：<b>再试一次的结果是否可能不同</b>。
     * 与错误"严重不严重"无关 —— 一次 400 很轻，但它是确定性的，重试毫无意义；
     * 一次 429 看起来很糟糕，但它恰恰是重试最该处理的信号。
     */
    static boolean retryable(Throwable e) {
        if (e == null) {
            return false;
        }
        if (e instanceof UpstreamEmptyResponseException) {
            // 上游 200 但正文是空的（F0 的 empty，推理型模型把 max_tokens 预算花在
            // reasoning 上）。值得重试：推理 token 数高度不确定，同一次提问重跑一遍
            // 完全可能正常产出（实测同一个问题在 512/1024/1935/1740 之间跳）。
            return true;
        }
        if (e instanceof UpstreamEmptyBodyException) {
            // 连响应体都没有。也是瞬时故障，且没有任何内容已经交给客户端，
            // 重试的代价只有钱。
            return true;
        }
        if (e instanceof WebClientResponseException w) {
            if (w.getStatusCode().is5xxServerError()) {
                return true;      // 上游自己出问题了
            }
            // 429 是唯一一个"重试就是正确响应"的 4xx：它不是请求错了，
            // 是"现在别来"。注意当前实现还没有读 Retry-After 头，
            // 退避时间走的是统一的指数退避 —— 上游若给了很长的 Retry-After，
            // 我们会在它说的窗口之前就重试（见 docs/step-19 的已知边界）。
            return w.getStatusCode().value() == 429;
        }
        if (e instanceof WebClientRequestException) {
            return true;          // 连不上 / 连接被重置 / DNS 失败，典型瞬时故障
        }
        if (e instanceof TimeoutException) {
            return true;          // 首片超时（Flux.timeout）或上游迟迟不返回
        }
        // 白名单之外一律不重试。特别注意两个**故意不重试**的：
        //   - CallNotPermittedException：熔断已打开。这个异常在重试边界**内层**产生，
        //     所以谓词看到的还是它的原样（onErrorMap 在更外层，还没轮到它出手）。
        //     重试也进不去熔断器，只会空转并把重试预算烧光。
        //   - IllegalArgumentException：请求本身不合法（messages 为空），重试结果一样。
        //   - 其余 4xx：模型名不存在、参数非法、鉴权失败 —— 全是确定性的。
        return false;
    }

    private static String describe(Throwable e) {
        if (e == null) {
            return "unknown";
        }
        if (e instanceof WebClientResponseException w) {
            return "HTTP " + w.getStatusCode().value();
        }
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
