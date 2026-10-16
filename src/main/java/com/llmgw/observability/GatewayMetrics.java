package com.llmgw.observability;

import com.llmgw.core.protocol.UpstreamOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * 关键指标。P7 补全 + F0 扩充 + F1 扩充后的全景：
 *
 * <pre>
 *   llmgw.cache.hit                 缓存命中（含层归属见日志）
 *   llmgw.cache.miss                缓存未命中
 *   llmgw.cascade.escalate          级联升级次数
 *   llmgw.ledger.write              账本落库成功
 *   llmgw.ledger.failure            账本落库失败   ← 一涨就说明对账会缺数据
 *   llmgw.quota.rejected            配额拒绝（对应 429）
 *   llmgw.quota.settled             配额结算次数（reserve-then-settle 的第二段）
 *   llmgw.quota.settle.failed       配额结算失败   ← 一涨就说明配额会系统性失真
 *   llmgw.upstream.empty            上游返回空正文  ← F0 新增，见下
 *   llmgw.upstream.truncated        上游 finish_reason=length（预算耗尽被截断）
 *   llmgw.upstream.retry            上游调用被放弃并重试  ← F1 新增，tag: provider
 *   llmgw.circuit.open              熔断器是否打开 0/1     ← F1 新增，tag: provider（Gauge）
 *   llmgw.circuit.rejected          因熔断被拦下的请求     ← F1 新增，tag: provider
 *   llmgw.request.latency           延迟直方图，tag: model / stream
 * </pre>
 *
 * <h3>为什么 F1 的三个指标都必须带 provider 标签</h3>
 * 熔断与重试的价值全在"哪条链路不健康"这个判断上。把三个 provider 的重试次数
 * 加成一个总数，等于把"1 家挂了"和"3 家都在轻微抖动"混成同一个数字 ——
 * 而这两种情况的处置完全相反。provider 数量是个位数到几十，基数完全可控。
 *
 * <h3>为什么 empty 与 truncated 必须分开计数（F0）</h3>
 * 两者会同时出现（推理型模型的典型失败形态），但**单独出现时含义完全相反**：
 * <ul>
 *   <li>{@code empty} 单独涨 → 上游或模型行为异常，是**故障信号**，该告警。</li>
 *   <li>{@code truncated} 单独涨 → 客户端把 {@code max_tokens} 设小了，
 *       是**用量信号**，该去联系调用方而不是查上游。</li>
 * </ul>
 * 合在一个计数器里，这两种情况就永远分不开 —— 而它们的处置动作完全不同。
 *
 * <h3>为什么必须补延迟指标（P7）</h3>
 * 补之前，延迟<b>只能从 Postgres 账本里查</b>。这在两处致命：
 * ① 账本写失败（Kafka 不可达、Postgres 抖动）时指标完全瞎掉 —— 而恰恰是
 * 系统出问题的时候最需要看延迟；② 面板要查库才能画图，把只读面板变成了数据库负载。
 * 计时器随进程存活（Prometheus 抓取），与账本这条异步落库链路彻底解耦。
 *
 * <h3>为什么区分 stream</h3>
 * 流式与非流式的"延迟"不是同一个东西：非流式的延迟是<b>全部</b>生成时间，
 * 流式真正决定体验的是<b>首字延迟</b>（本类记的是整体完成时间，因为首字延迟
 * 需要在上游分片流里打点，属另一个改动）。混在一个直方图里，
 * 两种模式的 P99 会互相污染 —— 看到 P99 变差时无法判断是哪条路径退化了。
 *
 * <p>基数控制：model 取值来自配置（几个到几十个），stream 是布尔，
 * 组合数可控，不会撑爆时序库。
 */
@Component
public class GatewayMetrics {

    private final MeterRegistry registry;
    private final Counter cacheHit;
    private final Counter cacheMiss;
    private final Counter escalate;
    private final Counter ledgerWrite;
    private final Counter ledgerFailure;
    private final Counter quotaRejected;
    private final Counter quotaSettled;
    private final Counter quotaSettleFailed;
    private final Counter upstreamEmpty;
    private final Counter upstreamTruncated;
    /**
     * 按 provider 打标的两组计数器，**懒创建 + 缓存**。
     *
     * <p>Micrometer 的 Counter.builder().tag(...).register() 每次调用都会做一次
     * 名字+标签的查表匹配；放在每个请求的热路径上反复调用虽然正确，但纯属浪费。
     * 缓存之后每个 provider 只走一次构建。
     *
     * <p>用 ConcurrentHashMap 而不是 HashMap：这些方法会被多个 Netty 事件循环
     * 线程并发调用（这正是网关的常态），而"指标本身把网关搞崩"是最讽刺的故障。
     */
    private final ConcurrentMap<String, Counter> upstreamRetryByProvider = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> circuitRejectedByProvider = new ConcurrentHashMap<>();

    public GatewayMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.cacheHit = Counter.builder("llmgw.cache.hit").register(registry);
        this.cacheMiss = Counter.builder("llmgw.cache.miss").register(registry);
        this.escalate = Counter.builder("llmgw.cascade.escalate").register(registry);
        this.ledgerWrite = Counter.builder("llmgw.ledger.write").register(registry);
        this.ledgerFailure = Counter.builder("llmgw.ledger.failure").register(registry);
        // 配额拒绝独立计数：/admin/status 与日志里能看到，但两者都不是可告警的时序数据。
        // "429 占比突然上升"是最该被自动发现的信号之一（要么有人在刷，要么额度设小了）。
        this.quotaRejected = Counter.builder("llmgw.quota.rejected").register(registry);
        this.quotaSettled = Counter.builder("llmgw.quota.settled").register(registry);
        this.quotaSettleFailed = Counter.builder("llmgw.quota.settle.failed").register(registry);
        this.upstreamEmpty = Counter.builder("llmgw.upstream.empty").register(registry);
        this.upstreamTruncated = Counter.builder("llmgw.upstream.truncated").register(registry);
    }

    public void onCacheHit() { cacheHit.increment(); }
    public void onCacheMiss() { cacheMiss.increment(); }
    public void onEscalate() { escalate.increment(); }
    public void onLedgerWrite() { ledgerWrite.increment(); }
    public void onLedgerFailure() { ledgerFailure.increment(); }
    public void onQuotaRejected() { quotaRejected.increment(); }
    public void onQuotaSettled() { quotaSettled.increment(); }
    public void onQuotaSettleFailed() { quotaSettleFailed.increment(); }

    /**
     * 记一次上游响应的可用性。两个计数器各自独立递增 —— 两者都成立时
     * 两个都加，因为它们表达的是两个不同的事实（"什么都没给" + "还被截断了"）。
     */
    public void onUpstreamOutcome(UpstreamOutcome outcome) {
        if (outcome == null) {
            return;
        }
        if (outcome.empty()) {
            upstreamEmpty.increment();
        }
        if (outcome.truncated()) {
            upstreamTruncated.increment();
        }
    }

    /**
     * 记录一次请求的端到端耗时。
     *
     * @param model  实际服务的模型名；缓存命中时传 "cache"（那是快路径，
     *               与任何模型的分布都不该混在一起）
     * @param stream 是否流式
     * @param millis 耗时
     */
    public void recordLatency(String model, boolean stream, long millis) {
        Timer.builder("llmgw.request.latency")
                .tag("model", model == null ? "unknown" : model)
                .tag("stream", String.valueOf(stream))
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(Duration.ofMillis(Math.max(0, millis)));
    }

    /** 一次被放弃的尝试（紧接着会重试）。它<b>不等于</b>一次失败请求：多数会重试成功。 */
    public void onUpstreamRetry(String provider) {
        upstreamRetryByProvider
                .computeIfAbsent(provider, p -> Counter.builder("llmgw.upstream.retry")
                        .tag("provider", p)
                        .register(registry))
                .increment();
    }

    /** 因熔断打开而被直接拦下、根本没发给上游的请求（对应 503）。 */
    public void onCircuitRejected(String provider) {
        circuitRejectedByProvider
                .computeIfAbsent(provider, p -> Counter.builder("llmgw.circuit.rejected")
                        .tag("provider", p)
                        .register(registry))
                .increment();
    }

    /**
     * 注册"熔断器是否打开"的 Gauge（每个 provider 一个）。
     *
     * <p>为什么是 Gauge 而不是 Counter：熔断器的状态是<b>当前值</b>而不是累计量 ——
     * "现在开着吗"才是告警要问的问题。用 Counter 表达会需要在状态迁移时
     * 加/减，一旦漏掉一次迁移就永久偏了，而且没人能发现。
     * Gauge 每次抓取时现读熔断器，永远不会漂。
     *
     * @param stateSupplier 返回 1 表示打开（OPEN / FORCED_OPEN），0 表示可以通过
     */
    public void registerCircuitGauge(String provider, Supplier<Number> stateSupplier) {
        Gauge.builder("llmgw.circuit.open", stateSupplier)
                .tag("provider", provider)
                .description("1 when the provider's circuit breaker is open")
                .register(registry);
    }
}
