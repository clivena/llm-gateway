package com.llmgw.observability;

import com.llmgw.core.protocol.UpstreamOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 关键指标。P7 补全 + F0 扩充后的全景：
 *
 * <pre>
 *   llmgw.cache.hit           缓存命中（含层归属见日志）
 *   llmgw.cache.miss          缓存未命中
 *   llmgw.cascade.escalate    级联升级次数
 *   llmgw.ledger.write        账本落库成功
 *   llmgw.ledger.failure      账本落库失败   ← 一涨就说明对账会缺数据
 *   llmgw.quota.rejected      配额拒绝（对应 429）
 *   llmgw.quota.settled       配额结算次数（reserve-then-settle 的第二段）
 *   llmgw.quota.settle.failed 配额结算失败   ← 一涨就说明配额会系统性失真
 *   llmgw.upstream.empty      上游返回空正文  ← F0 新增，见下
 *   llmgw.upstream.truncated  上游 finish_reason=length（预算耗尽被截断）
 *   llmgw.request.latency     延迟直方图，tag: model / stream
 * </pre>
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
}
