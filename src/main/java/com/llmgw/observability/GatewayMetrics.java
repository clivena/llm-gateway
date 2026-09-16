package com.llmgw.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * 关键指标：缓存命中/未命中、上游失败、级联升级、账本落库成功/失败。
 * 账本两个计数器是"钱有没有记对"的直接信号：failure 一涨就说明对账会缺数据。
 */
@Component
public class GatewayMetrics {

    private final Counter cacheHit;
    private final Counter cacheMiss;
    private final Counter escalate;
    private final Counter ledgerWrite;
    private final Counter ledgerFailure;

    public GatewayMetrics(MeterRegistry registry) {
        this.cacheHit = Counter.builder("llmgw.cache.hit").register(registry);
        this.cacheMiss = Counter.builder("llmgw.cache.miss").register(registry);
        this.escalate = Counter.builder("llmgw.cascade.escalate").register(registry);
        this.ledgerWrite = Counter.builder("llmgw.ledger.write").register(registry);
        this.ledgerFailure = Counter.builder("llmgw.ledger.failure").register(registry);
    }

    public void onCacheHit() { cacheHit.increment(); }
    public void onCacheMiss() { cacheMiss.increment(); }
    public void onEscalate() { escalate.increment(); }
    public void onLedgerWrite() { ledgerWrite.increment(); }
    public void onLedgerFailure() { ledgerFailure.increment(); }
}
