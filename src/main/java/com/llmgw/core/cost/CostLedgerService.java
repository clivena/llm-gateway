package com.llmgw.core.cost;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.config.GatewayProperties;
import com.llmgw.observability.GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 成本账本：每次调用写一条记录，用于报表、对账与成本归因。
 *
 * 落库方式由 gateway.cost.sink 决定：
 *   - postgres：R2DBC 直写 cost_ledger 表（MVP 闭环，立刻可查可对账）
 *   - kafka   ：发到 llm.cost.events，由下游消费者入仓（生产链路，解耦）
 *   - both    ：两条都走
 * 之所以做成可切换：生产上账本通常先进消息队列再入仓，但 MVP 阶段起一套
 * Kafka + 消费者成本太高，直写 Postgres 才能立刻拿到"可对账"的证据。
 *
 * 铁律：账本写失败绝不能影响用户请求 —— 所有异常只记日志/打点，不外抛。
 */
@Service
public class CostLedgerService {

    private static final Logger log = LoggerFactory.getLogger(CostLedgerService.class);
    public static final String TOPIC = "llm.cost.events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CostLedgerRepository repository;
    private final GatewayProperties properties;
    private final GatewayMetrics metrics;
    private final ObjectMapper mapper = new ObjectMapper();

    public CostLedgerService(KafkaTemplate<String, String> kafkaTemplate,
                             CostLedgerRepository repository,
                             GatewayProperties properties,
                             GatewayMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * 落一条账。
     *
     * @param feature 业务线（调用方自己标的，如 "resume-builder"）
     * @param task    网关判定的任务（code / json / summarize / chat…）。
     *                与 feature <b>语义正交</b>：feature 回答"哪个业务在花钱"，
     *                task 回答"这笔钱是按什么能力需求花出去的"。
     *                两者都为空是合法的（老客户端不传 feature、任务路由关闭）。
     * @param provider 实际服务这次请求的 provider 名（F2 之后同一 model 可能在多家部署，
     *                 只记 model 就分不清是谁答的）。缓存命中传 "cache"。
     */
    public void record(String traceId, String apiKey, String feature, String task, String provider,
                       String model, boolean cacheHit, int promptTokens, int completionTokens,
                       double cost, long latencyMs) {

        // trace_id 在表里是 NOT NULL：上游万一带了空值，这里必须兜住，
        // 否则整条 INSERT 会被数据库拒绝（而不是插入一条空 trace 的记录）。
        final String tid = (traceId == null || traceId.isBlank())
                ? "local-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16)
                : traceId;
        final String feat = feature == null ? "" : feature;

        if (properties.getCost().toPostgres()) {
            repository.insert(tid, apiKey, feat, task, provider, model, cacheHit,
                            promptTokens, completionTokens, cost, latencyMs)
                    .subscribe(
                            rows -> metrics.onLedgerWrite(),
                            ex -> {
                                metrics.onLedgerFailure();
                                log.warn("cost ledger postgres write failed: {}", ex.getMessage());
                            });
        }

        if (properties.getCost().toKafka()) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("traceId", tid);
            event.put("apiKey", apiKey);
            event.put("feature", feat);
            event.put("task", task);
            event.put("provider", provider);
            event.put("model", model);
            event.put("cacheHit", cacheHit);
            event.put("promptTokens", promptTokens);
            event.put("completionTokens", completionTokens);
            event.put("cost", cost);
            event.put("latencyMs", latencyMs);
            event.put("ts", System.currentTimeMillis());

            try {
                kafkaTemplate.send(TOPIC, apiKey, mapper.writeValueAsString(event))
                        .whenComplete((r, ex) -> {
                            if (ex != null) {
                                log.warn("cost event send failed: {}", ex.getMessage());
                            }
                        });
            } catch (Exception e) {
                log.warn("cost event build failed: {}", e.getMessage());
            }
        }
    }
}
