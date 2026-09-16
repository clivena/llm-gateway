package com.llmgw.core.cost;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 成本账本落库（Postgres / R2DBC）。
 *
 * 为什么用 R2DBC 而不是 JDBC：网关整条链路是 WebFlux 非阻塞的，
 * 用 JDBC 会在连接等待时占住线程、与"落账不能拖响应"的目标相冲突。
 * R2DBC 全程非阻塞，配合连接池把写账本的成本压到最低。
 *
 * 注意：本类只负责失败上报，异常处理在 CostLedgerService —— 那里会
 * onErrorResume 兜住，保证"账本写不进去"绝不会冒泡到用户请求。
 */
@Component
public class CostLedgerRepository {

    private final DatabaseClient db;

    public CostLedgerRepository(DatabaseClient db) {
        this.db = db;
    }

    private static final String INSERT_SQL = """
            INSERT INTO cost_ledger
              (trace_id, api_key, feature, model, cache_hit,
               prompt_tokens, completion_tokens, cost, latency_ms)
            VALUES
              (:traceId, :apiKey, :feature, :model, :cacheHit,
               :promptTokens, :completionTokens, :cost, :latencyMs)
            """;

    public Mono<Long> insert(String traceId, String apiKey, String feature, String model,
                             boolean cacheHit, int promptTokens, int completionTokens,
                             double cost, long latencyMs) {
        return db.sql(INSERT_SQL)
                .bind("traceId", traceId)
                .bind("apiKey", apiKey)
                // feature 允许为空 -> 必须显式绑 null，否则 SQL 里的 :feature 无法解析
                .bind("feature", feature == null ? "" : feature)
                .bind("model", model)
                .bind("cacheHit", cacheHit)
                .bind("promptTokens", promptTokens)
                .bind("completionTokens", completionTokens)
                .bind("cost", BigDecimal.valueOf(cost))
                .bind("latencyMs", (int) latencyMs)
                .fetch()
                .rowsUpdated();
    }

    /** 对账用：按租户汇总。 */
    public Flux<Map<String, Object>> summaryByApiKey() {
        return db.sql("""
                        SELECT api_key,
                               count(*)              AS calls,
                               sum(prompt_tokens)    AS prompt_tokens,
                               sum(completion_tokens) AS completion_tokens,
                               round(sum(cost), 6)   AS total_cost
                        FROM cost_ledger
                        GROUP BY api_key
                        ORDER BY total_cost DESC
                        """)
                .fetch().all();
    }

    /** 明细，按时间倒序。 */
    public Flux<Map<String, Object>> recent(int limit) {
        return db.sql("""
                        SELECT trace_id, api_key, feature, model, cache_hit,
                               prompt_tokens, completion_tokens, cost, latency_ms, created_at
                        FROM cost_ledger
                        ORDER BY id DESC
                        LIMIT :lim
                        """)
                .bind("lim", limit)
                .fetch().all();
    }
}
