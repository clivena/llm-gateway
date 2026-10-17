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
              (trace_id, api_key, feature, task, provider, model, cache_hit,
               prompt_tokens, completion_tokens, cost, latency_ms)
            VALUES
              (:traceId, :apiKey, :feature, :task, :provider, :model, :cacheHit,
               :promptTokens, :completionTokens, :cost, :latencyMs)
            """;

    /**
     * @param task 本次请求被判定成的任务（{@link com.llmgw.core.router.Route#task()}）。
     *             任务路由关闭时为 null —— 那表示"没有发生任务判定"，
     *             与"判定成 chat"是两件不同的事实，不能都用字符串占位符表示，
     *             否则按 task 分组统计时会出现一个假的 {@code n/a} 桶。
     * @param provider 实际服务这次请求的 provider。缓存命中为 "cache"。
     *                 它和 model 是两个维度：F2 之后同一个 model 可能在多家部署，
     *                 只记 model 时"备用链路接管之后质量/延迟有没有变"无法回答。
     */
    public Mono<Long> insert(String traceId, String apiKey, String feature, String task,
                             String provider, String model,
                             boolean cacheHit, int promptTokens, int completionTokens,
                             double cost, long latencyMs) {
        DatabaseClient.GenericExecuteSpec spec = db.sql(INSERT_SQL)
                .bind("traceId", traceId)
                .bind("apiKey", apiKey)
                // feature 历史上就是"空串代表没有"，保持不变（改它等于改既有账本语义）
                .bind("feature", feature == null ? "" : feature)
                .bind("model", model)
                .bind("cacheHit", cacheHit)
                .bind("promptTokens", promptTokens)
                .bind("completionTokens", completionTokens)
                .bind("cost", BigDecimal.valueOf(cost))
                .bind("latencyMs", (int) latencyMs);
        // task / provider 绑真正的 NULL 而不是空串：按任一维度分组统计时，
        // 空串会变成一个"叫空字符串的任务/提供方"的桶，而 NULL 才能被 GROUP BY 正确归组，
        // 也才能和"判定成 chat"区分开。
        spec = task == null ? spec.bindNull("task", String.class) : spec.bind("task", task);
        spec = provider == null ? spec.bindNull("provider", String.class)
                : spec.bind("provider", provider);
        return spec.fetch().rowsUpdated();
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

    /**
     * 按任务维度对账：每个任务花了多少。
     *
     * <p>这是 {@code task} 列真正的用处。没有它，只能从 {@code model} 反推
     * "当时大概走了什么任务" —— 而当多个任务映射到同一个模型时，
     * 反推直接失效，成本归因就没法回答"该砍哪个任务的预算"。
     *
     * <p>NULL 用 {@code (none)} 呈现：那表示任务路由关闭期间产生的账，
     * 与"判定成某个叫 none 的任务"是两件事（见 insert 的注释）。
     */
    public Flux<Map<String, Object>> summaryByTask() {
        return db.sql("""
                        SELECT COALESCE(task, '(none)') AS task,
                               count(*)                AS calls,
                               sum(prompt_tokens)      AS prompt_tokens,
                               sum(completion_tokens)  AS completion_tokens,
                               round(sum(cost), 6)     AS total_cost
                        FROM cost_ledger
                        GROUP BY COALESCE(task, '(none)')
                        ORDER BY total_cost DESC
                        """)
                .fetch().all();
    }

    /**
     * 按 provider 对账：哪家链路在承载成本。
     *
     * <p>与 {@link #summaryByTask()} 是同一条理由的两面：F2 之后同一个 model
     * 可能在多家部署，故障期最该能回答"备用那家接管了多少流量、花了多少钱"。
     * 只按 model 汇总时，这两家会被并成一行。
     */
    public Flux<Map<String, Object>> summaryByProvider() {
        return db.sql("""
                        SELECT COALESCE(provider, '(none)') AS provider,
                               count(*)                AS calls,
                               sum(prompt_tokens)      AS prompt_tokens,
                               sum(completion_tokens)  AS completion_tokens,
                               round(sum(cost), 6)     AS total_cost
                        FROM cost_ledger
                        GROUP BY COALESCE(provider, '(none)')
                        ORDER BY total_cost DESC
                        """)
                .fetch().all();
    }

    /** 明细，按时间倒序。 */
    public Flux<Map<String, Object>> recent(int limit) {
        return db.sql("""
                        SELECT trace_id, api_key, feature, task, provider, model, cache_hit,
                               prompt_tokens, completion_tokens, cost, latency_ms, created_at
                        FROM cost_ledger
                        ORDER BY id DESC
                        LIMIT :lim
                        """)
                .bind("lim", limit)
                .fetch().all();
    }
}
