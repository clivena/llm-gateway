package com.llmgw.api;

import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cost.CostLedgerRepository;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.RouterService;
import com.llmgw.observability.GatewayMetrics;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 只读运维接口，给 dashboard 提供真实数据。
 *
 * 设计约束：
 * 1. 全部只读——不扣配额、不写账本、不改配置，因此不会影响 /v1 主链路的任何语义；
 * 2. 与对话链路完全隔离——这个 Controller 挂了不影响网关对外服务（反过来也一样：
 *    Redis/Postgres 挂了这里会如实报错，前端据此降级到内置样例数据，而不是显示一个"看起来正常的空表"）。
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final CostLedgerRepository ledger;
    private final QuotaService quota;
    private final GatewayProperties properties;
    private final DatabaseClient db;
    private final UpstreamResilience resilience;
    private final RouterService router;
    private final GatewayMetrics metrics;

    public AdminController(CostLedgerRepository ledger, QuotaService quota,
                           GatewayProperties properties, DatabaseClient db,
                           UpstreamResilience resilience, RouterService router,
                           GatewayMetrics metrics) {
        this.ledger = ledger;
        this.quota = quota;
        this.properties = properties;
        this.db = db;
        this.resilience = resilience;
        this.router = router;
        this.metrics = metrics;
    }

    /** 账本明细，按 id 倒序。limit 收口在 1~200，避免被当成"全表导出"接口用。 */
    @GetMapping("/ledger/recent")
    public Flux<Map<String, Object>> recent(@RequestParam(defaultValue = "20") int limit) {
        return ledger.recent(Math.min(Math.max(limit, 1), 200));
    }

    /** 账本按租户汇总（对账口径）。 */
    @GetMapping("/ledger/summary")
    public Flux<Map<String, Object>> summary() {
        return ledger.summaryByApiKey();
    }

    /**
     * 账本按任务汇总：哪个任务在花钱、花了多少。
     *
     * <p>它是 {@code task} 列真正被用到的地方。只有明细里有这一列是不够的 ——
     * 排障时的问题是"code 任务这周的成本是不是涨了"，那需要一个聚合视图，
     * 而不是让人把明细导出去自己按列求和。
     */
    @GetMapping("/ledger/by-task")
    public Flux<Map<String, Object>> byTask() {
        return ledger.summaryByTask();
    }

    /**
     * 账本按 provider 汇总：哪条链路在承载成本。
     *
     * <p>它是 {@code provider} 列真正被用到的地方。F2 之后同一个 model 可能在
     * 多家部署，"备用那家接管了多少流量、花了多少钱"只按 model 汇总时会被并成一行。
     */
    @GetMapping("/ledger/by-provider")
    public Flux<Map<String, Object>> byProvider() {
        return ledger.summaryByProvider();
    }

    /**
     * 当前生效的任务路由：映射表 + 本进程的命中分布。
     *
     * <p>这个端点的存在理由与 {@code /admin/circuit} 相同：排障时人问的是
     * <b>带上下文</b>的问题 —— "code 现在配到哪个模型"、"最近这些请求是走
     * 显式声明的还是网关猜的"。配置文件和日志各能回答一半，合起来才是完整答案，
     * 而让人自己去 grep 日志就是在惩罚排障的人。
     *
     * <p>三个字段必须一起看：
     * <ul>
     *   <li>{@code tasks} 是从配置反查候选表得到的<b>真实档位</b>（含 qualityScore），
     *       与运行时用的完全是同一份 —— 它不是把配置原样回显。</li>
     *   <li>{@code hits} 是<b>进程启动以来</b>的计数，重启归零；跨重启看 Prometheus。</li>
     *   <li>{@code source} 的分布能回答"规则是不是太激进"：推断占比高且集中在
     *       某一个任务，通常说明那条规则命中面过宽。</li>
     * </ul>
     *
     * <p>只读，不触发任何上游调用，也不改写任何状态。
     */
    @GetMapping("/route")
    public Mono<Map<String, Object>> route() {
        GatewayProperties.TaskRouting tc = properties.getRouter().getTaskRouting();

        Map<String, Object> tasks = new LinkedHashMap<>();
        for (Map.Entry<String, ModelTier> e : router.taskTable().entrySet()) {
            ModelTier tier = e.getValue();
            Map<String, Object> one = new LinkedHashMap<>();
            one.put("model", tier.model());
            one.put("tier", tier.tier());
            one.put("costPer1k", tier.costPer1k());
            one.put("qualityScore", tier.qualityScore());
            tasks.put(e.getKey(), one);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", tc.isEnabled());
        out.put("defaultTask", tc.getDefaultTask());
        out.put("classifier", tc.getClassifier());
        out.put("policy", properties.getRouter().getPolicy());
        out.put("tasks", tasks);
        out.put("hits", metrics.routeCountsByTask());
        // 口径写在响应里，而不是只写在文档里：拿到这份数据的人未必读过 README，
        // 而"这个数是累计还是当前"直接决定他会不会用错。
        out.put("hitsScope", "since-process-start");
        return Mono.just(out);
    }

    /** 指定租户当前周期的配额使用情况。 */
    @GetMapping("/quota/{apiKey}")
    public Mono<Map<String, Object>> quotaOf(@PathVariable String apiKey) {
        return quota.currentUsage(apiKey);
    }

    /** 当前生效的策略与单价表——面板上"为什么是这个价"的依据。 */
    @GetMapping("/pricing")
    public Mono<Map<String, Object>> pricing() {
        Map<String, Object> prices = new LinkedHashMap<>();
        properties.getPricing().forEach((model, p) -> {
            Map<String, Object> one = new LinkedHashMap<>();
            one.put("inputPer1k", p.getInputPer1k());
            one.put("outputPer1k", p.getOutputPer1k());
            prices.put(model, one);
        });

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pricing", prices);
        out.put("routerPolicy", properties.getRouter().getPolicy());
        // 缓存拆成三级信息暴露：L1/L2 各自都能独立关闭（L2 在无 embedding 的供应商下
        // 开着只是空转），面板只报一个 "cacheEnabled" 会让人误判"缓存开着=会命中"。
        out.put("cacheEnabled", properties.getCache().isEnabled());
        out.put("cacheExactEnabled", properties.getCache().isExactEnabled());
        out.put("cacheSemanticEnabled", properties.getCache().isSemanticEnabled());
        out.put("similarityThreshold", properties.getCache().getSimilarityThreshold());
        out.put("costSink", properties.getCost().getSink());
        out.put("quotaLimitTokens", properties.getQuota().getDefaultLimitTokens());

        // 当前生效的 token 估算系数。暴露出来的理由不是"面板要展示"，
        // 而是让校准脚本（scripts/calibrate-token-estimator.py）能读到**实际生效**的值
        // 去校验预测精度 —— 否则脚本只能硬编码一份副本，配置一改就悄悄测错。
        Map<String, Object> tokenEstimate = new LinkedHashMap<>();
        tokenEstimate.put("baseOverhead", properties.getTokenEstimate().getBaseOverhead());
        tokenEstimate.put("cjkPerChar", properties.getTokenEstimate().getCjkPerChar());
        tokenEstimate.put("asciiPerChar", properties.getTokenEstimate().getAsciiPerChar());
        tokenEstimate.put("perMessageOverhead",
                properties.getTokenEstimate().getPerMessageOverhead());
        tokenEstimate.put("safetyFactor", properties.getTokenEstimate().getSafetyFactor());
        out.put("tokenEstimate", tokenEstimate);
        return Mono.just(out);
    }

    /**
     * 各 provider 的重试 / 熔断状态（F1）。
     *
     * <p>为什么要单独开一个端点而不是只靠 Prometheus：熔断状态需要
     * "现在开着吗 + 为什么（失败率多少、窗口里几个样本）"这种<b>带上下文</b>的读法，
     * 面板上光看到一个 0/1 的 Gauge 是没法判断"该不该人工干预"的。
     * 而且故障期人第一个会打开的就是这个页面 —— 那时候不该让他去拼 PromQL。
     *
     * <p>与 /admin/* 的其余接口一样：只读，不触发任何上游调用。
     * 注意它读的是熔断器的**当前状态**（含半开时的实时计数），不是快照历史。
     */
    @GetMapping("/circuit")
    public Mono<Map<String, Object>> circuit() {
        return Mono.just(resilience.states());
    }

    /**
     * 依赖健康检查。两项探测都 onErrorResume 成 false——
     * 面板的意义是"告诉人哪里坏了"，而不是自己跟着一起 500。
     */
    @GetMapping("/status")
    public Mono<Map<String, Object>> status() {
        Mono<Boolean> redisOk = quota.ping().onErrorResume(e -> Mono.just(false));
        Mono<Boolean> postgresOk = db.sql("SELECT 1 AS ok").fetch().one()
                .map(row -> true)
                .onErrorResume(e -> Mono.just(false));

        return Mono.zip(redisOk, postgresOk).map(t -> {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("gateway", true);
            out.put("redis", t.getT1());
            out.put("postgres", t.getT2());
            out.put("routerPolicy", properties.getRouter().getPolicy());
            out.put("taskRoutingEnabled", properties.getRouter().getTaskRouting().isEnabled());
            out.put("cacheEnabled", properties.getCache().isEnabled());
            out.put("cacheExactEnabled", properties.getCache().isExactEnabled());
            out.put("cacheSemanticEnabled", properties.getCache().isSemanticEnabled());
            out.put("costSink", properties.getCost().getSink());
            out.put("quotaLimitTokens", properties.getQuota().getDefaultLimitTokens());
            return out;
        });
    }
}
