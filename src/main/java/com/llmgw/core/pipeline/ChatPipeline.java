package com.llmgw.core.pipeline;

import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.cost.CostLedgerService;
import com.llmgw.core.protocol.ProviderAdapter;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.protocol.StreamChunk;
import com.llmgw.core.quota.QuotaExceededException;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.router.RoutingPolicy;
import com.llmgw.observability.GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一次请求的编排：配额 → 语义缓存 → 路由 → 上游 → 级联/降级 → 落账。
 * 流式路径与非流式路径共用前置步骤，差异只在最后一步的返回形态。
 *
 * 线程模型（P1 响应式化后）：
 *   全链路不阻塞事件循环线程。上游调用（OpenAiAdapter）与缓存
 *   （RedisSemanticCacheService）都返回 Mono/Flux，直接在当前链上组合，
 *   不再需要 subscribeOn(boundedElastic) 兜底 —— 那是旧实现为掩盖 .block()
 *   打的补丁，代价是并发上限被弹性线程池大小（默认 10 × CPU 核数）锁死，
 *   且客户端断开后上游请求不会被取消。
 *
 *   唯一的例外是 recordAsync：它调用的 CostLedgerService.record 内部会执行
 *   KafkaTemplate.send()，而该调用在 broker 不可达时会阻塞到 max.block.ms
 *   （已压到 500ms）。这不是"兜住整条链路"，而是把一个**已知会阻塞的边缘操作**
 *   隔离到独立线程，主链路不等待它。
 *
 * 三条不变量（都是被线上问题换回来的，改代码时别破坏）：
 *   A. 落账只能走 recordAsync —— 任何一次同步 ledger.record 都可能把响应拖住。
 *   B. 缓存 namespace 必须带模型维度 —— 否则强模型的答案会被弱模型请求命中。
 *   C. 每一次真实发生的上游调用都要落一条账 —— 级联升级时是两次调用、两份费用。
 */
@Service
public class ChatPipeline {

    private static final Logger log = LoggerFactory.getLogger(ChatPipeline.class);

    private final QuotaService quotaService;
    private final SemanticCacheService cacheService;
    private final RouterService routerService;
    private final ProviderRegistry providers;
    private final CostLedgerService ledger;
    private final GatewayMetrics metrics;
    private final GatewayProperties properties;

    public ChatPipeline(QuotaService quotaService,
                        SemanticCacheService cacheService,
                        RouterService routerService,
                        ProviderRegistry providers,
                        CostLedgerService ledger,
                        GatewayMetrics metrics,
                        GatewayProperties properties) {
        this.quotaService = quotaService;
        this.cacheService = cacheService;
        this.routerService = routerService;
        this.providers = providers;
        this.ledger = ledger;
        this.metrics = metrics;
        this.properties = properties;
    }

    public Mono<ChatResponse> execute(String apiKey, ChatRequest request, String traceId) {
        if (request.messages() == null || request.messages().isEmpty()) {
            // 显式校验而不是靠 Bean Validation：错误能沿现有链路变成 OpenAI 形状的 400，
            // 不需要再引入一层 @ControllerAdvice 来接管异常（见 README 的取舍说明）。
            return Mono.error(new IllegalArgumentException("messages 不能为空"));
        }
        String question = questionOf(request);
        long estimatedTokens = Math.max(1, question.length() / 4);
        long start = System.currentTimeMillis();
        // 档位提前解析：缓存 namespace 需要模型维度。
        // 放在配额之前是安全的 —— RouterService.pick 是纯函数，不做 IO。
        ModelTier tier = resolveTier(request);
        String namespace = cacheNamespace(apiKey, tier.model());

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMap(allowed -> {
                    if (!allowed) {
                        return Mono.error(new QuotaExceededException(apiKey));
                    }
                    // 缓存查询自身非阻塞：embedding 与 Redis 都走响应式客户端
                    return cacheService.lookup(namespace, question)
                            .flatMap(hit -> {
                                metrics.onCacheHit();
                                long latency = System.currentTimeMillis() - start;
                                ChatResponse payload = new ChatResponse("cache", "cache", hit.answer(),
                                        new Usage(0, 0), true);
                                // 不变量 A：命中同样要落账，但必须异步。
                                // 旧实现这里是同步 ledger.record —— 命中本来是最快的路径，
                                // 却在 Kafka 不可达时白等 max.block.ms，把优化点变成新瓶颈。
                                recordAsync(traceId, apiKey, request, payload, 0.0, latency);
                                return Mono.just(payload);
                            })
                            .switchIfEmpty(Mono.defer(() -> {
                                metrics.onCacheMiss();
                                return dispatch(apiKey, request, question, tier, start, traceId);
                            }));
                });
    }

    public Flux<StreamChunk> executeStream(String apiKey, ChatRequest request, String traceId) {
        if (request.messages() == null || request.messages().isEmpty()) {
            return Flux.error(new IllegalArgumentException("messages 不能为空"));
        }
        String question = questionOf(request);
        long estimatedTokens = Math.max(1, question.length() / 4);
        ModelTier tier = resolveTier(request);
        String namespace = cacheNamespace(apiKey, tier.model());

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMapMany(allowed -> {
                    if (!allowed) {
                        return Flux.error(new QuotaExceededException(apiKey));
                    }
                    return cacheService.lookup(namespace, question)
                            .flatMapMany(hit -> {
                                metrics.onCacheHit();
                                recordAsync(traceId, apiKey, request,
                                        new ChatResponse("cache", "cache", hit.answer(),
                                                new Usage(0, 0), true),
                                        0.0, 0);
                                // 模型名标记为 cache：客户端能从分片里看出这次没走上游。
                                return Flux.just(StreamChunk.delta("cache", hit.answer()));
                            })
                            .switchIfEmpty(Flux.defer(() -> {
                                // 非流式分支里有 onCacheMiss，流式分支曾经漏了 ——
                                // 结果是指标统计偏低（实测 3 次请求只记到 2 次 miss）
                                metrics.onCacheMiss();
                                return streamDispatch(apiKey, request, question, tier, traceId);
                            }));
                });
    }

    private Mono<ChatResponse> dispatch(String apiKey, ChatRequest request, String question,
                                         ModelTier tier, long start, String traceId) {
        ProviderAdapter adapter = providers.forModel(tier.model());
        long begin = System.currentTimeMillis();

        return adapter.chat(tier.model(), request)
                .flatMap(response -> {
                    boolean formatValid = response.content() != null && !response.content().isBlank();
                    if (routerService.shouldEscalate(tier, response.content(), formatValid)) {
                        metrics.onEscalate();
                        // 不变量 C：被升级掉的这一次调用已经真实发生并计费，
                        // 必须单独落一条账。旧实现只记最终强模型那一次，
                        // 于是"实际两次上游调用、账本记一份金额"，成本归因系统性偏低。
                        Usage firstUsage = adapter.estimateUsage(tier.model(), request, response.content());
                        recordAsync(traceId, apiKey, request,
                                new ChatResponse(response.id(), tier.model(), response.content(),
                                        firstUsage, false),
                                estimateCost(tier.model(), tier, firstUsage),
                                System.currentTimeMillis() - start);

                        ModelTier stronger = routerService.escalateFrom(tier);
                        ProviderAdapter strongAdapter = providers.forModel(stronger.model());
                        return strongAdapter.chat(stronger.model(), request)
                                .map(upgraded -> new ChatResponse(upgraded.id(), stronger.model(),
                                        upgraded.content(), upgraded.usage(), false));
                    }
                    return Mono.just(response);
                })
                .doOnNext(response -> {
                    long latency = System.currentTimeMillis() - (start == 0 ? begin : start);
                    Usage usage = response.usage() == null ? new Usage(0, 0) : response.usage();
                    recordAsync(traceId, apiKey, request, response,
                            estimateCost(response.model(), tier, usage), latency);
                    if (!response.cacheHit()) {
                        // 不变量 B：缓存段按"实际服务的模型"划，不用请求档位划，
                        // 否则升级后的高质量答案会被写进便宜模型的段里、被后续低价请求复用。
                        writeCache(cacheNamespace(apiKey, response.model()),
                                question, response.content());
                    }
                });
    }

    private Flux<StreamChunk> streamDispatch(String apiKey, ChatRequest request, String question,
                                              ModelTier tier, String traceId) {
        ProviderAdapter cheap = providers.forModel(tier.model());
        AtomicBoolean emitted = new AtomicBoolean(false);
        // 级联升级后实际服务的是更强的模型，账本与响应分片都必须记真实模型名，
        // 否则会出现"按便宜模型计价"的归因错误、以及客户端看到错的模型。
        AtomicReference<String> servedBy = new AtomicReference<>(tier.model());
        // 上游末帧的真实用量。拿不到时才回退估算（F4）。
        AtomicReference<Usage> upstreamUsage = new AtomicReference<>();
        StringBuilder collected = new StringBuilder();
        long start = System.currentTimeMillis();

        Flux<StreamChunk> primary = cheap.chatStream(tier.model(), request)
                .doOnNext(chunk -> collect(chunk, emitted, collected, upstreamUsage));

        return primary
                // 已输出内容后不再切换模型，避免用户看到两段拼接的答案
                .onErrorResume(e -> emitted.get()
                        ? Flux.<StreamChunk>error(e)
                        : Flux.defer(() -> {
                            metrics.onEscalate();
                            ModelTier stronger = routerService.escalateFrom(tier);
                            servedBy.set(stronger.model());
                            return providers.forModel(stronger.model())
                                    .chatStream(stronger.model(), request)
                                    .doOnNext(chunk -> collect(chunk, emitted, collected, upstreamUsage));
                        }))
                .doOnComplete(() -> {
                    long latency = System.currentTimeMillis() - start;
                    String model = servedBy.get();
                    // 优先用上游真实 usage；缺失才回退估算。回退值不冒充真实值，
                    // 这一点写在 README 的「已知边界」里。
                    Usage usage = upstreamUsage.get() != null
                            ? upstreamUsage.get()
                            : new Usage(question.length() / 4, collected.length() / 4);
                    ChatResponse snapshot = new ChatResponse("stream", model, collected.toString(), usage, false);
                    recordAsync(traceId, apiKey, request, snapshot,
                            estimateCost(model, tier, usage), latency);
                    writeCache(cacheNamespace(apiKey, model), question, collected.toString());
                });
    }

    /** 累积一条上游分片：文本进缓冲，用量进引用。用量帧不影响 emitted（它没有内容）。 */
    private static void collect(StreamChunk chunk, AtomicBoolean emitted,
                                StringBuilder collected, AtomicReference<Usage> upstreamUsage) {
        if (chunk.hasDelta()) {
            emitted.set(true);
            collected.append(chunk.delta());
        }
        if (chunk.usage() != null) {
            upstreamUsage.set(chunk.usage());
        }
    }

    private ModelTier resolveTier(ChatRequest request) {
        if (!"auto".equals(request.resolvedModel())) {
            return new ModelTier(request.resolvedModel(), "EXPLICIT", 0.0, 1.0);
        }
        return routerService.pick(RoutingPolicy.valueOf(properties.getRouter().getPolicy()));
    }

    /**
     * 缓存 namespace 必须带模型维度。
     *
     * 旧实现是 sem:{apiKey}，于是同一租户下 gpt-4o 的高质量答案会被 gpt-4o-mini
     * 的请求命中并复用 —— 路由策略（按成本/质量挑模型）被缓存整个架空。
     * 流式降级失败的那一次调用没有可信的 usage，这里不伪造账本行（见 README 已知边界）。
     */
    static String cacheNamespace(String apiKey, String model) {
        return apiKey + ":" + model;
    }

    private double estimateCost(String model, ModelTier tier, Usage usage) {
        // 优先用配置的真实单价（输入/输出分开计）——否则账本会全是 0，
        // 成本归因就成了空话。没有配单价的模型才回退到 ModelTier 的粗略估算。
        GatewayProperties.Price price = properties.getPricing().get(model);
        if (price != null) {
            return price.costOf(usage.promptTokens(), usage.completionTokens());
        }
        return usage.total() / 1000.0 * tier.costPer1k();
    }

    /**
     * 落账必须在响应路径之外。KafkaTemplate.send() 不是"立即返回"的：
     * broker 不可达时会阻塞到 max.block.ms 才抛 TimeoutException。
     * 双保险：① 本方法丢到独立线程；② application.yml 里把 max.block.ms 压到 500ms。
     *
     * 注意这是全链路里**唯一**保留 subscribeOn 的地方，且理由与前几处不同：
     * 不是因为"整条链路是阻塞的"，而是因为 CostLedgerService.record 内部
     * 有一次已知会阻塞的 Kafka 调用，需要把它隔离出去。
     */
    private void recordAsync(String traceId, String apiKey, ChatRequest request, ChatResponse response,
                             double cost, long latencyMs) {
        Usage u = response.usage() == null ? new Usage(0, 0) : response.usage();
        Mono.fromRunnable(() -> ledger.record(traceId, apiKey, request.feature(), response.model(),
                        response.cacheHit(), u.promptTokens(), u.completionTokens(), cost, latencyMs))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.debug("cost ledger failed: {}", e.getMessage()));
    }

    /**
     * 缓存写入是 fire-and-forget：写失败不能影响用户已经拿到的回答。
     * P1 之后 write 本身返回 Mono（内部不再 block），所以这里直接订阅即可，
     * 不再需要 subscribeOn —— 它是响应式链，在哪个线程订阅都不阻塞。
     */
    private void writeCache(String namespace, String question, String answer) {
        if (!properties.getCache().isEnabled() || answer == null || answer.isBlank()) {
            return;
        }
        cacheService.write(namespace, question, answer)
                .subscribe(null, e -> log.debug("cache write failed: {}", e.getMessage()));
    }

    private String questionOf(ChatRequest request) {
        return request.messages().stream()
                .filter(m -> "user".equals(m.role()))
                .reduce((a, b) -> b)
                .map(m -> m.content() == null ? "" : m.content())
                .orElse("");
    }
}
