package com.llmgw.core.pipeline;

import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.cost.CostLedgerService;
import com.llmgw.core.protocol.ProviderAdapter;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一次请求的编排：配额 → 语义缓存 → 路由 → 上游 → 级联/降级 → 落账。
 * 流式路径与非流式路径共用前置步骤，差异只在最后一步的返回形态。
 *
 * 线程模型（真机联调踩出来的，很关键）：
 *   ① 上游适配器与缓存实现都是「同步阻塞」风格（WebClient.block / Redis block），
 *      而 WebFlux 的 Controller 直接在 Netty 事件循环线程上订阅本管线。
 *      在事件循环线程上做阻塞调用，Reactor 会抛：
 *        block()/blockFirst()/blockLast() are blocking, which is not supported in thread reactor-http-nio-N
 *      => 阻塞调用一律 subscribeOn(Schedulers.boundedElastic())。
 *   ② 落账与写缓存是"边缘副作用"，绝不能出现在响应路径上：
 *      KafkaTemplate.send() 在 broker 不可达时会阻塞到 max.block.ms（默认 60s）才返回，
 *      实测把 62s 全耗在这里（表面看是"异步发送"，实际拖死了响应）。
 *      => 一律丢到独立线程 fire-and-forget。
 */
@Service
public class ChatPipeline {

    private static final Logger log = LoggerFactory.getLogger(ChatPipeline.class);

    private final QuotaService quotaService;
    private final SemanticCacheService cacheService;
    private final RouterService routerService;
    private final List<ProviderAdapter> adapters;
    private final CostLedgerService ledger;
    private final GatewayMetrics metrics;
    private final GatewayProperties properties;

    public ChatPipeline(QuotaService quotaService,
                        SemanticCacheService cacheService,
                        RouterService routerService,
                        List<ProviderAdapter> adapters,
                        CostLedgerService ledger,
                        GatewayMetrics metrics,
                        GatewayProperties properties) {
        this.quotaService = quotaService;
        this.cacheService = cacheService;
        this.routerService = routerService;
        this.adapters = adapters;
        this.ledger = ledger;
        this.metrics = metrics;
        this.properties = properties;
    }

    public Mono<ChatResponse> execute(String apiKey, ChatRequest request, String traceId) {
        String question = questionOf(request);
        long estimatedTokens = Math.max(1, question.length() / 4);
        long start = System.currentTimeMillis();

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMap(allowed -> {
                    if (!allowed) {
                        return Mono.error(new IllegalStateException("quota exceeded"));
                    }
                    // 缓存查询内部会 block（embedding HTTP + Redis），必须离开事件循环线程
                    return Mono.fromCallable(() -> cacheService.lookup(apiKey, question))
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(opt -> Mono.justOrEmpty(opt))
                            .flatMap(hit -> {
                                metrics.onCacheHit();
                                long latency = System.currentTimeMillis() - start;
                                ledger.record(traceId, apiKey, request.feature(), "cache", true, 0, 0, 0.0, latency);
                                return Mono.just(new ChatResponse("cache", "cache", hit.answer(), new Usage(0, 0), true));
                            })
                            .switchIfEmpty(Mono.defer(() -> {
                                metrics.onCacheMiss();
                                return dispatch(apiKey, request, question, start, traceId);
                            }));
                });
    }

    public Flux<String> executeStream(String apiKey, ChatRequest request, String traceId) {
        String question = questionOf(request);
        long estimatedTokens = Math.max(1, question.length() / 4);

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMapMany(allowed -> {
                    if (!allowed) {
                        return Flux.error(new IllegalStateException("quota exceeded"));
                    }
                    return Mono.fromCallable(() -> cacheService.lookup(apiKey, question))
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(opt -> Mono.justOrEmpty(opt))
                            .flatMapMany(hit -> {
                                metrics.onCacheHit();
                                ledger.record(traceId, apiKey, request.feature(), "cache", true, 0, 0, 0.0, 0);
                                return Flux.just(hit.answer());
                            })
                            .switchIfEmpty(Flux.defer(() -> {
                                // 非流式分支里有 onCacheMiss，流式分支曾经漏了 ——
                                // 结果是指标统计偏低（实测 3 次请求只记到 2 次 miss）
                                metrics.onCacheMiss();
                                return streamDispatch(apiKey, request, question, traceId);
                            }));
                });
    }

    private Mono<ChatResponse> dispatch(String apiKey, ChatRequest request, String question,
                                         long start, String traceId) {
        ModelTier tier = resolveTier(request);
        ProviderAdapter adapter = adapterFor(tier.model());
        long begin = System.currentTimeMillis();

        return Mono.fromCallable(() -> adapter.chat(tier.model(), request))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(response -> {
                    boolean formatValid = response.content() != null && !response.content().isBlank();
                    if (routerService.shouldEscalate(tier, response.content(), formatValid)) {
                        metrics.onEscalate();
                        ModelTier stronger = routerService.escalateFrom(tier);
                        ProviderAdapter strongAdapter = adapterFor(stronger.model());
                        ChatResponse upgraded = strongAdapter.chat(stronger.model(), request);
                        return Mono.just(new ChatResponse(upgraded.id(), stronger.model(),
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
                        writeCacheAsync(apiKey, question, response.content());
                    }
                });
    }

    private Flux<String> streamDispatch(String apiKey, ChatRequest request, String question, String traceId) {
        ModelTier tier = resolveTier(request);
        ProviderAdapter cheap = adapterFor(tier.model());
        AtomicBoolean emitted = new AtomicBoolean(false);
        // 级联升级后实际服务的是更强的模型，账本必须记真实模型名，
        // 否则会出现"按便宜模型计价"的归因错误。
        AtomicReference<String> servedBy = new AtomicReference<>(tier.model());
        StringBuilder collected = new StringBuilder();
        long start = System.currentTimeMillis();

        Flux<String> primary = cheap.chatStream(tier.model(), request)
                .doOnNext(chunk -> {
                    emitted.set(true);
                    collected.append(chunk);
                });

        return primary
                // 已输出内容后不再切换模型，避免用户看到两段拼接的答案
                .onErrorResume(e -> emitted.get()
                        ? Flux.<String>error(e)
                        : Flux.defer(() -> {
                            metrics.onEscalate();
                            ModelTier stronger = routerService.escalateFrom(tier);
                            servedBy.set(stronger.model());
                            return adapterFor(stronger.model()).chatStream(stronger.model(), request)
                                    .doOnNext(collected::append);
                        }))
                .doOnComplete(() -> {
                    long latency = System.currentTimeMillis() - start;
                    Usage usage = new Usage(question.length() / 4, collected.length() / 4);
                    String model = servedBy.get();
                    ChatResponse snapshot = new ChatResponse("stream", model, collected.toString(), usage, false);
                    recordAsync(traceId, apiKey, request, snapshot,
                            estimateCost(model, tier, usage), latency);
                    writeCacheAsync(apiKey, question, collected.toString());
                });
    }

    private ModelTier resolveTier(ChatRequest request) {
        if (!"auto".equals(request.resolvedModel())) {
            return new ModelTier(request.resolvedModel(), "EXPLICIT", 0.0, 1.0);
        }
        return routerService.pick(RoutingPolicy.valueOf(properties.getRouter().getPolicy()));
    }

    private ProviderAdapter adapterFor(String model) {
        return adapters.stream()
                .filter(a -> a.supports(model))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no adapter for model " + model));
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
     * 缓存写入同样是阻塞的（embedding 走 HTTP），丢到弹性线程池异步执行。
     * 写缓存失败不能影响用户已经拿到的回答，所以只记 debug 日志。
     */
    private void writeCacheAsync(String apiKey, String question, String answer) {
        if (!properties.getCache().isEnabled() || answer == null || answer.isBlank()) {
            return;
        }
        Mono.fromRunnable(() -> cacheService.write(apiKey, question, answer))
                .subscribeOn(Schedulers.boundedElastic())
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
