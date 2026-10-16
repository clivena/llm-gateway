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
import com.llmgw.core.protocol.UpstreamOutcome;
import com.llmgw.core.quota.QuotaExceededException;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.router.RoutingPolicy;
import com.llmgw.core.token.TokenEstimator;
import com.llmgw.observability.GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    private final TokenEstimator tokenEstimator;

    public ChatPipeline(QuotaService quotaService,
                        SemanticCacheService cacheService,
                        RouterService routerService,
                        ProviderRegistry providers,
                        CostLedgerService ledger,
                        GatewayMetrics metrics,
                        GatewayProperties properties,
                        TokenEstimator tokenEstimator) {
        this.quotaService = quotaService;
        this.cacheService = cacheService;
        this.routerService = routerService;
        this.providers = providers;
        this.ledger = ledger;
        this.metrics = metrics;
        this.properties = properties;
        this.tokenEstimator = tokenEstimator;
    }

    public Mono<ChatResponse> execute(String apiKey, ChatRequest request, String traceId) {
        if (request.messages() == null || request.messages().isEmpty()) {
            // 显式校验而不是靠 Bean Validation：错误能沿现有链路变成 OpenAI 形状的 400，
            // 不需要再引入一层 @ControllerAdvice 来接管异常（见 README 的取舍说明）。
            return Mono.error(new IllegalArgumentException("messages 不能为空"));
        }
        String question = questionOf(request);
        // 配额预扣必须覆盖**全部消息**（P5）。旧实现在这里算的是 question.length()/4，
        // 那既用了英文公式（中文低估 6.43 倍），又只算最后一条 user 消息 ——
        // system prompt 与多轮历史全都白用。两项叠加，配额在中文多轮场景下基本失效。
        // 注意 questionOf 仍然只取最后一条 user 消息：那是**缓存键**的语义
        // （用户问的"这一句"是什么），与配额预算需要的"整包多大"是两件事。
        long estimatedTokens = tokenEstimator.estimatePrompt(request.messages());
        long start = System.currentTimeMillis();
        // 档位提前解析：缓存 namespace 需要模型维度。
        // 放在配额之前是安全的 —— RouterService.pick 是纯函数，不做 IO。
        ModelTier tier = resolveTier(request);
        String namespace = cacheNamespace(apiKey, tier.model());

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMap(allowed -> {
                    if (!allowed) {
                        metrics.onQuotaRejected();
                        return Mono.error(new QuotaExceededException(apiKey));
                    }
                    // 缓存查询自身非阻塞：embedding 与 Redis 都走响应式客户端
                    return cacheService.lookup(namespace, question)
                            .flatMap(hit -> {
                                metrics.onCacheHit();
                                long latency = System.currentTimeMillis() - start;
                                // 命中路径的延迟单独记（model 标为 "cache"）：
                                // 它本该是最快的一条路径，与任何模型的耗时混在一个分布里
                                // 就看不出"缓存到底省了多少"—— 而那正是缓存存在的理由。
                                metrics.recordLatency("cache", false, latency);
                                ChatResponse payload = new ChatResponse("cache", "cache", hit.answer(),
                                        new Usage(0, 0), true);
                                // 不变量 A：命中同样要落账，但必须异步。
                                // 旧实现这里是同步 ledger.record —— 命中本来是最快的路径，
                                // 却在 Kafka 不可达时白等 max.block.ms，把优化点变成新瓶颈。
                                recordAsync(traceId, apiKey, request, payload, 0.0, latency);
                                // 结算（F0）：缓存命中 = 完全没走上游 = 真实用量为 0。
                                // 预扣的那笔必须**全额退还** —— 否则命中一次反而白扣一次配额，
                                // 而"命中就省钱"正是缓存存在的全部理由。
                                // 这一条在 F0 之前是漏的：命中省了钱，却照样吃配额。
                                return settleQuota(apiKey, estimatedTokens, 0).thenReturn(payload);
                            })
                            .switchIfEmpty(Mono.defer(() -> {
                                metrics.onCacheMiss();
                                return dispatch(apiKey, request, question, tier, start, traceId,
                                        estimatedTokens);
                            }));
                });
    }

    public Flux<StreamChunk> executeStream(String apiKey, ChatRequest request, String traceId) {
        if (request.messages() == null || request.messages().isEmpty()) {
            return Flux.error(new IllegalArgumentException("messages 不能为空"));
        }
        String question = questionOf(request);
        long estimatedTokens = tokenEstimator.estimatePrompt(request.messages());
        ModelTier tier = resolveTier(request);
        String namespace = cacheNamespace(apiKey, tier.model());

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMapMany(allowed -> {
                    if (!allowed) {
                        metrics.onQuotaRejected();
                        return Flux.error(new QuotaExceededException(apiKey));
                    }
                    return cacheService.lookup(namespace, question)
                            .flatMapMany(hit -> {
                                metrics.onCacheHit();
                                recordAsync(traceId, apiKey, request,
                                        new ChatResponse("cache", "cache", hit.answer(),
                                                new Usage(0, 0), true),
                                        0.0, 0);
                                // 命中即全额退还预扣，与非流式路径同一条理由（见 execute）。
                                // 注意这里是 flatMapMany：退还完成后再把分片发出去，
                                // 保证"流结束 ⇒ 配额已结清"这个不变量成立。
                                // 模型名标记为 cache：客户端能从分片里看出这次没走上游。
                                return settleQuota(apiKey, estimatedTokens, 0)
                                        .thenMany(Flux.just(StreamChunk.delta("cache", hit.answer())));
                            })
                            .switchIfEmpty(Flux.defer(() -> {
                                // 非流式分支里有 onCacheMiss，流式分支曾经漏了 ——
                                // 结果是指标统计偏低（实测 3 次请求只记到 2 次 miss）
                                metrics.onCacheMiss();
                                return streamDispatch(apiKey, request, question, tier, traceId,
                                        estimatedTokens);
                            }));
                });
    }

    private Mono<ChatResponse> dispatch(String apiKey, ChatRequest request, String question,
                                         ModelTier tier, long start, String traceId, long reserved) {
        ProviderAdapter adapter = providers.forModel(tier.model());
        long begin = System.currentTimeMillis();
        // 本次请求实际消耗的总 token，可能来自**两次**上游调用（级联升级）。
        // 用 AtomicLong 在这里汇合两条路径的用量，正好是配额结算需要的口径：
        // 不变量 C 说"两次调用就有两份费用"，那就该有两份 token 计入配额 ——
        // 账本与配额两个口径必须一致，否则对不上账时不知道信哪个。
        AtomicLong spentTokens = new AtomicLong(0);

        return adapter.chat(tier.model(), request)
                .flatMap(response -> {
                    // F0：先判定"上游到底给了什么"，再谈怎么处理。
                    UpstreamOutcome outcome = UpstreamOutcome.of(response.content(), response.finishReason());
                    metrics.onUpstreamOutcome(outcome);
                    logOutcome("non-stream", tier.model(), outcome, response.usage());

                    if (routerService.shouldEscalate(tier, response.content(), !outcome.empty())) {
                        metrics.onEscalate();
                        // 不变量 C：被升级掉的这一次调用已经真实发生并计费，
                        // 必须单独落一条账。旧实现只记最终强模型那一次，
                        // 于是"实际两次上游调用、账本记一份金额"，成本归因系统性偏低。
                        //
                        // 用**真实** usage 而不是 estimateUsage：真实值就在 response.usage() 里，
                        // 手上有真值却去估算，会让账本与配额两个口径同时失真（F0 修）。
                        // usageOrEstimate 只在真实值缺失时才回退。
                        Usage firstUsage = usageOrEstimate(request, response);
                        spentTokens.addAndGet(firstUsage.total());
                        recordAsync(traceId, apiKey, request,
                                new ChatResponse(response.id(), tier.model(), response.content(),
                                        firstUsage, false, response.finishReason()),
                                estimateCost(tier.model(), tier, firstUsage),
                                System.currentTimeMillis() - start);

                        ModelTier stronger = routerService.escalateFrom(tier);
                        ProviderAdapter strongAdapter = providers.forModel(stronger.model());
                        return strongAdapter.chat(stronger.model(), request)
                                .map(upgraded -> new ChatResponse(upgraded.id(), stronger.model(),
                                        upgraded.content(), upgraded.usage(), false,
                                        upgraded.finishReason()));
                    }
                    return Mono.just(response);
                })
                .flatMap(response -> {
                    long latency = System.currentTimeMillis() - (start == 0 ? begin : start);
                    // 上游没给可信 usage 时回退估算（F0）。这一步不能省：
                    // 结算按 actual 补扣，若把"没给 usage"当成 0，会**退掉整笔预扣** ——
                    // 而这次请求明明花了钱。账本那边同理，会记一条 0 成本的假账。
                    Usage usage = usageOrEstimate(request, response);
                    spentTokens.addAndGet(usage.total());
                    // 延迟指标必须独立于账本：账本走的是 Kafka/Postgres 异步链路，
                    // 那条链路挂了正是最需要看延迟的时刻 —— 记在账本里会一起瞎掉。
                    metrics.recordLatency(response.model(), false, latency);
                    // 落账用同一份 usage（缺失时是回退值），保证账本与配额两个口径一致 ——
                    // 两边不一致时对账根本不知道信哪个。传入的 ChatResponse 只在
                    // "真实 usage 缺失"时才需要重建，正常路径直接复用原对象。
                    ChatResponse ledgerRow = (response.usage() != null && response.usage().total() > 0)
                            ? response
                            : new ChatResponse(response.id(), response.model(), response.content(),
                                    usage, response.cacheHit(), response.finishReason());
                    recordAsync(traceId, apiKey, request, ledgerRow,
                            estimateCost(response.model(), tier, usage), latency);
                    if (!response.cacheHit()) {
                        // 不变量 B：缓存段按"实际服务的模型"划，不用请求档位划，
                        // 否则升级后的高质量答案会被写进便宜模型的段里、被后续低价请求复用。
                        // 空答案由 writeCache 自己拒绝写入（它会打 warn），这里不重复判。
                        writeCache(cacheNamespace(apiKey, response.model()),
                                question, response.content());
                    }
                    // 结算放在返回之前（不是 fire-and-forget）：一次 Redis 往返，
                    // 换来一个容易验证的不变量 —— "响应已返回 ⇒ 配额已结清"。
                    // 若改为异步，配额面板就可能比账本慢一拍，对账时说不清谁是对的。
                    return settleQuota(apiKey, reserved, spentTokens.get()).thenReturn(response);
                });
    }

    private Flux<StreamChunk> streamDispatch(String apiKey, ChatRequest request, String question,
                                              ModelTier tier, String traceId, long reserved) {
        ProviderAdapter cheap = providers.forModel(tier.model());
        AtomicBoolean emitted = new AtomicBoolean(false);
        // 级联升级后实际服务的是更强的模型，账本与响应分片都必须记真实模型名，
        // 否则会出现"按便宜模型计价"的归因错误、以及客户端看到错的模型。
        AtomicReference<String> servedBy = new AtomicReference<>(tier.model());
        // 上游末帧的真实用量。拿不到时才回退估算（F4）。
        AtomicReference<Usage> upstreamUsage = new AtomicReference<>();
        // 上游的收尾帧给的原因（F0）。流式同样需要它 —— 否则客户端看到 [DONE]
        // 也分不清"正常结束"和"被 length 截断"。
        AtomicReference<String> finishReason = new AtomicReference<>();
        StringBuilder collected = new StringBuilder();
        long start = System.currentTimeMillis();
        // 待结算的真实用量。-1 表示"还没走到流结束"，此时不该结算。
        AtomicLong settleAmount = new AtomicLong(-1);

        Flux<StreamChunk> primary = cheap.chatStream(tier.model(), request)
                .doOnNext(chunk -> collect(chunk, emitted, collected, upstreamUsage, finishReason));

        Flux<StreamChunk> body = primary
                // 已输出内容后不再切换模型，避免用户看到两段拼接的答案
                .onErrorResume(e -> emitted.get()
                        ? Flux.<StreamChunk>error(e)
                        : Flux.defer(() -> {
                            metrics.onEscalate();
                            ModelTier stronger = routerService.escalateFrom(tier);
                            servedBy.set(stronger.model());
                            // 换了模型，上一次调用留下的 finish_reason 必须清掉：
                            // 那是"便宜模型为什么被换掉"的原因，不是最终答案的原因。
                            finishReason.set(null);
                            return providers.forModel(stronger.model())
                                    .chatStream(stronger.model(), request)
                                    .doOnNext(chunk -> collect(chunk, emitted, collected, upstreamUsage, finishReason));
                        }))
                .doOnComplete(() -> {
                    long latency = System.currentTimeMillis() - start;
                    String model = servedBy.get();
                    // stream=true 单独打标：流式的"总耗时"与非流式不是同一件事
                    // （流式真正决定体验的是首字延迟），混在一个直方图里会互相污染 P99。
                    metrics.recordLatency(model, true, latency);
                    // 优先用上游真实 usage；缺失才回退估算。回退值不冒充真实值，
                    // 这一点写在 README 的「已知边界」里。
                    // 回退估算改用 TokenEstimator（P5）：/4 的英文公式对中文答案同样低估，
                    // 而流式恰恰是中文输出的主战场。
                    Usage usage = upstreamUsage.get() != null
                            ? upstreamUsage.get()
                            : tokenEstimator.estimateUsage(request.messages(), collected.toString());
                    // F0：流式也要判定可用性。用"累积到的整条正文"而不是单帧 ——
                    // 流式途中每一帧都很短，只有汇总才说明问题。
                    UpstreamOutcome outcome = UpstreamOutcome.of(collected.toString(), finishReason.get());
                    metrics.onUpstreamOutcome(outcome);
                    logOutcome("stream", model, outcome, usage);

                    ChatResponse snapshot = new ChatResponse("stream", model, collected.toString(),
                            usage, false, finishReason.get());
                    recordAsync(traceId, apiKey, request, snapshot,
                            estimateCost(model, tier, usage), latency);
                    writeCache(cacheNamespace(apiKey, model), question, collected.toString());
                    settleAmount.set(usage.total());
                });

        // 结算串在流的**末尾**而不是丢进 doOnComplete 里 fire-and-forget。
        // 理由是那个可验证的不变量："客户端收到 [DONE] ⇒ 配额已结清"。
        // 若用 doFinally 会把"客户端中途断开"也当成完成去结算 —— 而那种情况下
        // 上游已经花了钱，预扣本就该留着。
        return body.concatWith(Flux.defer(() -> {
            long amount = settleAmount.get();
            if (amount < 0) {
                return Flux.empty();
            }
            return settleQuota(apiKey, reserved, amount).thenMany(Flux.<StreamChunk>empty());
        }));
    }

    /** 累积一条上游分片：文本进缓冲，用量与收尾原因进引用。用量/收尾帧不影响 emitted（它们没有内容）。 */
    private static void collect(StreamChunk chunk, AtomicBoolean emitted, StringBuilder collected,
                                AtomicReference<Usage> upstreamUsage,
                                AtomicReference<String> finishReason) {
        if (chunk.hasDelta()) {
            emitted.set(true);
            collected.append(chunk.delta());
        }
        if (chunk.usage() != null) {
            upstreamUsage.set(chunk.usage());
        }
        if (chunk.isFinish()) {
            finishReason.set(chunk.finishReason());
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
     * 优先取上游回传的**真实** usage，缺失才回退估算（F0）。
     *
     * <p>为什么必须回退而不是填 0：配额结算是按 {@code actual - reserved} 补扣的，
     * 如果"上游没给 usage"就当成 actual=0，等于**把整笔预扣退回去** ——
     * 而这次请求明明花了钱。同理账本会记一条 0 成本的假账。
     * "不知道花了多少"不等于"没花钱"，宁可退回估算值。
     *
     * <p>用 {@code tokenEstimator} 而不是 {@code adapter.estimateUsage}：
     * 级联升级后最终响应的模型可能是**另一家** provider 的，用旧的 adapter
     * 去估算语义上就不成立（虽然当前实现恰好是同一个纯公式，那是巧合不是契约）。
     * 管线手上有 estimator，直接用。
     *
     * <p>判定用 {@code total() > 0} 而不是 {@code usage != null}：上游确实可能回一个
     * {@code {prompt_tokens:0, completion_tokens:0}} 的壳，那种"有对象但没数"的情况
     * 也应当走估算。
     */
    private Usage usageOrEstimate(ChatRequest request, ChatResponse response) {
        Usage real = response.usage();
        if (real != null && real.total() > 0) {
            return real;
        }
        return tokenEstimator.estimateUsage(request.messages(), response.content());
    }

    /**
     * 上游"给了响应但没给答案"时的告警（F0）。
     *
     * 为什么必须留这条日志：这类失败在 F0 之前**完全不可见** —— 上游 HTTP 200、
     * 网关主链路一切正常、账本正常落账、日志一片干净，只有客户端发现答案是空的，
     * 而它还以为这就叫成功。指标能告诉你"变多了"，日志才能告诉你"长什么样"：
     * finish_reason 是什么、这次花了多少 token。
     */
    private void logOutcome(String path, String model, UpstreamOutcome outcome, Usage usage) {
        if (outcome.empty() || outcome.truncated()) {
            log.warn("upstream returned unusable response [{} model={} finish_reason={}]: {} usage={}",
                    path, model, outcome.finishReason(), outcome.describe(), usage);
        }
    }

    /**
     * 按真实用量结算配额差额（F0）。失败不抛 —— 结算失败不该把一次**已经成功**
     * 的请求变成失败，代价只是这一次的配额失真（已计数 + 打日志）。
     */
    private Mono<Void> settleQuota(String apiKey, long reserved, long actual) {
        return quotaService.settle(apiKey, reserved, actual)
                .doOnNext(finalUsed -> {
                    if (finalUsed >= 0) {
                        metrics.onQuotaSettled();
                    }
                })
                .onErrorResume(e -> {
                    metrics.onQuotaSettleFailed();
                    log.warn("quota settle failed [apiKey={} reserved={} actual={}]: {}",
                            apiKey, reserved, actual, e.toString());
                    return Mono.empty();
                })
                .then();
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
            // 这个 warn 不是调试残留，而是必须保留的可观测项：
            // "缓存开着但从不写入"是最难查的一类静默失效 —— 主链路一切正常、
            // 日志一片干净，只有翻开 Redis 才发现一个键都没有。
            // 把跳过的原因（哪个条件挡住的）直接写出来，是最低成本的可观测性。
            log.warn("cache write skipped [ns={}]: cacheEnabled={} answerBlank={}",
                    namespace, properties.getCache().isEnabled(),
                    answer == null || answer.isBlank());
            return;
        }
        cacheService.write(namespace, question, answer)
                .doOnSuccess(v -> log.debug("cache written [ns={}]", namespace))
                .subscribe(null, e -> log.warn("cache write failed [ns={}]: {}",
                        namespace, e.toString()));
    }

    private String questionOf(ChatRequest request) {
        return request.messages().stream()
                .filter(m -> "user".equals(m.role()))
                .reduce((a, b) -> b)
                .map(m -> m.content() == null ? "" : m.content())
                .orElse("");
    }
}
