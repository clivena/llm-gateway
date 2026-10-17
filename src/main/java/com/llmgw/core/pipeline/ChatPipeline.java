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
import com.llmgw.core.resilience.UpstreamCircuitOpenException;
import com.llmgw.core.resilience.UpstreamEmptyResponseException;
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.Route;
import com.llmgw.core.router.RouteTrace;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.token.TokenEstimator;
import com.llmgw.observability.GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 *   D. 每一次预扣都必须有结算，**包括失败路径**（F1）——
 *      在此之前，上游报错 / 熔断 / 连不上的请求会**白吃掉一整笔预扣**：
 *      一次都没调通，配额却按预扣额扣掉了。平时看不出来，
 *      但因为熔断本身就是"连续失败"触发的，F1 会让这条泄漏在故障期集中爆发。
 *   E. 一次逻辑请求只预扣一次，但**每一家候选的尝试都要各自结算**（F2）——
 *      故障转移意味着一次请求可能真的打给了 2 家上游（都计费），
 *      而预扣只发生在那一次。所以转移路径上的失败尝试必须把**已知用量**
 *      累加进 spentTokens，否则"花了 N 次的钱、只结算了 1 次"。
 *      关联的已知取舍：故障期用户实际消耗可能超过配额上限（预扣是按单次算的）——
 *      见 docs/step-20 的已知边界，方向与 F1 一致（宁可少扣、不可多扣）。
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
    private final UpstreamResilience resilience;

    public ChatPipeline(QuotaService quotaService,
                        SemanticCacheService cacheService,
                        RouterService routerService,
                        ProviderRegistry providers,
                        CostLedgerService ledger,
                        GatewayMetrics metrics,
                        GatewayProperties properties,
                        TokenEstimator tokenEstimator,
                        UpstreamResilience resilience) {
        this.quotaService = quotaService;
        this.cacheService = cacheService;
        this.routerService = routerService;
        this.providers = providers;
        this.ledger = ledger;
        this.metrics = metrics;
        this.properties = properties;
        this.tokenEstimator = tokenEstimator;
        this.resilience = resilience;
    }

    /**
     * 不带路由回执的入口（内部调用与单元测试用）。
     *
     * <p>保留它不是为了兼容，而是为了让"我只关心管线跑通"的调用点
     * 不必为了一个新参数改二十多处 —— 那种全量改动会把真正需要 review 的
     * 差异淹没掉。生产路径由控制器传一个真实的 {@link RouteTrace}。
     */
    public Mono<ChatResponse> execute(String apiKey, ChatRequest request, String traceId) {
        return execute(apiKey, request, traceId, RouteTrace.ignored());
    }

    /**
     * @param trace 路由回执，解析完成时写入；供 HTTP 层设置响应头。
     *              它不参与任何业务判断 —— 传 {@code ignored()} 与传真实实例
     *              的行为差异只有一个：外部能不能看到这次选了哪个任务。
     */
    public Mono<ChatResponse> execute(String apiKey, ChatRequest request, String traceId,
                                      RouteTrace trace) {
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
        // 放在配额之前是安全的 —— 路由与任务判定都是纯函数，不做 IO。
        // 任务路由（按 task 选模型）就发生在这里；它只影响下面这个 route.tier()，
        // 后面的配额、缓存、候选链、级联全部沿用原有逻辑。
        Route route;
        try {
            route = resolveRoute(request, trace);
        } catch (RuntimeException e) {
            // ★ 必须显式转成 Mono.error。
            // 控制器把所有错误映射（状态码 + OpenAI 错误体）放在 onErrorResume 里，
            // 而这里若**同步抛出**，就不再是"流里的错误"，onErrorResume 根本不会执行 ——
            // 异常会冒泡给 Spring 变成 500，于是"未知任务 → 400"这条设计直接失效，
            // 用户还会以为自己的请求触发了服务端故障。任务名写错正是最容易踩到它的地方。
            return Mono.error(e);
        }
        String namespace = cacheNamespace(apiKey, route.tier().model());

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
                                recordAsync(traceId, apiKey, request, route.task(), payload, 0.0, latency);
                                // 结算（F0）：缓存命中 = 完全没走上游 = 真实用量为 0。
                                // 预扣的那笔必须**全额退还** —— 否则命中一次反而白扣一次配额，
                                // 而"命中就省钱"正是缓存存在的全部理由。
                                // 这一条在 F0 之前是漏的：命中省了钱，却照样吃配额。
                                return settleQuota(apiKey, estimatedTokens, 0).thenReturn(payload);
                            })
                            .switchIfEmpty(Mono.defer(() -> {
                                metrics.onCacheMiss();
                                return dispatch(apiKey, request, question, route, start, traceId,
                                        estimatedTokens);
                            }));
                });
    }

    /** 流式入口的"不带路由回执"版本，理由同 {@link #execute(String, ChatRequest, String)}。 */
    public Flux<StreamChunk> executeStream(String apiKey, ChatRequest request, String traceId) {
        return executeStream(apiKey, request, traceId, RouteTrace.ignored());
    }

    public Flux<StreamChunk> executeStream(String apiKey, ChatRequest request, String traceId,
                                           RouteTrace trace) {
        if (request.messages() == null || request.messages().isEmpty()) {
            return Flux.error(new IllegalArgumentException("messages 不能为空"));
        }
        String question = questionOf(request);
        long estimatedTokens = tokenEstimator.estimatePrompt(request.messages());
        Route route;
        try {
            route = resolveRoute(request, trace);
        } catch (RuntimeException e) {
            // 同 execute：同步抛出的异常绕过 onErrorResume，会被 Spring 变成 500。
            // 流式路径同样需要这条转换，否则"未知任务"在流式客户端那里是 500、非流式是 400 ——
            // 同一个错误两种状态码，是最难向调用方解释的那类不一致。
            return Flux.error(e);
        }
        String namespace = cacheNamespace(apiKey, route.tier().model());

        return quotaService.tryConsume(apiKey, estimatedTokens)
                .flatMapMany(allowed -> {
                    if (!allowed) {
                        metrics.onQuotaRejected();
                        return Flux.error(new QuotaExceededException(apiKey));
                    }
                    return cacheService.lookup(namespace, question)
                            .flatMapMany(hit -> {
                                metrics.onCacheHit();
                                recordAsync(traceId, apiKey, request, route.task(),
                                        new ChatResponse("cache", "cache", hit.answer(),
                                                new Usage(0, 0), true),
                                        0.0, 0);
                                // 命中即全额退还预扣，与非流式路径同一条理由（见 execute）。
                                // 注意这里是 flatMapMany：退还完成后再把分片发出去，
                                // 保证"流结束 ⇒ 配额已结清"这个不变量成立。
                                // 模型名标记为 cache：客户端能从分片里看出这次没走上游。
                                //
                                // ★ 必须补收尾帧，不能只发 delta。
                                // 走上游的流在末帧一定带 finish_reason（F0），而缓存命中
                                // 只发一个 delta 的话，客户端拿到的是一条**没有收尾帧**的流：
                                // 累积 finish_reason 的实现（截断检测、JSON 输出是否完整）
                                // 只能得到 null，与"走上游但被截断"无法区分。
                                // 语义上 stop 是对的：网关交出去的是一份完整答案。
                                return settleQuota(apiKey, estimatedTokens, 0)
                                        .thenMany(Flux.just(
                                                StreamChunk.delta("cache", hit.answer()),
                                                StreamChunk.finish("cache", "stop")));
                            })
                            .switchIfEmpty(Flux.defer(() -> {
                                // 非流式分支里有 onCacheMiss，流式分支曾经漏了 ——
                                // 结果是指标统计偏低（实测 3 次请求只记到 2 次 miss）
                                metrics.onCacheMiss();
                                return streamDispatch(apiKey, request, question, route, traceId,
                                        estimatedTokens);
                            }));
                });
    }

    private Mono<ChatResponse> dispatch(String apiKey, ChatRequest request, String question,
                                         Route route, long start, String traceId, long reserved) {
        // 只在这里把 route 拆成 tier，下面整个函数体保持原样。
        // 逐处把 tier 改成 route.tier() 看起来更"干净"，但漏改一处不会编译报错，
        // 只会静默用错变量 —— 那种错误要靠真机才能发现。
        ModelTier tier = route.tier();
        long begin = System.currentTimeMillis();
        // 本次请求实际消耗的总 token，可能来自**多次**上游调用（重试、故障转移、级联升级）。
        // 用 AtomicLong 在这里汇合所有路径的用量，正好是配额结算需要的口径：
        // 不变量 C 说"两次调用就有两份费用"，那就该有两份 token 计入配额 ——
        // 账本与配额两个口径必须一致，否则对不上账时不知道信哪个。
        AtomicLong spentTokens = new AtomicLong(0);
        // 被重试 / 转移丢弃的尝试：它们同样真的花了钱，必须各自落一条账（不变量 C）。
        // 整个请求共用一个（含级联升级那一段），理由同 spentTokens 的注释。
        DiscardedAttempts discarded = new DiscardedAttempts(traceId, apiKey, request,
                route.task(), tier, spentTokens);

        return callWithFailover(tier.model(), request, discarded)
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
                        recordAsync(traceId, apiKey, request, route.task(),
                                new ChatResponse(response.id(), tier.model(), response.content(),
                                        firstUsage, false, response.finishReason()),
                                estimateCost(tier.model(), tier, firstUsage),
                                System.currentTimeMillis() - start);

                        ModelTier stronger = routerService.escalateFrom(tier);
                        // 升级后的这一次同样走重试 + 熔断 + 转移 —— 用的是**另一家** provider
                        // 的熔断器（按 provider 名分实例）。这正是 F2 要利用的形状：
                        // 便宜那家的熔断器打开了，不代表升级目标那家也不健康。
                        return callWithFailover(stronger.model(), request, discarded)
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
                    recordAsync(traceId, apiKey, request, route.task(), ledgerRow,
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
                })
                // 不变量 D（F1）：失败路径也必须结算。
                // 以前这里直接让异常穿过去，于是预扣那笔**永远留在账上** ——
                // 上游 502、熔断打开、连不上，全都白扣一次配额。
                // 用 spentTokens 而不是 reserved 作为 actual：级联升级时第一次调用
                // 已经真实花掉的钱必须照扣，否则升级前那次的成本就凭空消失了。
                // spentTokens 为 0 时由 chargedOnFailure 决定"退还还是保守保留"。
                .onErrorResume(e -> settleQuota(apiKey, reserved,
                                chargedOnFailure(e, spentTokens.get(), reserved))
                        .then(Mono.error(e)));
    }

    /**
     * 一次上游调用，带重试与熔断（F1）。
     *
     * <h3>为什么要在这里把"空正文"转成异常、再在末尾转回响应</h3>
     * 重试算子只认错误信号，而"HTTP 200 + 空正文 + finish_reason=length"在传输层
     * 是彻底的成功（F0 的四分表第一行说明它有多容易被当成正常响应）。
     * 所以进重试边界前把它翻译成 {@link UpstreamEmptyResponseException}，
     * 出边界后再翻译回来。
     *
     * <p>末尾那一步 <b>不是可选的</b>：重试耗尽后必须把上游那次**原始响应**交还给上层。
     * 少了它，客户端会从"200 + finish_reason=length + 空正文"退化成 502 ——
     * 那正是 F0 刚刚修掉的"把被截断伪装成正常结束"的镜像错误：
     * 这次是把"上游明确说明了原因"伪装成"网关坏了"。
     *
     * <p>还要注意<b>重试的每一次尝试都会真的花钱</b>。只为"最终交给客户端的那次"落账
     * 会让故障期的成本被系统性低估 —— 一次请求最多 4 次上游调用，账本却只有 1 行。
     * 现在由 {@link DiscardedAttempts} 补齐：拿到响应却没被交付的那些尝试
     * 各自落一条账，并计入配额（不变量 C / E）。
     *
     * @param attempts 本次允许的总尝试次数（含首次），F2 由故障转移的预算分配决定。
     *                 调用方不指定时用配置里的 {@code retry.max-attempts}。
     */
    private Mono<ChatResponse> callUpstream(ProviderAdapter adapter, String model, ChatRequest request,
                                            int attempts, DiscardedAttempts discarded) {
        // ★ Mono.defer 不是多余的包装，它是"重试 = 真的再发一次请求"这句承诺的实现。
        // adapter.chat() 返回的是一条**冷** Publisher，每次订阅才会真正出网；
        // 但"冷"是当前 OpenAiAdapter 的实现细节，不是 ProviderAdapter 接口的契约。
        // 万一将来有人写出一个会复用/缓存内部 Publisher 的适配器，
        // 重试就会变成"把上一次的旧响应再吐一遍"—— 看起来重试成功了，
        // 实际上一次请求都没发出去，而且没有任何报错。defer 把契约写进代码里。
        //
        // defer 里还有一件事：**结算上一次被丢弃的尝试**。
        // 能执行到这里，说明重试算子又订阅了一次，也就证明了上一次拿到的响应
        // 没有被交付给任何人 —— 它是真的被丢弃了，该落账。
        // "下一次尝试开始"是这里唯一可靠的判据：拿到响应的那一刻还无法知道
        // 会不会重试，而等整个请求结束再判断，又分不清哪一次成了最终答案。
        Mono<ChatResponse> attempt = Mono.defer(() -> {
            discarded.settleSuperseded();
            return adapter.chat(model, request);
        })
                .map(resp -> {
                    if (UpstreamOutcome.of(resp.content(), resp.finishReason()).empty()) {
                        // 先登记再抛：这份响应已经有 usage 了（钱已经花了），
                        // 但最终会不会被重试丢掉，此刻还不知道。登记之后由
                        // "下一次尝试开始"或 terminal 的 adopt 来定它是哪种。
                        discarded.hold(resp);
                        throw new UpstreamEmptyResponseException(resp);
                    }
                    return resp;
                });
        // ★ 注意这里**没有**把 UpstreamEmptyResponseException 还原成响应 ——
        // F1 时代还原放在这一层，F2 必须把它挪到整条候选链的末端（见 terminal）。
        // 理由：还原 = "放弃这次请求、如实把上游说的原因交给客户端"，
        // 而"放弃"是一个**跨候选**的决定 —— 在这家被还原成正常响应之后，
        // 外层拿到的就是一个成功值，再没有机会去问下一家。
        // 空正文恰恰是最该换一家试试的失败（模型侧问题，不是链路问题），
        // 所以判定必须推迟到"确认没有下一个候选"的那一刻。
        return resilience.protect(adapter.name(), attempt, attempts);
    }

    /**
     * 整条候选链的终点：把"不值得继续转移"的错误按最终形态交出去。
     *
     * <p>这里是 F0 的透传与 F2 的转移接壤的地方，顺序不能反：
     * <b>先穷尽转移，再还原响应</b>。
     * 反过来（在单次调用内部就还原）会让空正文变成一个成功值，
     * 外层再没有机会问下一家 —— 一个"换个部署就能拿到正常答案"的请求
     * 于是被当成"上游明确说了它答不出"直接交付给客户端。
     */
    private Mono<ChatResponse> terminal(Throwable e, DiscardedAttempts discarded) {
        if (e instanceof UpstreamEmptyResponseException empty) {
            // 重试与转移都试过了，上游仍然只给出空正文 —— 这时必须把那份**原始响应**
            // 交还给上层，而不是 502。少了这一步，客户端会从
            // "200 + finish_reason=length + 空正文"退化成"网关坏了"，
            // 那正是 F0 修掉的错误的镜像版本。
            //
            // 这份响应**不是被丢弃的**：它就是最终交给客户端的那个，
            // 会由正常路径落一条账。所以这里要把"待定"标记撤掉，
            // 否则它会被记两次（一次按丢弃、一次按最终）。
            discarded.adopt();
            return Mono.just(empty.response());
        }
        return Mono.error(e);
    }

    // ---------------------------------------------------------------- F2 故障转移

    /**
     * 在模型的候选链上做故障转移，直到拿到一个响应（F2）。
     *
     * <h3>与 F1 的分工</h3>
     * F1 的问题域是"<b>同一家</b>上游：再试一次还是直接放弃"；
     * F2 的问题域是"<b>换一家</b>问：这家不行的时候，候选链上还有谁"。
     * 两者是同一个失败的两种处置方式，所以共用一套错误分类（
     * {@code failoverable ⊇ retryable}），但边界完全分开：
     * F1 在 {@link UpstreamResilience} 里面，F2 在它外面（本方法）。
     *
     * <h3>为什么预算是"扣减式"而不是"事后统计"</h3>
     * 每次尝试之前先把这家要用的次数从总预算里扣掉，扣多少按<b>能用的最多次数</b>算
     * （{@code min(attemptsPerCall, 剩余预算)}），而不是等它真的失败了几次再补扣。
     * 因为重试算子内部的计数对外不可见 —— 想知道"这次实际用了几次"就得去问它，
     * 而那意味着把 resilience4j 的内部状态泄漏到编排层。
     * 扣减法宁可少给一次机会（链首之外通常只分到 1 次），换一个可以静态算出的上限：
     * <b>任何一次请求的上游调用次数都不会超过 {@code max-total-attempts}</b>，
     * 无论链路怎么坏。故障期的账单能算出来，比多给备选一次重试重要得多。
     */
    private Mono<ChatResponse> callWithFailover(String model, ChatRequest request,
                                                DiscardedAttempts discarded) {
        List<ProviderAdapter> chain = providers.candidatesFor(model);
        int limit = failoverLimit(chain);
        AtomicInteger budget = new AtomicInteger(totalAttemptBudget());
        return attemptChain(chain, limit, 0, budget, model, request, discarded);
    }

    private Mono<ChatResponse> attemptChain(List<ProviderAdapter> chain, int limit, int idx,
                                            AtomicInteger budget, String model, ChatRequest request,
                                            DiscardedAttempts discarded) {
        int i = nextCallable(chain, idx, limit, model);
        if (i < 0) {
            return Mono.error(UpstreamCircuitOpenException.allCandidatesUnavailable(
                    model, namesOf(chain, limit)));
        }
        ProviderAdapter adapter = chain.get(i);
        int attempts = allocate(budget);
        // 预算是否已经在这次分配后用尽 —— 先算好，错误处理里只看这个布尔量。
        boolean budgetSpent = budget.get() <= 0;

        // 转移决策全部放在 onErrorResume 里（错误发生时）而不是装配期：
        // ① 装配期去挑"下一个候选"会在明明不打算转移的情况下也去查一遍熔断状态，
        //    顺手把 circuit.rejected 记到那些根本没轮到它上场的 provider 头上；
        // ② 挑不到下一个候选时应当把**原始错误**上抛，而不是换成一个
        //    "全部不可用"的新错误 —— 后者会把"上游真的报了 500"这个事实抹掉。
        return callUpstream(adapter, model, request, attempts, discarded)
                .onErrorResume(e -> {
                    if (budgetSpent) {
                        return terminal(e, discarded);
                    }
                    if (!UpstreamResilience.failoverable(e)) {
                        // 确定性错误（4xx / 配额 / 参数非法）：换一家会拿到一模一样的错误，
                        // 转移只是把同一份失败再买一次。直接上抛，让错误如实到达客户端。
                        return terminal(e, discarded);
                    }
                    int next = nextCallable(chain, i + 1, limit, model);
                    if (next < 0) {
                        return terminal(e, discarded);
                    }
                    ProviderAdapter to = chain.get(next);
                    metrics.onFailover(adapter.name(), to.name());
                    log.warn("failing over [model={} from={} to={}]: {}",
                            model, adapter.name(), to.name(), e.toString());
                    return attemptChain(chain, limit, next, budget, model, request, discarded);
                });
    }

    /**
     * 被重试 / 转移丢弃的尝试 —— 它们的钱真的花了，但不会成为最终答案。
     *
     * <h3>为什么需要单独的判据</h3>
     * 上游回了一个响应（带 usage），但这个响应最终没有被交付给客户端：可能是空正文触发了
     * 重试，也可能是这家坏了换了下一家。不变量 C 说"每一次真实发生的上游调用都要落一条账"，
     * 所以这些尝试必须各自落账 —— 否则一次请求 4 次上游调用、账本只有 1 行，
     * 成本在故障期被系统性低估，而故障期恰恰是账单最难看的时候。
     *
     * <h3>判据为什么是"下一次尝试开始"</h3>
     * 拿到响应的那一刻还不知道会不会重试，所以只能先 {@link #hold} 住；
     * 等重试算子重新订阅（{@code Mono.defer} 再次执行）时，才算证明"上一次没有被交付"。
     * 如果最终没有再重试，那这条响应会由 {@link #adopt} 认领为最终答案，
     * 由正常路径落账 —— 两条路互斥，不会重复记账。
     *
     * <h3>为什么不做成全局的"每次尝试都立即落账"</h3>
     * 那样每一次订阅都要写一条账，包括**成功交付**的那一次，于是正常的单次请求
     * 也会写两行（一次按尝试、一次按最终）。判据放在"是否被取代"上，
     * 才能保证"一次请求的账本行数 = 真实上游调用次数"这个可以直接对账的口径。
     *
     * <p>只在非流式路径使用：流式拿不到被丢弃尝试的 usage（末帧没到就断了），
     * 没有可记的数字；流式的用量口径见 {@code streamDispatch}。
     */
    private final class DiscardedAttempts {

        private final String traceId;
        private final String apiKey;
        private final ChatRequest request;
        /** 本次请求的任务，原样带进账本 —— 被丢弃的尝试也属于同一个任务。 */
        private final String task;
        private final ModelTier tier;
        private final AtomicLong spentTokens;
        private final AtomicReference<ChatResponse> pending = new AtomicReference<>();

        DiscardedAttempts(String traceId, String apiKey, ChatRequest request, String task,
                          ModelTier tier, AtomicLong spentTokens) {
            this.traceId = traceId;
            this.apiKey = apiKey;
            this.request = request;
            this.task = task;
            this.tier = tier;
            this.spentTokens = spentTokens;
        }

        /** 登记"已收到、可能被丢弃"的响应。 */
        void hold(ChatResponse response) {
            pending.set(response);
        }

        /**
         * 下一次尝试即将开始 → 上一次登记的响应确认被丢弃，落账并计入配额。
         *
         * <p>延迟记 0：单次尝试的耗时没有单独计时，而这一行的作用是成本，
         * 不是延迟分析（延迟由 {@code metrics.recordLatency} 单独统计，
         * 口径是"整条请求"，两者不该混）。
         */
        void settleSuperseded() {
            ChatResponse stale = pending.getAndSet(null);
            if (stale == null) {
                return;
            }
            Usage usage = usageOrEstimate(request, stale);
            double cost = estimateCost(stale.model(), tier, usage);
            recordAsync(traceId, apiKey, request, task, stale, cost, 0);
            spentTokens.addAndGet(usage.total());
            log.warn("upstream attempt discarded by retry/failover and billed [model={} "
                            + "finish_reason={} usage={}]",
                    stale.model(), stale.finishReason(), usage);
        }

        /** 最终答案就是这条登记过的响应 —— 它会被正常路径落账，这里只撤掉待定标记。 */
        void adopt() {
            pending.set(null);
        }
    }

    /**
     * 流式版的候选链转移（F2）。
     *
     * <h3>为什么 {@code upstreamProduced} 必须由调用方传进来、而不是每条流各自新建</h3>
     * 这是 F2 最容易写错、而且错了以后<b>看起来一切正常</b>的一处。
     * F1 里这个标记的生命周期是"一次 {@code protectStream} 调用"，因为那时候候选只有一家。
     * 到了 F2，如果每次换 provider 都新建一个标记，就会变成：
     * <pre>
     *   a 吐了 3 个正文分片 → 报错 → 新标记是 false → "闸门是开的" → 换 b 重放一遍
     * </pre>
     * 客户端于是收到<b>两段拼接的答案</b>，而服务端日志里只有一条漂亮的
     * "failing over a→b、最终成功"。F1 把重试的重复推送防住了，F2 如果
     * 各建各的标记就会从故障转移这条新路径上把它放回来 ——
     * 所以这个 AtomicBoolean 必须<b>跨整个请求</b>共用，包括级联升级那一段。
     */
    private Flux<StreamChunk> streamWithFailover(String model, ChatRequest request,
                                                 AtomicBoolean upstreamProduced) {
        List<ProviderAdapter> chain = providers.candidatesFor(model);
        int limit = failoverLimit(chain);
        AtomicInteger budget = new AtomicInteger(totalAttemptBudget());
        return attemptStreamChain(chain, limit, 0, budget, model, request, upstreamProduced);
    }

    private Flux<StreamChunk> attemptStreamChain(List<ProviderAdapter> chain, int limit, int idx,
                                                 AtomicInteger budget, String model,
                                                 ChatRequest request, AtomicBoolean upstreamProduced) {
        int i = nextCallable(chain, idx, limit, model);
        if (i < 0) {
            return Flux.error(UpstreamCircuitOpenException.allCandidatesUnavailable(
                    model, namesOf(chain, limit)));
        }
        ProviderAdapter adapter = chain.get(i);
        int attempts = allocate(budget);
        boolean budgetSpent = budget.get() <= 0;
        Flux<StreamChunk> attempt = resilience.protectStream(adapter.name(),
                // Flux.defer 的理由同 callUpstream：把"每次重试/转移都重新发起一次
                // 流式调用"写成代码里的承诺，而不是依赖适配器的"冷 Publisher"这个实现细节。
                Flux.defer(() -> adapter.chatStream(model, request)),
                upstreamProduced, attempts);
        return attempt.onErrorResume(e -> {
            // ★ 闸门：只要有任何一帧到达过客户端，就绝不许换 provider。
            // 判断依据用 upstreamProduced（任何帧都算）而不是 emitted（只有正文帧算）：
            // 转移比重试更"重"（换的是另一家的模型），只要有过任何输出，
            // 重放都会让客户端看到两段不同来源的内容，宁可不救。
            if (upstreamProduced.get() || budgetSpent) {
                return Flux.error(e);
            }
            if (!UpstreamResilience.failoverable(e)) {
                return Flux.error(e);
            }
            int next = nextCallable(chain, i + 1, limit, model);
            if (next < 0) {
                return Flux.error(e);
            }
            ProviderAdapter to = chain.get(next);
            metrics.onFailover(adapter.name(), to.name());
            log.warn("failing over stream [model={} from={} to={}]: {}",
                    model, adapter.name(), to.name(), e.toString());
            return attemptStreamChain(chain, limit, next, budget, model, request, upstreamProduced);
        });
    }

    /**
     * 返回 idx 起第一个"值得调用"的候选下标；顺手把跳过的那些记进指标与日志。
     *
     * <p>跳过熔断打开的候选而不是"让它自己抛错再转移"，是为了<b>不浪费预算</b>：
     * 一次会被熔断器瞬间拒绝的调用不会真的出网，但仍然要按分配额扣掉重试预算，
     * 于是备选只剩 1 次机会。主力链路长时间不可用时，那意味着**每一个请求**
     * 都会被削掉重试能力 —— 一个由"白扣预算"引起的、静默的体验退化。
     */
    private int nextCallable(List<ProviderAdapter> chain, int from, int limit, String model) {
        for (int i = Math.max(0, from); i < limit; i++) {
            ProviderAdapter adapter = chain.get(i);
            if (resilience.callable(adapter.name())) {
                return i;
            }
            metrics.onCircuitRejected(adapter.name());
            log.warn("skipping provider whose circuit is open [model={} provider={}]", model,
                    adapter.name());
        }
        return -1;
    }

    private int failoverLimit(List<ProviderAdapter> chain) {
        var fo = properties.getResilience().getFailover();
        if (!fo.isEnabled()) {
            return 1;
        }
        return Math.max(1, Math.min(chain.size(), fo.getMaxProviders()));
    }

    private int totalAttemptBudget() {
        var fo = properties.getResilience().getFailover();
        return fo.isEnabled() ? Math.max(1, fo.getMaxTotalAttempts()) : Integer.MAX_VALUE;
    }

    /** 从总预算里扣掉"这家最多会用掉几次"，返回分给它的次数上限。 */
    private int allocate(AtomicInteger budget) {
        int perCall = resilience.attemptsPerCall();
        int granted = Math.min(perCall, Math.max(1, budget.get()));
        budget.addAndGet(-granted);
        return granted;
    }

    private static List<String> namesOf(List<ProviderAdapter> chain, int limit) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, chain.size()); i++) {
            names.add(chain.get(i).name());
        }
        return names;
    }

    private Flux<StreamChunk> streamDispatch(String apiKey, ChatRequest request, String question,
                                              Route route, String traceId, long reserved) {
        ModelTier tier = route.tier();
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
        // F1：上游是否**真的**产出过分片。一个标记服务三个判断，三者是同一个事实：
        //   ① 重试闸门 —— 一个分片都没产出时才允许重试（否则会重复推送内容）
        //   ② 故障转移闸门（F2）—— 同理，但边界更严：**跨 provider 共用一个标记**
        //   ③ 失败时的配额口径 —— 产出过就说明确实花了钱，不能退预扣
        // 交给 UpstreamResilience 写入：它把标记挂在重试边界**之外**，
        // 因此被重试丢弃的那次尝试产出的分片不会被算进来。
        AtomicBoolean upstreamProduced = new AtomicBoolean(false);

        Flux<StreamChunk> primary = streamWithFailover(tier.model(), request, upstreamProduced)
                .doOnNext(chunk -> collect(chunk, emitted, collected, upstreamUsage, finishReason));

        Flux<StreamChunk> body = primary
                // 已输出内容后不再切换模型或 provider，避免用户看到两段拼接的答案
                .onErrorResume(e -> emitted.get()
                        ? Flux.<StreamChunk>error(e)
                        : Flux.defer(() -> {
                            metrics.onEscalate();
                            ModelTier stronger = routerService.escalateFrom(tier);
                            servedBy.set(stronger.model());
                            // 换了模型，上一次调用留下的 finish_reason 必须清掉：
                            // 那是"便宜模型为什么被换掉"的原因，不是最终答案的原因。
                            finishReason.set(null);
                            // 升级后的流同样走重试 + 熔断 + 转移，用那家 provider 自己的熔断器。
                            // 复用同一个 upstreamProduced：便宜那家一个分片都没给出来过，
                            // 所以闸门在这里仍然是开的 —— 这正是我们还能升级的前提。
                            return streamWithFailover(stronger.model(), request, upstreamProduced)
                                    .doOnNext(chunk -> collect(chunk, emitted, collected,
                                            upstreamUsage, finishReason));
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
                    recordAsync(traceId, apiKey, request, route.task(), snapshot,
                            estimateCost(model, tier, usage), latency);
                    writeCache(cacheNamespace(apiKey, model), question, collected.toString());
                    settleAmount.set(usage.total());
                });

        // 结算串在流的**末尾**而不是丢进 doOnComplete 里 fire-and-forget。
        // 理由是那个可验证的不变量："客户端收到 [DONE] ⇒ 配额已结清"。
        // 若用 doFinally 会把"客户端中途断开"也当成完成去结算 —— 而那种情况下
        // 上游已经花了钱，预扣本就该留着。
        Flux<StreamChunk> settled = body.concatWith(Flux.defer(() -> {
            long amount = settleAmount.get();
            if (amount < 0) {
                return Flux.empty();
            }
            return settleQuota(apiKey, reserved, amount).thenMany(Flux.<StreamChunk>empty());
        }));

        // 不变量 D（F1）：流断掉也必须结算。
        // 注意这里用的是**流式**口径而不是非流式的 spentTokens：
        // 流式的真实用量只在末帧给出，流一旦断掉就永远等不到那一帧，
        // 所以"已经花了多少"只能靠"有没有分片出来过"来推断。
        // 熔断打开 / 连不上这两种"确定一分钱没花"的情况由 chargedOnFailure 退还。
        return settled.onErrorResume(e -> {
            long known = upstreamUsage.get() == null ? 0 : upstreamUsage.get().total();
            return settleQuota(apiKey, reserved, chargedOnFailure(e, known, reserved))
                    .thenMany(Flux.error(e));
        });
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

    /**
     * 解析路由，并在这里统一记录"为什么这么选"。
     *
     * <p>流式与非流式都经过这里，所以日志和指标只写一处。如果分别放进
     * dispatch / streamDispatch，很快就会出现"流式那条忘了记"的静默分叉 ——
     * 而这种分叉在单元测试里往往也是各测一条、看不出来。
     *
     * <p>这条日志是排查"这个请求为什么走了贵模型"的第一个入口：它同时给出
     * 判定出的任务、判定来源（显式 / 推断 / 兜底）和具体命中的规则。
     */
    private Route resolveRoute(ChatRequest request, RouteTrace trace) {
        Route route = routerService.resolve(request);
        // 先写回执再记指标/日志：回执的消费者（响应头）在写 body 之前就要读到它。
        // 这里不能反过来依赖"日志已经打了"，日志是给人看的，回执是给程序用的。
        trace.record(route);
        metrics.onRoute(route.task(), route.source());
        log.info("route: model={} task={} source={} reason={}",
                route.tier().model(), route.task(), route.source(), route.reason());
        return route;
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

    /**
     * 一次调用的成本。优先用配好的单价（输入/输出分开计），没有才回退到档位的粗略单价。
     *
     * <p>★ 回退时必须按<b>实际服务的模型</b>去查档位，不能用调用方手上的 tier。
     * 级联升级之后这两者已经不是一回事：{@code tier} 是"请求开始时选的便宜档"，
     * 而 {@code model} 是"最终服务的那家强模型"。拿旧 tier 兜底会让升级后的那次调用
     * 按便宜模型计价 —— 成本被系统性低估，而升级恰恰发生在"便宜的答不好"的场景，
     * 也就是账本最需要准的地方。而且这种错不会报错，只会在月底对账时表现为
     * "总比账单少一点"，很难追到这一行。
     *
     * <p>只有模型不在候选表里（客户端显式指定了一个未纳入路由的模型）时，
     * 才退回调用方给的那个档位 —— 那时它是唯一可用的信息。
     *
     * <p>包级可见是为了让计费口径能被直接断言：账本写入走的是
     * {@code recordAsync}（fire-and-forget + 独立线程），从测试里等它落下来本身就是
     * 不可靠的；而"算得对不对"和"写得进不进得去"是两件事，该分开验。
     */
    double estimateCost(String model, ModelTier tier, Usage usage) {
        GatewayProperties.Price price = properties.getPricing().get(model);
        if (price != null) {
            return price.costOf(usage.promptTokens(), usage.completionTokens());
        }
        double per1k = properties.getRouter().getCandidates().stream()
                .filter(c -> model.equals(c.getModel()))
                .mapToDouble(GatewayProperties.Candidate::getCostPer1k)
                .findFirst()
                .orElse(tier.costPer1k());
        return usage.total() / 1000.0 * per1k;
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
     * 失败路径上"这次请求到底该记多少 token"（F1，不变量 D 的另一半）。
     *
     * <h3>为什么不能简单地在失败时退掉整笔预扣</h3>
     * 直觉上"请求失败了就该退钱"，但上游对失败的请求并非都不计费：
     * <ul>
     *   <li>上游 5xx：请求已经送到，prompt 部分大概率已经计费。</li>
     *   <li>首片超时：上游可能正在生成，费用已经在产生。</li>
     *   <li>流中途断掉：前面那些分片是真实产出的 token。</li>
     * </ul>
     * 一律退还等于把"真实花掉的钱"记成免费，而且这条路径恰恰集中在故障期 ——
     * 退得最多的时候正是最需要配额起到止血作用的时候。
     *
     * <h3>但也不能一律保留预扣</h3>
     * 因为熔断正是"连续失败"打开的，而熔断打开的请求<b>一个字节都没发出去</b>。
     * 一次都不退还的话，上游整体挂掉的那段时间里，每个被拒的请求都会白吃掉
     * 一整笔预扣 —— F1 引入的新失败模式反而成了最大的配额泄漏点。
     *
     * <h3>取舍规则：只在"确定没花钱"时才退还，不确定时保留预扣</h3>
     * 两个方向的错都不是一样重：少退一次 = 用户多扣了一点额度（可事后补偿），
     * 多退一次 = 故障期的配额上限失效（不可逆）。所以默认取保守方向，
     * 只有能<b>确定</b>请求没到上游时才退还。
     *
     * @param knownUsage 已经拿到的真实用量（非流式是 spentTokens，流式是末帧 usage）；拿不到传 0
     * @param reserved   本次请求的预扣额，作为"不知道花了多少"时的保守估计
     */
    private static long chargedOnFailure(Throwable e, long knownUsage, long reserved) {
        if (knownUsage > 0) {
            // 已经有真实用量（级联升级时第一次调用的用量）—— 用它，别猜。
            return knownUsage;
        }
        if (neverReachedUpstream(e)) {
            return 0;
        }
        return reserved;
    }

    /**
     * 这次失败是否<b>确定</b>没有到达上游、因此确定没有产生费用。
     *
     * <p>只列两种能"确定"的情况，其余一律当成"可能已经计费"：
     * <ul>
     *   <li>熔断打开 —— 请求在网关内部就被拦下了，连 TCP 连接都没建。</li>
     *   <li>{@link WebClientRequestException} —— 连接被拒 / DNS 失败 / TLS 握手失败，
     *       请求到不了模型。注意这个异常也覆盖"响应读到一半连接被重置"，
     *       那种情况下上游其实可能已经计费了 —— 这是本判断刻意接受的一处低估，
     *       因为它在真实故障里占比极低，而为它单独加一层区分会让规则难以解释。</li>
     * </ul>
     * 特别地，{@code TimeoutException} 不在此列：请求已经发出去了，上游可能正在算。
     */
    private static boolean neverReachedUpstream(Throwable e) {
        return e instanceof UpstreamCircuitOpenException
                || e instanceof WebClientRequestException;
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
    private void recordAsync(String traceId, String apiKey, ChatRequest request, String task,
                             ChatResponse response, double cost, long latencyMs) {
        Usage u = response.usage() == null ? new Usage(0, 0) : response.usage();
        Mono.fromRunnable(() -> ledger.record(traceId, apiKey, request.feature(), task,
                        response.model(), response.cacheHit(), u.promptTokens(),
                        u.completionTokens(), cost, latencyMs))
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
