package com.llmgw.core.pipeline;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.cost.CostLedgerService;
import com.llmgw.core.protocol.ProviderAdapter;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.protocol.StreamChunk;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.resilience.UpstreamCircuitOpenException;
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.Route;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.token.TokenEstimator;
import com.llmgw.observability.GatewayMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * F2：跨 provider 故障转移。
 *
 * <h3>这一组测试守着什么</h3>
 * F2 引入的失败模式有个共同特征：<b>出问题时一切看起来都正常</b>。
 * 转移本身是"救活请求"，所以坏掉的转移不会以报错的形式暴露，只会表现为
 * 答案被重放了一遍、账单悄悄放大、或者本该转移的时候没转移。
 * 所以这里每一条断言都对着一个"看不出来的坏法"：
 *
 * <ol>
 *   <li><b>答案拼接</b>（{@link #streamIsNotFailedOverAfterAChunkReachedTheClient}）：
 *       换 provider 时如果各自新建"是否已产出"的标记，客户端会看到两段不同来源的答案，
 *       而日志里只有一条漂亮的"转移成功"。这是 F2 最可能的真实回归点。</li>
 *   <li><b>账单被乘法放大</b>（{@link #totalAttemptsAcrossProvidersNeverExceedsTheBudget}）：
 *       {@code maxAttempts × 候选家数}。故障期账单爆炸属于事后最难解释的事故。</li>
 *   <li><b>该转移却没转移</b>（{@link #secondProviderServesTheRequestWhenTheFirstOneFails}）：
 *       熔断与转移必须成对 —— 只有熔断的话，一次链路故障等于整站不可用。</li>
 *   <li><b>不该转移却转移了</b>（{@link #deterministicClientErrorIsNotRetriedOnAnotherProvider}）：
 *       4xx 换一家会拿到一模一样的错误，转移只是把同一份失败多买一次。</li>
 * </ol>
 *
 * <p>用<b>真实的</b> UpstreamResilience：F2 的被测对象正是"重试内、转移外"这个组合语义，
 * 把 resilience mock 掉就等于把被测对象换成了桩。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineFailoverTest {

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private ProviderAdapter adapterA;
    @Mock private ProviderAdapter adapterB;

    private final GatewayProperties props = new GatewayProperties();
    private final TokenEstimator estimator = new TokenEstimator(props);
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());

    private ChatPipeline pipeline;

    private static final String KEY = "tenant-a";
    private static final String MODEL = "shared-model";
    private static final String PROVIDER_A = "provider-a";
    private static final String PROVIDER_B = "provider-b";

    @BeforeEach
    void setUp() {
        props.getCache().setEnabled(true);
        var r = props.getResilience().getRetry();
        r.setMaxAttempts(3);
        r.setWaitDurationMillis(1);
        r.setBackoffMultiplier(1.0);
        r.setMaxWaitDurationMillis(1);
        // 熔断窗口默认放大：绝大多数用例测的是转移，不该被熔断顺手插手
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(100);
        cb.setMinimumNumberOfCalls(100);

        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(cacheService.write(anyString(), anyString(), anyString())).thenReturn(Mono.empty());

        when(adapterA.name()).thenReturn(PROVIDER_A);
        when(adapterB.name()).thenReturn(PROVIDER_B);
        // ★ 顺序就是候选优先顺序：A 是主力，B 是备选
        when(providers.candidatesFor(MODEL)).thenReturn(List.of(adapterA, adapterB));

        when(routerService.shouldEscalate(any(ModelTier.class), any(), anyBoolean())).thenReturn(false);
        when(routerService.escalateFrom(any(ModelTier.class)))
                .thenReturn(new ModelTier(MODEL, "CHEAP", 0.0, 0.5));
        // 任务路由：本组测的是转移与预算，把路由固定成单模型 ——
        // 让任务判定插手会把"调用了几次"的断言变得难以归因。
        when(routerService.resolve(any())).thenReturn(
                new Route(new ModelTier(MODEL, "CHEAP", 0.0, 0.5), "chat", "fallback", "test"));

        buildPipeline();
    }

    private void buildPipeline() {
        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, metrics, props, estimator, new UpstreamResilience(props, metrics));
    }

    private ChatRequest request() {
        return new ChatRequest(MODEL, false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", "什么是故障转移")), "test", null);
    }

    /** 声明成 long：estimatePrompt 若返回 int，{@code List.of(...)} 会变成 List&lt;Integer&gt;，
     *  与捕获到的 List&lt;Long&gt; 元素相等但列表不相等 —— 断言会以"看到 [34] vs [34]"的形式失败。 */
    private long reserved() {
        return estimator.estimatePrompt(request().messages());
    }

    private static ChatResponse answer(String content) {
        return new ChatResponse("id", MODEL, content, new Usage(20, 30), false, "stop");
    }

    private static WebClientResponseException serverError() {
        return WebClientResponseException.create(500, "boom", HttpHeaders.EMPTY, new byte[0], null);
    }

    private static WebClientResponseException badRequest() {
        return WebClientResponseException.create(400, "bad", HttpHeaders.EMPTY, new byte[0], null);
    }

    // --- 基本转移 ---

    /**
     * 主力挂掉 → 备选透明接管，客户端看到的是 200 + 备选的答案。
     *
     * <p>断言 A 被调用 3 次（首次 + 2 次重试）而不止 1 次：这说明"重试在内、
     * 转移在外"这个嵌套顺序是对的 —— 先在同一家把该试的试完，再换地方。
     * 反过来（一上来就换家）会表现为 A 只被调 1 次。
     */
    @Test
    void secondProviderServesTheRequestWhenTheFirstOneFails() {
        AtomicInteger callsA = new AtomicInteger();
        AtomicInteger callsB = new AtomicInteger();
        when(adapterA.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsA.incrementAndGet();
            return Mono.error(serverError());
        });
        when(adapterB.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsB.incrementAndGet();
            return Mono.just(answer("B 的答案"));
        });

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f1"))
                .expectNextMatches(r -> "B 的答案".equals(r.content()))
                .verifyComplete();

        assertEquals(3, callsA.get(), "主力应该先在自己的重试预算内试满");
        assertEquals(1, callsB.get(), "备选只需要一次就够（剩余预算只够一次）");
    }

    /**
     * 4xx 不转移。
     *
     * <p>确定性错误换一家 provider 会拿到一模一样的错误 —— 网关发出去的请求没变，
     * 而 400 的含义就是"这个请求不对"。转移在这里是纯浪费：
     * 多付一次调用，客户端的错误信息还是一样的。
     */
    @Test
    void deterministicClientErrorIsNotRetriedOnAnotherProvider() {
        AtomicInteger callsA = new AtomicInteger();
        when(adapterA.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsA.incrementAndGet();
            return Mono.error(badRequest());
        });

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f2"))
                .expectErrorSatisfies(e -> assertTrue(e instanceof WebClientResponseException))
                .verify();

        assertEquals(1, callsA.get(), "400 是确定性的，重试都没有意义");
        verify(adapterB, never()).chat(anyString(), any());
    }

    /**
     * 跨 provider 的总调用次数有上限。
     *
     * <p>默认 {@code max-attempts=3} + {@code max-total-attempts=4}：
     * 主力拿 3 次，备选只分到剩下的 1 次。这个上限的作用是关住
     * {@code maxAttempts × 候选家数} 这个乘法 —— 没有它，3 家候选就是 9 次真实计费调用。
     *
     * <p>注意断言的是<b>总次数</b>而不是"主力 3 次 + 备选 1 次"这种分配细节：
     * 分配策略将来可能调整（比如按健康度加权），但总量上限是这个配置项的全部意义。
     */
    @Test
    void totalAttemptsAcrossProvidersNeverExceedsTheBudget() {
        AtomicInteger callsA = new AtomicInteger();
        AtomicInteger callsB = new AtomicInteger();
        when(adapterA.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsA.incrementAndGet();
            return Mono.error(serverError());
        });
        when(adapterB.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsB.incrementAndGet();
            return Mono.error(serverError());
        });

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f3"))
                .expectError()
                .verify();

        assertEquals(4, callsA.get() + callsB.get(),
                "跨 provider 的调用总次数必须落在 max-total-attempts 里");
        assertEquals(4, props.getResilience().getFailover().getMaxTotalAttempts());
    }

    /** 关掉转移时行为退回 F1：只用链首，备选一次都不碰。这是一键回退开关的意义。 */
    @Test
    void disablingFailoverFallsBackToSingleProviderBehaviour() {
        props.getResilience().getFailover().setEnabled(false);
        buildPipeline();
        when(adapterA.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f4"))
                .expectError()
                .verify();

        verify(adapterB, never()).chat(anyString(), any());
    }

    /**
     * 空正文也触发转移。
     *
     * <p>"上游 200 但模型什么都没产出"是模型侧的问题 —— 换一家部署完全可能正常。
     * 这条断言把"空正文属于可转移错误"钉住：它很容易在重构谓词时被漏掉，
     * 因为它在传输层是成功（F0 的四分表第一行），
     * 一个只按"HTTP 状态码"分类的谓词永远看不见它。
     */
    @Test
    void emptyAnswerTriggersFailover() {
        when(adapterA.chat(eq(MODEL), any()))
                .thenReturn(Mono.just(new ChatResponse("id", MODEL, "", new Usage(33, 64), false, "length")));
        when(adapterB.chat(eq(MODEL), any())).thenReturn(Mono.just(answer("B 正常产出")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f5"))
                .expectNextMatches(r -> "B 正常产出".equals(r.content()))
                .verifyComplete();
    }

    // --- 熔断打开的候选：跳过而不要白烧预算 ---

    /**
     * 主力熔断打开之后：请求<b>直接</b>落到备选，且主力的适配器一次都不再被调用。
     *
     * <p>这条是"熔断 + 转移成对存在"的完整形态：熔断负责"不再问这家"，
     * 转移负责"那接下来问谁"。少了转移，熔断打开就等于整站 503；
     * 少了熔断，转移会对着一条已经确认坏掉的链路反复冲。
     *
     * <p>实现上还额外要求<b>跳过</b>而不是"让它抛错再转移"：被熔断器拒绝的调用
     * 根本不出网，但仍然会按分配额扣掉预算，于是备选只剩 1 次机会 ——
     * 主力长时间不可用的时候，每个请求都会被静默削掉重试能力。
     *
     * <p>用 400 而不是 500 来把 A 的熔断器打开：500 是可转移错误，
     * 一旦转移 B 也会跟着吃失败、跟着被熔断，就分不清"跳过"和"没得跳"了。
     * （顺带暴露出一个真实边界：4xx 目前也会计入熔断窗口，见 docs/step-20。）
     */
    @Test
    void providerWithOpenCircuitIsSkippedWithoutCallingIt() {
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(2);
        cb.setMinimumNumberOfCalls(2);
        props.getResilience().getRetry().setEnabled(false);
        buildPipeline();

        AtomicInteger callsA = new AtomicInteger();
        AtomicInteger callsB = new AtomicInteger();
        when(adapterA.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsA.incrementAndGet();
            return Mono.error(badRequest());
        });
        when(adapterB.chat(eq(MODEL), any())).thenAnswer(inv -> {
            callsB.incrementAndGet();
            return Mono.just(answer("B 接管"));
        });

        // 前两轮：A 报 400（不可转移，B 不会被碰），两次失败把 A 的滑动窗口填满
        for (int i = 0; i < 2; i++) {
            StepVerifier.create(pipeline.execute(KEY, request(), "trace-f6-" + i))
                    .expectError().verify();
        }
        assertEquals(2, callsA.get());
        assertEquals(0, callsB.get(), "400 不该触发转移");

        // 第三轮：A 已熔断 → 直接走 B
        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f6-3"))
                .expectNextMatches(r -> "B 接管".equals(r.content()))
                .verifyComplete();

        assertEquals(2, callsA.get(), "熔断打开后不该再往 A 发请求 —— 哪怕一次");
        assertEquals(1, callsB.get());
    }

    /**
     * 全部候选都熔断打开 → 一次调用都不发，报 503 语义的错（F1 的异常类型）。
     *
     * <p>复用 UpstreamCircuitOpenException 而不是新开一个类型：对客户端来说
     * "整个候选链都不健康"和"那一家不健康"的应对完全一样（退避再来），
     * 多一个类型只会在状态码映射、错误类型、指标三处各加一条永远只有一种走法的分支。
     */
    @Test
    void allCandidatesUnavailableIsReportedWithoutCallingAnyone() {
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(1);
        cb.setMinimumNumberOfCalls(1);
        props.getResilience().getRetry().setEnabled(false);
        buildPipeline();

        AtomicInteger calls = new AtomicInteger();
        when(adapterA.chat(eq(MODEL), any())).thenAnswer(inv -> {
            calls.incrementAndGet();
            return Mono.error(serverError());
        });
        when(adapterB.chat(eq(MODEL), any())).thenAnswer(inv -> {
            calls.incrementAndGet();
            return Mono.error(serverError());
        });

        // 一轮就让两家都熔断（窗口 1，1 次失败 = 100%）
        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f7-1"))
                .expectError().verify();
        int afterFirst = calls.get();

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f7-2"))
                .expectErrorSatisfies(e ->
                        assertEquals(UpstreamCircuitOpenException.class, e.getClass()))
                .verify();

        assertEquals(afterFirst, calls.get(), "两家都熔断了，一次上游调用都不该发生");
    }

    // --- 流式：闸门必须跨 provider 共享 ---

    /**
     * 首片之前转移：客户端的答案由备选给出，且<b>没有重复内容</b>。
     */
    @Test
    void streamFailsOverBeforeAnyChunkWasEmitted() {
        when(adapterA.chatStream(eq(MODEL), any())).thenReturn(Flux.error(serverError()));
        when(adapterB.chatStream(eq(MODEL), any())).thenReturn(Flux.just(
                StreamChunk.delta(MODEL, "备选"),
                StreamChunk.delta(MODEL, "的答案"),
                StreamChunk.finish(MODEL, "stop")));
        props.getResilience().getRetry().setEnabled(false);
        buildPipeline();

        StepVerifier.create(pipeline.executeStream(KEY, request(), "trace-f8")
                        .filter(StreamChunk::hasDelta)
                        .map(StreamChunk::delta))
                .expectNext("备选", "的答案")
                .verifyComplete();
    }

    /**
     * ★ F2 最关键的一条：<b>已经吐过分片之后绝不换 provider</b>。
     *
     * <p>这里断言的"B 一次都没被调用"正是"标记必须跨 provider 共享"这件事本身。
     * 如果 {@code attemptStreamChain} 在每次换 provider 时新建一个
     * {@code upstreamProduced}，A 那次尝试的标记就留在旧对象上，
     * 新的标记是 false → 闸门看起来是开的 → 换 B 重放一遍 →
     * 客户端收到「先 + 备选」两段拼接的答案，而服务端日志只有一条"转移成功"。
     *
     * <p>F1 已经把重试路径上的重复推送防住了；F2 如果各建各的标记，
     * 就会从<b>故障转移这条新路径</b>上把同一个 bug 放回来 ——
     * 这正是"一个不变量必须在新路径上重新被验证"的典型例子。
     */
    @Test
    void streamIsNotFailedOverAfterAChunkReachedTheClient() {
        // A 吐一个字，然后炸掉
        when(adapterA.chatStream(eq(MODEL), any())).thenReturn(Flux.concat(
                Mono.just(StreamChunk.delta(MODEL, "先")),
                Mono.error(serverError())));
        when(adapterB.chatStream(eq(MODEL), any())).thenReturn(Flux.just(
                StreamChunk.delta(MODEL, "备选"),
                StreamChunk.finish(MODEL, "stop")));
        props.getResilience().getRetry().setEnabled(false);
        buildPipeline();

        StepVerifier.create(pipeline.executeStream(KEY, request(), "trace-f9")
                        .filter(StreamChunk::hasDelta)
                        .map(StreamChunk::delta))
                .expectNext("先")
                .expectError()
                .verify();

        verify(adapterB, never()).chatStream(anyString(), any());
    }

    // --- 失败路径的配额口径（不变量 D + 不变量 E） ---

    /**
     * 转移全败：预扣<b>不退还</b>。
     *
     * <p>请求确实送到过上游（至少第一家接到了），prompt 部分大概率已经计费。
     * 退还等于把故障期的真实成本抹掉。
     */
    @Test
    void failoverExhaustedKeepsTheReservation() {
        when(adapterA.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));
        when(adapterB.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f10"))
                .expectError()
                .verify();

        assertEquals(List.of(reserved()), settledActuals(),
                "请求送到过上游，不该退还预扣");
    }

    /**
     * 一次调用都没发生（全部熔断）→ 预扣全额退还。
     *
     * <p>与上一条构成一对：两条一起才说明这条规则不是"一律退"也不是"一律不退"，
     * 而是严格按"有没有真的到达上游"分流。
     */
    @Test
    void nothingCallableRefundsTheReservation() {
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(1);
        cb.setMinimumNumberOfCalls(1);
        props.getResilience().getRetry().setEnabled(false);
        buildPipeline();

        when(adapterA.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));
        when(adapterB.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f11-1"))
                .expectError().verify();

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-f11-2"))
                .expectError().verify();

        List<Long> settled = settledActuals();
        assertEquals(2, settled.size(), "两次请求都必须结算 —— 失败路径也不能漏");
        assertEquals(0L, settled.get(1).longValue(),
                "第二次两家都熔断、一次调用都没发出去，必须全额退还");
    }

    /** 收集所有 settle 调用的 actual，按调用顺序返回。 */
    private List<Long> settledActuals() {
        ArgumentCaptor<Long> actual = ArgumentCaptor.forClass(Long.class);
        verify(quotaService, org.mockito.Mockito.atLeastOnce())
                .settle(anyString(), anyLong(), actual.capture());
        return actual.getAllValues();
    }
}
