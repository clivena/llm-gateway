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
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.resilience.UpstreamCircuitOpenException;
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * F1：重试 / 熔断接进管线之后，与 F0 的两条既有语义是否还成立。
 *
 * <h3>本类存在的理由：F1 最容易破坏的恰好是 F0 刚建立的东西</h3>
 * 重试要给"空正文"套一层异常才能被算子识别，而那层异常**必须在重试耗尽后被还原**。
 * 少还原一次，客户端就从"200 + finish_reason=length + 空正文"退化成 502 ——
 * 那正是 F0 修掉的"把被截断伪装成正常结束"的镜像错误：
 * 把"上游明确说明了原因"伪装成"网关坏了"。
 * 这种回归在真机脚本里只表现为一条断言变红，在代码里则完全看不出来。
 *
 * <p>这里用**真实的** UpstreamResilience（它的组合语义就是被测对象），
 * 只把 adapter / 配额 / 缓存这些边界 mock 掉。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineResilienceTest {

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private ProviderAdapter cheapAdapter;
    @Mock private ProviderAdapter strongAdapter;

    private final GatewayProperties props = new GatewayProperties();
    private final TokenEstimator estimator = new TokenEstimator(props);
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());

    private ChatPipeline pipeline;

    private static final String KEY = "tenant-a";
    private static final String MODEL = "deepseek-flash";
    private static final String STRONG = "deepseek-v4-pro";

    @BeforeEach
    void setUp() {
        props.getCache().setEnabled(true);
        var r = props.getResilience().getRetry();
        r.setMaxAttempts(3);
        r.setWaitDurationMillis(1);
        r.setBackoffMultiplier(1.0);
        r.setMaxWaitDurationMillis(1);
        // 熔断窗口默认放大，避免在"测重试"的用例里被顺手打开
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(100);
        cb.setMinimumNumberOfCalls(100);

        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(cacheService.write(anyString(), anyString(), anyString())).thenReturn(Mono.empty());
        when(providers.forModel(MODEL)).thenReturn(cheapAdapter);
        when(providers.forModel(STRONG)).thenReturn(strongAdapter);
        when(cheapAdapter.name()).thenReturn("deepseek");
        when(strongAdapter.name()).thenReturn("deepseek");
        when(routerService.shouldEscalate(any(ModelTier.class), any(), anyBoolean())).thenReturn(false);

        buildPipeline();
    }

    private void buildPipeline() {
        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, metrics, props, estimator,
                new UpstreamResilience(props, metrics));
    }

    private ChatRequest request() {
        return new ChatRequest(MODEL, false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", "什么是语义缓存")), "test");
    }

    private long reserved() {
        return estimator.estimatePrompt(request().messages());
    }

    /** 上游 200 但正文是空的 —— 推理型模型把 max_tokens 预算花在 reasoning 上（F0）。 */
    private static ChatResponse emptyAnswer() {
        return new ChatResponse("id", MODEL, "", new Usage(33, 64), false, "length");
    }

    private static WebClientResponseException serverError() {
        return WebClientResponseException.create(500, "boom", HttpHeaders.EMPTY, new byte[0], null);
    }

    // --- 空正文：重试，但耗尽后必须还原成原始响应 ---

    /**
     * 空正文会触发重试；重试全部失败后，客户端拿到的仍然是那条**原始响应**，
     * 而不是 502。
     *
     * <p>三个断言各自守一件事：
     * <ol>
     *   <li>调用次数 = maxAttempts —— 证明重试真的发生了（不是"看起来像发生了"）；</li>
     *   <li>content 为空、finish_reason = {@code length} —— 证明没有在重试过程中
     *       被换成别的响应或空壳；</li>
     *   <li>没有异常 —— 证明 F0 的"透传"没有被 F1 的异常翻译破坏。</li>
     * </ol>
     */
    @Test
    void emptyAnswerIsRetriedButFinallyRestoredAsTheOriginalResponse() {
        AtomicInteger calls = new AtomicInteger();
        when(cheapAdapter.chat(eq(MODEL), any())).thenAnswer(inv -> {
            calls.incrementAndGet();
            return Mono.just(emptyAnswer());
        });

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-1"))
                .expectNextMatches(r -> "".equals(r.content()) && "length".equals(r.finishReason()))
                .verifyComplete();

        assertEquals(3, calls.get(), "空正文值得重试，应该试满 maxAttempts");
    }

    /**
     * 反面：<b>有正文</b>但被截断 = 客户端的 max_tokens 设小了，不是上游故障。
     *
     * <p>这条不重试是刻意的：同样的请求再问一遍，预算还是那么小，结果还是被截断 ——
     * 白花两次钱。它与上一条的区别只在"正文是否为空"，
     * 而那正是 F0 的四分表里最容易写反的一格。
     */
    @Test
    void truncatedButNonEmptyAnswerIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        when(cheapAdapter.chat(eq(MODEL), any())).thenAnswer(inv -> {
            calls.incrementAndGet();
            return Mono.just(new ChatResponse("id", MODEL, "被截断的半句",
                    new Usage(33, 64), false, "length"));
        });

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-2"))
                .expectNextMatches(r -> "被截断的半句".equals(r.content()))
                .verifyComplete();

        assertEquals(1, calls.get(), "max_tokens 偏小是调用方的问题，重试只会白花钱");
    }

    @Test
    void transientServerErrorIsRetriedAndCanSucceedOnASecondAttempt() {
        AtomicInteger calls = new AtomicInteger();
        when(cheapAdapter.chat(eq(MODEL), any())).thenAnswer(inv -> {
            if (calls.incrementAndGet() == 1) {
                return Mono.error(serverError());
            }
            return Mono.just(new ChatResponse("id", MODEL, "第二次成功了",
                    new Usage(20, 30), false, "stop"));
        });

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-3"))
                .expectNextMatches(r -> "第二次成功了".equals(r.content()))
                .verifyComplete();

        assertEquals(2, calls.get());
        // 成功的这次按真实用量结算，与"有没有重试过"无关
        assertEquals(List.of(50L), verifySettle());
    }

    // --- 失败路径的配额口径（不变量 D） ---

    /**
     * 熔断打开 → 请求压根没发出去 → 预扣<b>全额退还</b>。
     *
     * <p>这一条是 F1 顺带修掉的一个真实漏洞：在此之前，任何失败的请求
     * （上游 502、连不上、熔断）都不会调用结算，那笔预扣就永远留在账上。
     * 平时流量小看不出来，但熔断正是"连续失败"打开的 ——
     * 上游整体挂掉的那段时间里，每个被拒的请求都白吃一整笔配额。
     *
     * <p>同时断言了反面的另一半：上游 5xx（请求<b>已经送到</b>、prompt 大概率
     * 已经计费）<b>不退</b>预扣。两个方向都钉住，才知道这条规则不是"一律退款"。
     */
    @Test
    void circuitOpenRefundsTheReservationWhileServerErrorsKeepIt() {
        // 熔断器调小：2 次失败（100%）即打开；关掉重试以便一次请求只记 1 笔失败
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(2);
        cb.setMinimumNumberOfCalls(2);
        props.getResilience().getRetry().setEnabled(false);
        buildPipeline();

        when(cheapAdapter.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));

        // 前两次：上游 5xx → 请求已送出 → 保留预扣
        for (int i = 0; i < 2; i++) {
            StepVerifier.create(pipeline.execute(KEY, request(), "trace-4"))
                    .expectError()
                    .verify();
        }

        // 第三次：熔断已打开 → 请求没有发出去 → 全额退还
        StepVerifier.create(pipeline.execute(KEY, request(), "trace-5"))
                .expectErrorSatisfies(e ->
                        assertEquals(UpstreamCircuitOpenException.class, e.getClass()))
                .verify();

        List<Long> settled = verifySettle();
        assertEquals(3, settled.size(), "三次请求都必须结算 —— 失败路径也不能漏");
        assertEquals(reserved(), settled.get(0).longValue(), "5xx 保留预扣：请求送到过上游");
        assertEquals(reserved(), settled.get(1).longValue());
        assertEquals(0L, settled.get(2).longValue(), "熔断：一分钱没花，必须退还");
    }

    @Test
    void retryExhaustedServerErrorStillKeepsTheReservation() {
        when(cheapAdapter.chat(eq(MODEL), any())).thenReturn(Mono.error(serverError()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-6"))
                .expectError()
                .verify();

        List<Long> settled = verifySettle();
        assertEquals(List.of(reserved()), settled,
                "重试全败也不退还 —— 请求确实送到了上游，prompt 部分大概率已经计费");
    }

    // --- 级联升级 + 失败的组合 ---

    /**
     * 级联升级时第二次调用失败：结算必须用<b>第一次调用的真实用量</b>。
     *
     * <p>这是"不变量 C（两次调用两份费用）"与"不变量 D（失败路径也要结算）"
     * 交叉出来的那个格子。用 reserved 代替真实用量会错一次，
     * 用 0 会把第一次已经花掉的钱整个抹掉。
     */
    @Test
    void escalationFollowedByFailureSettlesWithTheFirstCallsRealUsage() {
        when(cheapAdapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "", new Usage(11, 22), false, "length")));
        when(routerService.shouldEscalate(any(ModelTier.class), any(), anyBoolean())).thenReturn(true);
        when(routerService.escalateFrom(any(ModelTier.class)))
                .thenReturn(new ModelTier(STRONG, "STRONG", 0.0, 1.0));
        when(strongAdapter.chat(eq(STRONG), any())).thenReturn(Mono.error(serverError()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-7"))
                .expectError()
                .verify();

        List<Long> settled = verifySettle();
        assertEquals(List.of(33L), settled,
                "第一次调用真实花了 11+22=33，必须照扣；用 reserved 或 0 都不对");
    }

    /** 收集所有 settle 调用的 actual，按调用顺序返回。 */
    private List<Long> verifySettle() {
        ArgumentCaptor<Long> actual = ArgumentCaptor.forClass(Long.class);
        org.mockito.Mockito.verify(quotaService, org.mockito.Mockito.atLeastOnce())
                .settle(anyString(), anyLong(), actual.capture());
        return actual.getAllValues();
    }
}
