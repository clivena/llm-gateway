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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 被重试丢弃的尝试也必须记账、也必须计配额（不变量 C / E）。
 *
 * <h3>这是一个"账本看起来完全正常"的缺陷</h3>
 * 一次请求重试 2 次、最终成功，账本会安安静静写出 1 行、金额正确、traceId 对得上。
 * 唯一的问题是这次请求真实发生了 2 次计费调用，账本少了一半。
 * 它不会报错，也不会让任何监控曲线变形 —— 只会在月底对账时表现为
 * "网关统计比账单少一点"，而那时已经没有任何线索能指到重试路径。
 *
 * <h3>为什么断言"下游收到的 actual"而不只断言账本行数</h3>
 * 账本与配额是两个口径，任何一个漏了都会导致对不上账：
 * 账本少记 → 成本归因偏低；配额少扣 → 上限被突破。
 * 所以这里同时钉住两件事：写了几行、结算了多少 token。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineAttemptLedgerTest {

    private static final String KEY = "tenant-a";
    private static final String MODEL = "deepseek-flash";
    private static final String PROVIDER = "provider-a";

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private ProviderAdapter adapter;
    @Mock private UpstreamResilience resilience;

    private final GatewayProperties props = new GatewayProperties();
    private final TokenEstimator estimator = new TokenEstimator(props);
    private ChatPipeline pipeline;

    @BeforeEach
    void setUp() {
        props.getCache().setEnabled(false);
        // 重试 2 次（首次 + 1 次重试）就够演示：第一次空正文被丢弃，第二次成功。
        var r = props.getResilience().getRetry();
        r.setMaxAttempts(2);
        r.setWaitDurationMillis(1);
        r.setBackoffMultiplier(1.0);
        r.setMaxWaitDurationMillis(1);

        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        // 缓存关掉了也要桩：编排层照样会调 lookup，"关"是装配层的事，
        // 这里 mock 返回 null 会让整条链 NPE（而且报错指向 flatMap，看不出根因）。
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(routerService.resolve(any())).thenReturn(
                new Route(new ModelTier(MODEL, "CHEAP", 0.0015, 0.72), "chat", "fallback", "test"));
        when(routerService.shouldEscalate(any(), any(), anyBoolean())).thenReturn(false);
        when(providers.candidatesFor(MODEL)).thenReturn(List.of(adapter));
        when(adapter.name()).thenReturn(PROVIDER);

        // 真 resilience4j：本用例要验的正是"重试真的发生了"
        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, new GatewayMetrics(new SimpleMeterRegistry()), props, estimator,
                new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry())));
    }

    private static ChatRequest request() {
        return new ChatRequest(MODEL, false, 0.0, 256, null, null, null, null,
                List.of(new ChatMessage("user", "把这段代码重构成更清晰的写法")), "test", null);
    }

    /** 上游 200 但正文为空（推理型模型把预算花在 reasoning 上）—— F0 的最常见失败形态。 */
    private static ChatResponse emptyAnswer() {
        return new ChatResponse("id-empty", MODEL, "", new Usage(34, 800), false, "length");
    }

    private static ChatResponse goodAnswer() {
        return new ChatResponse("id-ok", MODEL, "重构后的版本如下", new Usage(34, 20), false, "stop");
    }

    @Test
    void discardedAttemptIsBilledAndSettledToo() {
        AtomicInteger calls = new AtomicInteger();
        when(adapter.chat(eq(MODEL), any())).thenAnswer(inv ->
                Mono.just(calls.incrementAndGet() == 1 ? emptyAnswer() : goodAnswer()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-1"))
                .expectNextMatches(resp -> "重构后的版本如下".equals(resp.content()))
                .verifyComplete();

        assertEquals(2, calls.get(), "首次空正文 + 重试一次");

        // ① 账本：两次真实上游调用 → 两行
        // 顺带钉住账本的 task 列：路由判定成 chat，两行都必须带这个值 ——
        // 只记 model 的话，多个任务映射到同一模型时就再也分不出成本归因。
        verify(ledger, timeout(2000).times(2)).record(
                eq("trace-1"), eq(KEY), any(), eq("chat"), eq(MODEL), eq(false),
                anyInt(), anyInt(), anyDouble(), anyLong());

        // ② 配额：结算的是两次用量之和，不是最终那一次
        ArgumentCaptor<Long> actual = ArgumentCaptor.forClass(Long.class);
        verify(quotaService).settle(eq(KEY), anyLong(), actual.capture());
        assertEquals((34 + 800) + (34 + 20), actual.getValue().longValue(),
                "被丢弃的那次也真的花了钱：834 + 54 = 888");
    }

    /**
     * 反向用例：没有重试时只能有一行账。
     *
     * <p>这条看着平淡，但它守的是最容易在实现"补账"时写错的边界 ——
     * 如果判据写成了"每次订阅都立即落账"，正常请求也会写两行
     * （一次按尝试、一次按最终），账本会整体翻倍，而每一条看起来都很合理。
     */
    @Test
    void successfulFirstAttemptIsBilledExactlyOnce() {
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(goodAnswer()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-2"))
                .expectNextCount(1)
                .verifyComplete();

        verify(ledger, timeout(2000).times(1)).record(
                eq("trace-2"), eq(KEY), any(), eq("chat"), eq(MODEL), eq(false),
                eq(34), eq(20), anyDouble(), anyLong());

        ArgumentCaptor<Long> actual = ArgumentCaptor.forClass(Long.class);
        verify(quotaService).settle(eq(KEY), anyLong(), actual.capture());
        assertEquals(54L, actual.getValue().longValue());
    }

    /**
     * 空正文重试到底、最终把原始响应交还给客户端（F0 的透传）时，
     * <b>不能多记一行</b>：那条响应就是最终答案，由正常路径落账。
     *
     * <p>这正是"哪次算被丢弃"这个判据最容易出错的地方 —— 最后一次尝试的空响应
     * 既是"失败的尝试"，又是"最终交付的响应"。记两次就是凭空多出一笔费用。
     */
    @Test
    void exhaustedEmptyAnswerIsBilledOnceNotTwice() {
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(emptyAnswer()));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-3"))
                .expectNextMatches(resp -> resp.content().isEmpty()
                        && "length".equals(resp.finishReason()))
                .verifyComplete();

        // 2 次尝试：前 1 次被重试丢弃 → 1 行；最后 1 次作为最终响应 → 1 行
        verify(ledger, timeout(2000).times(2)).record(
                eq("trace-3"), eq(KEY), any(), eq("chat"), eq(MODEL), eq(false),
                eq(34), eq(800), anyDouble(), anyLong());
    }
}
