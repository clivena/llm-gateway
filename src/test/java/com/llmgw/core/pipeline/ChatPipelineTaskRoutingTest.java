package com.llmgw.core.pipeline;

import com.llmgw.api.OpenAiProtocol;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 任务路由在编排层的落地。
 *
 * <p>RouterServiceTest 管"选得对不对"，这一组只管一件事：
 * <b>选出来的模型有没有被真正用在该用的地方</b>。两者必须分开测 ——
 * 路由算得再准，只要编排层还拿着旧的 {@code request.resolvedModel()} 去查缓存、
 * 或者某个分支偷偷用了 policy 的模型，结果都是错的，而所有路由单测依然全绿。
 *
 * <p>四条断言各守一个接触面：
 * <ol>
 *   <li>{@link #nonStreamUsesModelFromTaskRoute} / {@link #streamUsesModelFromTaskRoute}
 *       —— 两条执行路径都用 route.tier()，不是只有非流式改了。</li>
 *   <li>{@link #cacheNamespaceFollowsRoutedModel} —— <b>最容易被漏掉的一条</b>：
 *       缓存段必须跟着"路由后的模型"划，否则不同任务的答案会互相命中，
 *       任务路由被缓存整个架空（和当初不变量 B 那个 bug 同一形状）。</li>
 *   <li>{@link #unknownTaskSurfacesAsBadRequest} —— 显式任务名写错必须是 400，
 *       不能变成 502（IllegalStateException 的映射）也不能静默兜底。</li>
 *   <li>{@link #routeDecisionIsCounted} —— 指标真的写了，而不只是"代码里调了个方法"。</li>
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineTaskRoutingTest {

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private ProviderAdapter adapter;
    @Mock private UpstreamResilience resilience;

    private final GatewayProperties props = new GatewayProperties();
    private final TokenEstimator estimator = new TokenEstimator(props);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final GatewayMetrics metrics = new GatewayMetrics(meterRegistry);

    private ChatPipeline pipeline;

    private static final String KEY = "tenant-a";
    private static final String CHEAP = "deepseek-flash";
    private static final String STRONG = "deepseek-v4-pro";

    @BeforeEach
    void setUp() {
        props.getCache().setEnabled(true);

        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, metrics, props, estimator, resilience);

        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(cacheService.write(anyString(), anyString(), anyString())).thenReturn(Mono.empty());

        when(adapter.name()).thenReturn("deepseek");
        when(adapter.chat(anyString(), any())).thenReturn(Mono.just(answer(CHEAP, "答案")));
        when(adapter.chatStream(anyString(), any()))
                .thenReturn(Flux.just(StreamChunk.delta(CHEAP, "答"), StreamChunk.finish(CHEAP, "stop")));
        when(providers.candidatesFor(anyString())).thenReturn(List.of(adapter));

        // 重试与熔断直通：本组测的是"用了哪个模型"，不是调用了几次。
        when(resilience.callable(anyString())).thenReturn(true);
        when(resilience.attemptsPerCall()).thenReturn(1);
        when(resilience.protect(anyString(), any(), anyInt())).thenAnswer(inv -> inv.getArgument(1));
        when(resilience.protectStream(anyString(), any(), any(), anyInt()))
                .thenAnswer(inv -> inv.getArgument(1));

        when(routerService.shouldEscalate(any(ModelTier.class), any(), anyBoolean())).thenReturn(false);
    }

    @Test
    void nonStreamUsesModelFromTaskRoute() {
        when(routerService.resolve(any())).thenReturn(route(STRONG, "code", "explicit"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "t1"))
                .expectNextCount(1)
                .verifyComplete();

        verify(providers).candidatesFor(STRONG);
        verify(adapter).chat(eq(STRONG), any());
    }

    /** 流式那条路径同样走 route.tier()。分别断言是必要的：将来有人把路由内联进某一条路径时，另一条会静默分叉。 */
    @Test
    void streamUsesModelFromTaskRoute() {
        when(routerService.resolve(any())).thenReturn(route(STRONG, "code", "inferred"));

        StepVerifier.create(pipeline.executeStream(KEY, request(true), "t2"))
                .expectNextCount(2)
                .verifyComplete();

        verify(providers).candidatesFor(STRONG);
    }

    /**
     * 缓存段必须跟着路由后的模型划。
     *
     * <p>如果这里退化成 {@code tenant-a:auto}，同一个问题在 code 任务与 chat 任务下
     * 会命中同一份答案 —— 任务路由配了也等于没配，而且不会有任何报错。
     */
    @Test
    void cacheNamespaceFollowsRoutedModel() {
        when(routerService.resolve(any())).thenReturn(route(CHEAP, "chat", "fallback"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "t3"))
                .expectNextCount(1)
                .verifyComplete();

        verify(cacheService).lookup(eq(KEY + ":" + CHEAP), anyString());
    }

    @Test
    void unknownTaskSurfacesAsBadRequest() {
        when(routerService.resolve(any()))
                .thenThrow(new IllegalArgumentException("未知任务 [cdoe]。当前可用：[chat]"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "t4"))
                .expectErrorSatisfies(e -> {
                    assertTrue(e instanceof IllegalArgumentException, "实际类型：" + e.getClass());
                    assertEquals(HttpStatus.BAD_REQUEST, OpenAiProtocol.statusOf(e),
                            "必须是 400；502 会让调用方以为是上游故障，排查方向全错");
                })
                .verify();
    }

    @Test
    void routeDecisionIsCounted() {
        when(routerService.resolve(any())).thenReturn(route(CHEAP, "code", "inferred"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "t5"))
                .expectNextCount(1)
                .verifyComplete();

        double count = meterRegistry.get("llmgw.route.task")
                .tag("task", "code")
                .tag("source", "inferred")
                .counter()
                .count();
        assertEquals(1.0, count);
    }

    // --- helpers ---

    private static Route route(String model, String task, String source) {
        return new Route(new ModelTier(model, "CHEAP", 0.0, 0.5), task, source, "test");
    }

    private static ChatRequest request(boolean stream) {
        return new ChatRequest("auto", stream, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", "什么是语义缓存")), "test", null);
    }

    private static ChatResponse answer(String model, String content) {
        return new ChatResponse("id", model, content, new Usage(20, 30), false, "stop");
    }
}
