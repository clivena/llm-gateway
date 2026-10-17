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
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.Route;
import com.llmgw.core.router.RouteTrace;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * 路由回执（{@link RouteTrace}）的写入契约。
 *
 * <h3>为什么要单独一个测试类</h3>
 * 这个对象只有"写一次、读一次"两个动作，看起来不值得测。但它守着一条
 * <b>失败时完全没有症状</b>的链路：
 * <pre>
 *   RouterService 判定  →  RouteTrace  →  ChatController  →  响应头
 * </pre>
 * 中间任何一环断了，请求依然成功、答案依然正确、状态码依然是 200 ——
 * 只是响应头里少一个字段。没有断言的话，这种断法可以一直活到有人
 * 拿它去排查线上问题、发现排不出来为止。
 *
 * <p>所以这里钉住三件事：<b>两条路径都要写</b>（流式曾经漏过一次指标，
 * 同类错误）、<b>失败时要留空</b>（不能留上一次请求的残值）、
 * <b>三参数重载不能变成空实现</b>。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineRouteTraceTest {

    private static final String KEY = "tenant-a";
    private static final String MODEL = "deepseek-flash";
    private static final String PROVIDER = "provider-a";

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private ProviderAdapter adapter;

    private final GatewayProperties props = new GatewayProperties();
    private final TokenEstimator estimator = new TokenEstimator(props);
    private ChatPipeline pipeline;

    @BeforeEach
    void setUp() {
        props.getCache().setEnabled(false);
        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(providers.candidatesFor(MODEL)).thenReturn(List.of(adapter));
        when(adapter.name()).thenReturn(PROVIDER);
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(answer()));
        when(adapter.chatStream(eq(MODEL), any())).thenReturn(Flux.just(
                StreamChunk.delta(MODEL, "你好"),
                StreamChunk.usage(MODEL, new Usage(12, 3)),
                StreamChunk.finish(MODEL, "stop")));
        when(routerService.shouldEscalate(any(), any(), anyBoolean())).thenReturn(false);

        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, new GatewayMetrics(new SimpleMeterRegistry()), props, estimator,
                new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry())));
    }

    @Test
    void nonStreamRecordsTheRoute() {
        RouteTrace trace = new RouteTrace();
        when(routerService.resolve(any())).thenReturn(
                new Route(new ModelTier(MODEL, "CHEAP", 0.0015, 0.72), "code", "inferred", "code-marker"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "trace-1", trace))
                .expectNextCount(1)
                .verifyComplete();

        assertEquals("code", trace.get().task());
        assertEquals("inferred", trace.get().source());
        assertEquals(MODEL, trace.get().tier().model());
    }

    /** 流式单独断言：两条路径共用 resolveRoute，但"共用"是要靠测试证明的，不是靠注释。 */
    @Test
    void streamRecordsTheRoute() {
        RouteTrace trace = new RouteTrace();
        when(routerService.resolve(any())).thenReturn(
                new Route(new ModelTier(MODEL, "CHEAP", 0.0015, 0.72), "summarize", "fallback", "long-input"));

        pipeline.executeStream(KEY, request(true), "trace-2", trace).collectList().block();

        assertEquals("summarize", trace.get().task());
        assertEquals("fallback", trace.get().source());
    }

    /**
     * 路由判定失败时不写回执 —— 调用方据此<b>不设</b>响应头。
     *
     * <p>注意这里断言的是 null 而不是某个兜底值。若实现改成"出错时写一个 unknown"，
     * 响应头上就会出现一个看起来像"判定成了 unknown 任务"的值，
     * 而真实情况是这次请求根本没走到路由 —— 诊断信息在撒谎比缺失更坏。
     */
    @Test
    void unknownTaskLeavesTraceEmptyForNonStream() {
        RouteTrace trace = new RouteTrace();
        when(routerService.resolve(any())).thenThrow(new IllegalArgumentException("未知任务 [nope]"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "trace-3", trace))
                .expectError(IllegalArgumentException.class)
                .verify();

        assertNull(trace.get());
    }

    @Test
    void unknownTaskLeavesTraceEmptyForStream() {
        RouteTrace trace = new RouteTrace();
        when(routerService.resolve(any())).thenThrow(new IllegalArgumentException("未知任务 [nope]"));

        StepVerifier.create(pipeline.executeStream(KEY, request(true), "trace-4", trace))
                .expectError(IllegalArgumentException.class)
                .verify();

        assertNull(trace.get());
    }

    /**
     * 三参数重载必须真的跑完管线，而不是"为了兼容留个空壳"。
     *
     * <p>它被大量单元测试与内部调用点使用；如果哪天有人图省事把它改成
     * {@code return Mono.empty()}，所有调用点的测试会一起"通过"—— 因为没人断言内容。
     */
    @Test
    void threeArgOverloadStillRunsThePipeline() {
        when(routerService.resolve(any())).thenReturn(
                new Route(new ModelTier(MODEL, "CHEAP", 0.0015, 0.72), "chat", "policy", "test"));

        StepVerifier.create(pipeline.execute(KEY, request(false), "trace-5"))
                .expectNextMatches(resp -> "好的".equals(resp.content()))
                .verifyComplete();
    }

    private static ChatRequest request(boolean stream) {
        return new ChatRequest(MODEL, stream, 0.0, 256, null, null, null, null,
                List.of(new ChatMessage("user", "帮我总结这段代码")), "test", null);
    }

    private static ChatResponse answer() {
        return new ChatResponse("id-1", MODEL, "好的", new Usage(12, 3), false, "stop");
    }
}
