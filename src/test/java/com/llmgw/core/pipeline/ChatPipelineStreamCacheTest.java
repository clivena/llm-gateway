package com.llmgw.core.pipeline;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.CacheEntry;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.cost.CostLedgerService;
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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 流式路径的缓存命中：一个本可以被"看起来对"掩盖的差异。
 *
 * <h3>问题</h3>
 * 走上游的流在末帧一定带 {@code finish_reason}（F0 修的透传）。而缓存命中
 * 曾经只发一个 delta 分片就结束 —— 客户端拿到的是一条<b>没有收尾帧</b>的流。
 *
 * <p>后果不是"少一个字段"那么轻：所有靠累积 {@code finish_reason} 做判断的实现
 * （答案是否被截断、结构化输出是否完整）在命中时只能得到 {@code null}，
 * 与"走上游但被 length 截断"无法区分。而缓存命中恰恰是最该被信任的那条路径 ——
 * 交出去的是一份已经完整、被验证过的答案。
 *
 * <h3>为什么这条必须用单测守</h3>
 * 真机脚本只断言"命中很快、没计费"，这两种证据对"缺收尾帧"完全无感：
 * 正文是对的、状态码是 200、账本也正常。缺帧只有在客户端视角才看得见。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineStreamCacheTest {

    private static final String KEY = "tenant-a";
    private static final String MODEL = "deepseek-flash";
    private static final String ANSWER = "缓存里的答案";

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private UpstreamResilience resilience;

    private ChatPipeline pipeline;

    @BeforeEach
    void setUp() {
        var props = new GatewayProperties();
        props.getCache().setEnabled(true);

        when(routerService.resolve(any())).thenReturn(
                new Route(new ModelTier(MODEL, "CHEAP", 0.0, 0.5), "chat", "fallback", "test"));
        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        when(cacheService.lookup(anyString(), anyString()))
                .thenReturn(Mono.just(new CacheEntry(ANSWER, 1.0)));

        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, new GatewayMetrics(new SimpleMeterRegistry()), props,
                new TokenEstimator(props), resilience);
    }

    private ChatRequest request() {
        return new ChatRequest(MODEL, true, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", "什么是语义缓存")), "test", null);
    }

    /**
     * 命中 → delta + finish 两个分片，且 finish 的 reason 是 stop。
     *
     * <p>断言分片<b>个数</b>而不只是最后一个：只发 finish 会把答案弄丢，
     * 发三个又会重复。缓存命中路径的分片序列就该是"内容 + 收尾"这一种形状。
     */
    @Test
    void cacheHitOnStreamEmitsAFinishChunk() {
        StepVerifier.create(pipeline.executeStream(KEY, request(), "trace-1"))
                .expectNextMatches(c -> "cache".equals(c.model()) && ANSWER.equals(c.delta()))
                .expectNextMatches(c -> "cache".equals(c.model())
                        && "stop".equals(c.finishReason())
                        && (c.delta() == null || c.delta().isEmpty()))
                .verifyComplete();
    }

    /**
     * 命中必须全额退还预扣 —— 与非流式路径同一个口径。
     * 这条和上一条一起看才有意义：命中既不该花钱，也不该吃配额。
     */
    @Test
    void cacheHitOnStreamRefundsTheReservation() {
        StepVerifier.create(pipeline.executeStream(KEY, request(), "trace-1"))
                .expectNextCount(2)
                .verifyComplete();

        // settle(apiKey, reserved, actual=0)：actual 为 0 即全额退还
        org.mockito.Mockito.verify(quotaService).settle(
                org.mockito.ArgumentMatchers.eq(KEY),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.eq(0L));
    }

    /** 分片模型名是 cache，不是请求里写的模型 —— 客户端据此能看出这次没走上游。 */
    @Test
    void cacheHitChunksCarryTheCacheModelName() {
        // 不用 .block()：全仓"零阻塞"这条约束是可以用 grep 验证的，
        // 让测试目录里出现一个 block 会把这句可验证的承诺变成"除了测试以外"。
        List<StreamChunk> chunks = new java.util.ArrayList<>();
        StepVerifier.create(pipeline.executeStream(KEY, request(), "trace-1"))
                .recordWith(() -> chunks)
                .expectNextCount(2)
                .verifyComplete();

        assertEquals(2, chunks.size());
        assertEquals("cache", chunks.get(0).model());
        assertEquals("cache", chunks.get(1).model());
    }
}
