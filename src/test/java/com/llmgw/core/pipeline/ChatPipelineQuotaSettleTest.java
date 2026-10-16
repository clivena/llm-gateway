package com.llmgw.core.pipeline;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.CacheEntry;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.cost.CostLedgerService;
import com.llmgw.core.protocol.ProviderAdapter;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.protocol.UpstreamOutcome;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.token.TokenEstimator;
import com.llmgw.observability.GatewayMetrics;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * F0.2：配额 reserve-then-settle。
 *
 * <h3>为什么要用 mock 单测，而不是只靠真机脚本</h3>
 * 真机脚本能证明"配额数字对上了"，但证明不了**它在什么情况下退多少**：
 * 缓存命中要全额退、结算失败不能把成功请求变成失败、实际用量不能用估算 ——
 * 这些都是分支逻辑。分支必须逐条钉死，否则真机跑一次"看起来对"就过去了。
 *
 * <h3>测的是"给 settle 传了什么"，不是"settle 自己怎么算"</h3>
 * 后者由 Redis 脚本负责，属集成层面的验证（真机脚本 + quota_settle.lua 的语义）。
 * 这里守的是管线这一侧的口径：**传下去的 actual 到底是不是真实用量**。
 */
@ExtendWith(MockitoExtension.class)
// 这里刻意放宽桩校验：setUp 里那几行是**所有**用例共用的脚手架
// （配额放行、缓存写入、provider 解析），单个用例未必每条都用到。
// 严格模式下每次都要在用例里重复它们一遍，噪音大于收益。
// 真正需要严格的地方是"断言用了什么参数"—— 那部分由 verify 保证。
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineQuotaSettleTest {

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private GatewayMetrics metrics;
    @Mock private ProviderAdapter adapter;

    private final GatewayProperties props = new GatewayProperties();
    private final TokenEstimator estimator = new TokenEstimator(props);

    private ChatPipeline pipeline;

    private static final String KEY = "tenant-a";
    private static final String MODEL = "deepseek-flash";

    @BeforeEach
    void setUp() {
        props.getCache().setEnabled(true);
        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, metrics, props, estimator);

        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(true));
        when(quotaService.settle(anyString(), anyLong(), anyLong())).thenReturn(Mono.just(0L));
        when(cacheService.write(anyString(), anyString(), anyString())).thenReturn(Mono.empty());
        when(providers.forModel(MODEL)).thenReturn(adapter);
        // 显式指定模型 → 不让路由自作主张升级（否则会静默产生贵模型账单）
        when(routerService.shouldEscalate(any(ModelTier.class), any(), anyBoolean())).thenReturn(false);
    }

    private ChatRequest request() {
        return new ChatRequest(MODEL, false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", "什么是语义缓存")), "test");
    }

    private long reserved() {
        return estimator.estimatePrompt(request().messages());
    }

    // --- 命中：必须全额退还 ---

    /**
     * 缓存命中 = 完全没走上游 = 真实用量为 0。
     *
     * 这一条在 F0 之前是漏的：命中省了钱，却照样按 prompt 估算吃一次配额。
     * "命中就省钱"是缓存存在的全部理由，配额侧却还在扣，等于把这个理由削掉一半。
     */
    @Test
    void cacheHitRefundsTheEntireReservation() {
        when(cacheService.lookup(anyString(), anyString()))
                .thenReturn(Mono.just(new CacheEntry("缓存的答案", 1.0)));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-1"))
                .expectNextMatches(ChatResponse::cacheHit)
                .verifyComplete();

        verify(quotaService).settle(eq(KEY), eq(reserved()), eq(0L));
    }

    // --- 未命中：按真实用量结算，不是按估算 ---

    /**
     * 结算用的必须是上游回传的真实 total（prompt + completion），
     * 而不是 prompt 估算值。
     *
     * 这条断言里的 2205 就是 F0 要修的那个缺口的量化形式：这一批真机实测里
     * 一个"什么是语义缓存"的请求 prompt=33、**completion=2172**。
     * 如果管线只把 prompt 侧的 33 报上去，配额就永远只覆盖实际 token 量的百分之几。
     */
    @Test
    void upstreamRealUsageIsSettledIncludingCompletion() {
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "语义缓存是……", new Usage(33, 2172), false, "stop")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-2"))
                .expectNextCount(1)
                .verifyComplete();

        verify(quotaService).settle(eq(KEY), eq(reserved()), eq(2205L));
    }

    /**
     * 上游没给可信 usage 时才回退估算 —— 且回退值必须包含 completion 侧。
     *
     * <p>这一条守的是一个**很容易写反**的分支：结算按 {@code actual - reserved} 补扣，
     * 如果把"上游没给 usage"当成 actual=0，就会把整笔预扣**退回去**，
     * 而这次请求明明花了钱。所以期望值是"预扣 + 答案的估算"，不是 0。
     */
    @Test
    void fallsBackToEstimateWhenUpstreamUsageIsMissing() {
        String answer = "一个不算短的答案";
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, answer, new Usage(0, 0), false, "stop")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-3"))
                .expectNextCount(1)
                .verifyComplete();

        long expected = estimator.estimateUsage(request().messages(), answer).total();
        ArgumentCaptor<Long> actual = ArgumentCaptor.forClass(Long.class);
        verify(quotaService).settle(eq(KEY), eq(reserved()), actual.capture());
        assertEquals(expected, actual.getValue(),
                "usage 缺失必须回退估算，不能当成 0（那会退掉整笔预扣）");
    }

    // --- 结算失败不能污染请求结果 ---

    @Test
    void settleFailureDoesNotFailTheRequest() {
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "答案", new Usage(10, 20), false, "stop")));
        when(quotaService.settle(anyString(), anyLong(), anyLong()))
                .thenReturn(Mono.error(new RuntimeException("redis down")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-4"))
                .expectNextMatches(r -> "答案".equals(r.content()))
                .verifyComplete();

        // 失败要被计数，否则"配额系统性失真"这件事只在日志里、告警看不到
        verify(metrics).onQuotaSettleFailed();
    }

    // --- 上游可用性判定接入管线 ---

    /**
     * 推理型模型的失败形态必须被识别出来并计数 ——
     * 这是 F1/F2 能不能在真实失败上触发的前提。
     */
    @Test
    void emptyTruncatedResponseIsCountedAsUpstreamFailure() {
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "", new Usage(33, 64), false, "length")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-5"))
                .expectNextCount(1)
                .verifyComplete();

        ArgumentCaptor<UpstreamOutcome> captor = ArgumentCaptor.forClass(UpstreamOutcome.class);
        verify(metrics).onUpstreamOutcome(captor.capture());
        assertTrue(captor.getValue().empty(), "空正文必须被判成 empty");
        assertTrue(captor.getValue().truncated(), "finish_reason=length 必须被判成 truncated");
    }

    /**
     * 反例：有正文 + length 是**客户端的 max_tokens 设小了**，不是上游故障。
     * 这两类混在一起，"上游开始抽风"这个告警就永远不干净。
     */
    @Test
    void truncatedButNonEmptyIsNotCountedAsEmpty() {
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "被截断的半句", new Usage(33, 64), false, "length")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-6"))
                .expectNextCount(1)
                .verifyComplete();

        ArgumentCaptor<UpstreamOutcome> captor = ArgumentCaptor.forClass(UpstreamOutcome.class);
        verify(metrics).onUpstreamOutcome(captor.capture());
        assertFalse(captor.getValue().empty());
        assertTrue(captor.getValue().truncated());
    }

    /**
     * 正常响应也必须上报（否则 empty/truncated 两个计数器的分母无从知道）。
     * 顺带证明"正常"不会被误判成失败。
     */
    @Test
    void normalResponseIsReportedAsClean() {
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "完整答案", new Usage(33, 50), false, "stop")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-7"))
                .expectNextCount(1)
                .verifyComplete();

        ArgumentCaptor<UpstreamOutcome> captor = ArgumentCaptor.forClass(UpstreamOutcome.class);
        verify(metrics).onUpstreamOutcome(captor.capture());
        assertFalse(captor.getValue().empty());
        assertFalse(captor.getValue().truncated());
    }

    // --- 配额拒绝时不该有结算 ---

    /**
     * 被配额拦下的请求**根本没预扣**，自然不该结算。
     * 若结算在这里被调用，会给一个此前从未扣过的 key 加上一笔 —— 凭空制造用量。
     */
    @Test
    void quotaRejectedRequestIsNeverSettled() {
        when(quotaService.tryConsume(anyString(), anyLong())).thenReturn(Mono.just(false));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-8"))
                .expectError()
                .verify();

        verify(quotaService, never()).settle(anyString(), anyLong(), anyLong());
        verify(metrics).onQuotaRejected();
    }

    // --- finish_reason 必须带到响应对象上（供 Controller 透传） ---

    @Test
    void finishReasonReachesTheResponseObject() {
        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "", new Usage(33, 64), false, "length")));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-9"))
                .expectNextMatches(r -> "length".equals(r.finishReason()))
                .verifyComplete();
    }

    @Test
    void cacheHitResponseIsMarkedAsStopped() {
        when(cacheService.lookup(anyString(), anyString()))
                .thenReturn(Mono.just(new CacheEntry("缓存的答案", 1.0)));

        StepVerifier.create(pipeline.execute(KEY, request(), "trace-10"))
                .expectNextMatches(r -> "stop".equals(r.finishReason()))
                .verifyComplete();
    }

    @Test
    void reservedTokensComeFromTheFullMessageListNotTheLastMessage() {
        // P5 的口径：预扣要覆盖全部消息。这里用一条两倍长的 system + user 组合，
        // 断言预扣值大于"只算最后一条 user 消息"的估算 —— 若退回旧口径会立刻失败。
        long single = estimator.estimateText("什么是语义缓存");
        ChatRequest multi = new ChatRequest(MODEL, false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("system", "你是一个严谨的技术助手，回答要给出具体例子。"),
                        new ChatMessage("user", "什么是语义缓存")), "test");

        when(cacheService.lookup(anyString(), anyString())).thenReturn(Mono.empty());
        when(adapter.chat(eq(MODEL), any())).thenReturn(Mono.just(
                new ChatResponse("id-1", MODEL, "答案", new Usage(10, 20), false, "stop")));

        StepVerifier.create(pipeline.execute(KEY, multi, "trace-11"))
                .expectNextCount(1)
                .verifyComplete();

        ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
        verify(quotaService).settle(eq(KEY), captor.capture(), anyLong());
        assertTrue(captor.getValue() > single,
                "预扣必须覆盖 system + user 两条消息（旧实现只算最后一条 user 消息）");
        assertEquals(estimator.estimatePrompt(multi.messages()), captor.getValue());
    }
}
