package com.llmgw.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.api.dto.Usage;
import com.llmgw.core.quota.QuotaExceededException;
import com.llmgw.core.resilience.UpstreamCircuitOpenException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 守住对外响应形状。
 *
 * 为什么断言"序列化后的 JSON"而不是直接读 Map：
 * 契约是**线上字节**。Map 里键写对了、但值类型或嵌套结构不对，
 * 客户端一样解析失败。所以这里统一走 ObjectMapper 再读 JsonNode。
 *
 * 这组测试要防的具体回归：网关曾经成功响应是扁平结构
 * （{content, usage:{promptTokens, completionTokens}, cacheHit}），
 * 而错误响应却是 OpenAI 形状，导致 agentkit 报"响应里没有 choices"。
 */
class OpenAiProtocolTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(Object payload) throws Exception {
        return MAPPER.readTree(MAPPER.writeValueAsString(payload));
    }

    // --- 非流式 ---

    @Test
    void completionBodyCarriesChoicesWithAssistantMessage() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(
                "abc", "deepseek-flash", "你好", new Usage(10, 20), "stop"));

        assertEquals("chat.completion", node.get("object").asText());
        assertEquals("abc", node.get("id").asText());
        assertEquals("deepseek-flash", node.get("model").asText());
        assertTrue(node.hasNonNull("created"), "created 必须存在");

        JsonNode choice = node.get("choices").get(0);
        assertEquals(0, choice.get("index").asInt());
        assertEquals("assistant", choice.get("message").get("role").asText());
        assertEquals("你好", choice.get("message").get("content").asText());
        assertEquals("stop", choice.get("finish_reason").asText());
    }

    /**
     * F0 的核心回归点：finish_reason 必须**原样透传**。
     *
     * 这里曾经硬编码 "stop"，于是上游因预算耗尽返回的
     * {@code finish_reason=length} + 空正文，会被网关报成"正常结束"。
     * 客户端于是完全没有办法分辨"答案就是空的"和"被截断了" —— 比不返回该字段更坏。
     */
    @Test
    void finishReasonIsPassedThroughNotHardcoded() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(
                "abc", "deepseek-flash", "", new Usage(33, 64), "length"));

        assertEquals("length", node.get("choices").get(0).get("finish_reason").asText(),
                "上游给的 length 必须原样出去，不能被美化成 stop");
    }

    @Test
    void nullFinishReasonFallsBackToStop() throws Exception {
        // 兜底只针对"上游没给"（缓存命中、流式快照这类网关自己合成的响应）。
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(1, 2), null));

        assertEquals("stop", node.get("choices").get(0).get("finish_reason").asText());
    }

    @Test
    void usageKeysAreSnakeCase() throws Exception {
        // 这一条是本次修复的核心：Java DTO 是 camelCase，
        // 一旦漏出去客户端读到的 prompt_tokens 就是 undefined/0。
        JsonNode usage = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(11, 22), null)).get("usage");

        assertEquals(11, usage.get("prompt_tokens").asInt());
        assertEquals(22, usage.get("completion_tokens").asInt());
        assertEquals(33, usage.get("total_tokens").asInt());
    }

    @Test
    void camelCaseUsageKeysNeverLeak() throws Exception {
        JsonNode usage = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(1, 2), null)).get("usage");

        assertFalse(usage.has("promptTokens"), "不允许出现 camelCase 的 promptTokens");
        assertFalse(usage.has("completionTokens"), "不允许出现 camelCase 的 completionTokens");
    }

    @Test
    void cacheHitNeverEntersBody() throws Exception {
        // 规则 A：缓存命中只能走响应头。body 里出现 cacheHit
        // 就等于放弃"任何 OpenAI 客户端可直连"这个前提。
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(1, 2), null));

        assertFalse(node.has("cacheHit"));
        assertFalse(node.has("cache_hit"));
    }

    @Test
    void nullUsageFallsBackToZeros() throws Exception {
        JsonNode usage = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", null, null)).get("usage");

        assertEquals(0, usage.get("prompt_tokens").asInt());
        assertEquals(0, usage.get("total_tokens").asInt());
    }

    @Test
    void nullContentBecomesEmptyStringNotNull() throws Exception {
        // 客户端对 null content 的处理千奇百怪，统一给空串。
        JsonNode node = json(OpenAiProtocol.chatCompletionBody("abc", "m", null, null, null));

        assertEquals("", node.get("choices").get(0).get("message").get("content").asText());
    }

    @Test
    void blankIdGetsPlaceholder() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(null, "m", "x", null, null));

        assertEquals("chatcmpl-unknown", node.get("id").asText());
    }

    // --- 流式 ---

    @Test
    void chunkCarriesDeltaContentAndNullFinishReason() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionChunk("cid", "m", "部分"));

        assertEquals("chat.completion.chunk", node.get("object").asText());
        JsonNode choice = node.get("choices").get(0);
        assertEquals("部分", choice.get("delta").get("content").asText());
        assertTrue(choice.get("finish_reason").isNull(),
                "文本分片的 finish_reason 必须为 null —— 流结束由收尾帧与 [DONE] 共同表达");
    }

    /**
     * F0：流式也必须能把"被截断"告诉客户端。
     *
     * 收尾帧的形状是 {@code delta: {}}（空对象）+ 非 null 的 finish_reason。
     * 不转发这个帧的话，流式路径上"正常结束"与"被 length 截断"长得一模一样：
     * 用户只看到一个突然停住的答案，然后收到 [DONE]。
     */
    @Test
    void finishChunkCarriesReasonWithEmptyDeltaObject() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionFinishChunk("cid", "m", "length"));

        JsonNode choice = node.get("choices").get(0);
        assertEquals("length", choice.get("finish_reason").asText());
        assertTrue(choice.get("delta").isObject(), "delta 必须是空**对象**，不是 null");
        assertEquals(0, choice.get("delta").size(), "收尾帧的 delta 里不能有任何字段");
    }

    @Test
    void finishChunkNeverCarriesDeltaContent() throws Exception {
        // 空 delta.content 分片在部分客户端上会被当成"又收到一段空回复"。
        // 这正是收尾帧不复用 chatCompletionChunk 的原因。
        JsonNode node = json(OpenAiProtocol.chatCompletionFinishChunk("cid", "m", "stop"));

        assertFalse(node.get("choices").get(0).get("delta").has("content"));
    }

    @Test
    void finishChunkNormalizesBlankReasonToStop() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionFinishChunk("cid", "m", ""));

        assertEquals("stop", node.get("choices").get(0).get("finish_reason").asText());
    }

    @Test
    void finishChunkCarriesNoUsageNode() throws Exception {
        // 收尾帧与用量帧是两帧：客户端可能只要其一（include_usage 是可选的）。
        JsonNode node = json(OpenAiProtocol.chatCompletionFinishChunk("cid", "m", "stop"));

        assertFalse(node.has("usage"));
    }

    @Test
    void doneMarkerIsLiteralBracketDone() {
        assertEquals("[DONE]", OpenAiProtocol.DONE);
    }

    @Test
    void usageChunkHasEmptyChoicesAndSnakeCaseUsage() throws Exception {
        // include_usage 那一帧的形状：choices 是空数组，usage 非空。
        // 空数组不能写成"含一个空 delta 的 choice"—— 客户端会当成又收到一段空回复。
        JsonNode node = json(OpenAiProtocol.chatCompletionUsageChunk(
                "cid", "deepseek-flash", new Usage(7, 9)));

        assertEquals("chat.completion.chunk", node.get("object").asText());
        assertEquals("deepseek-flash", node.get("model").asText());
        assertTrue(node.get("choices").isArray(), "choices 必须是数组");
        assertEquals(0, node.get("choices").size(), "用量帧的 choices 必须为空");
        assertEquals(7, node.get("usage").get("prompt_tokens").asInt());
        assertEquals(9, node.get("usage").get("completion_tokens").asInt());
        assertEquals(16, node.get("usage").get("total_tokens").asInt());
    }

    @Test
    void usageChunkCarriesNoDeltaNode() throws Exception {
        // delta 一旦出现，客户端就会把它当成文本分片去追加。
        JsonNode node = json(OpenAiProtocol.chatCompletionUsageChunk(
                "cid", "m", new Usage(1, 1)));

        assertFalse(node.has("delta"));
    }

    // --- 错误 ---

    @Test
    void quotaExceededMapsTo429WithRateLimitType() throws Exception {
        QuotaExceededException e = new QuotaExceededException("tenant-a");
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, OpenAiProtocol.statusOf(e));

        JsonNode node = json(OpenAiProtocol.errorPayload(e));
        assertEquals("rate_limit_exceeded", node.get("error").get("type").asText());
        assertTrue(node.get("error").get("message").asText().contains("quota exceeded"),
                "verify-quota-ledger.py 靠这个字符串识别配额拦截");
    }

    @Test
    void upstreamFailureMapsTo502() {
        assertEquals(HttpStatus.BAD_GATEWAY,
                OpenAiProtocol.statusOf(new IllegalStateException("upstream error: rate limited")));
    }

    /**
     * 熔断打开 → 503，且必须是独立的 error type（F1）。
     *
     * <p>与 502 分开的理由不是"更精确"，而是<b>给客户端的指令不同</b>：
     * 502 = "我替你问了，上游答得不对" → 客户端可以立刻重试；
     * 503 = "我现在不打算替你去问" → 客户端必须退避。
     * 把熔断也报成 502，就等于在网关已经开始自我保护的时候，
     * 还用状态码鼓励客户端加速重试 —— 那会把恢复期又拖长。
     */
    @Test
    void circuitOpenMapsTo503WithItsOwnErrorType() throws Exception {
        UpstreamCircuitOpenException e = new UpstreamCircuitOpenException(
                "deepseek", new RuntimeException("breaker open"));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, OpenAiProtocol.statusOf(e));

        JsonNode node = json(OpenAiProtocol.errorPayload(e));
        assertEquals("circuit_open", node.get("error").get("type").asText(),
                "熔断必须有自己的 error type，不能混进 upstream_error");
        assertTrue(node.get("error").get("message").asText().contains("deepseek"),
                "错误信息要指出是哪家 provider 的链路被判定为不可用");
    }

    @Test
    void emptyAnswerStillMapsTo502Not503() {
        // 反面：空正文是"上游答了但没答出来"（502），不是"网关拒绝发请求"（503）。
        // 两者都发生在故障期，混在一起会让告警分不清该查上游还是查网关配置。
        assertEquals(HttpStatus.BAD_GATEWAY, OpenAiProtocol.statusOf(
                new com.llmgw.core.resilience.UpstreamEmptyResponseException(
                        new com.llmgw.api.dto.ChatResponse("id", "m", "",
                                new com.llmgw.api.dto.Usage(1, 2), false, "length"))));
    }

    @Test
    void unexpectedErrorMapsTo500() {
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR,
                OpenAiProtocol.statusOf(new RuntimeException("boom")));
    }

    /**
     * <b>F1 补的一个既有缺陷</b>：上游任何非 2xx 都曾落到 500。
     *
     * <p>{@code WebClientResponseException} 继承 {@code NestedRuntimeException}，
     * 既不是 {@code IllegalStateException} 也不是 {@code IllegalArgumentException}，
     * 于是穿过所有分支掉进最后的 {@code → 500}。客户端看到的是"网关自己崩了"，
     * 而实际问题在下游 —— 排查方向从一开始就是错的。
     *
     * <p>这个缺陷一直存在，只是 F1 之前网关从不重试、上游一次 500 就直接穿过去，
     * 没人核对过状态码。重试让"重试耗尽"变成一条必然走到的路径之后，
     * 它才由 scripts/check-resilience.py 的真机断言抓出来。
     */
    @Test
    void upstreamHttpErrorMapsTo502Not500() {
        for (int code : new int[]{400, 429, 500, 502, 503}) {
            WebClientResponseException e = WebClientResponseException.create(
                    code, "from upstream", HttpHeaders.EMPTY, new byte[0], null);
            assertEquals(HttpStatus.BAD_GATEWAY, OpenAiProtocol.statusOf(e),
                    "上游 HTTP " + code + " 必须报 502（网关的下游问题），不是 500");
        }
    }

    /** 连不上上游（连接被拒 / DNS / TLS）同样是"下游不可达" → 502。 */
    @Test
    void upstreamConnectionFailureMapsTo502() {
        WebClientRequestException e = new WebClientRequestException(
                new RuntimeException("connection refused"), HttpMethod.POST,
                java.net.URI.create("http://127.0.0.1:9/v1/chat/completions"), HttpHeaders.EMPTY);
        assertEquals(HttpStatus.BAD_GATEWAY, OpenAiProtocol.statusOf(e));
    }

    @Test
    void badRequestMapsTo400Not502() throws Exception {
        // 回归点：messages 为空、model 不在服务范围都属于**调用方**错误。
        // 旧实现把它报成 502，客户端会误判为网关故障去退避重试，而重试永远没用。
        IllegalArgumentException e = new IllegalArgumentException("messages 不能为空");

        assertEquals(HttpStatus.BAD_REQUEST, OpenAiProtocol.statusOf(e));

        JsonNode node = json(OpenAiProtocol.errorPayload(e));
        assertEquals("invalid_request_error", node.get("error").get("type").asText(),
                "400 必须配 invalid_request_error，否则客户端按 type 分流还是错的");
    }

    @Test
    void nonQuotaErrorIsTypedUpstreamError() throws Exception {
        JsonNode node = json(OpenAiProtocol.errorPayload(new RuntimeException("boom")));
        assertEquals("upstream_error", node.get("error").get("type").asText());
    }

    // --- /v1/models ---

    @Test
    void modelsBodyListsEntriesInOpenAiShape() throws Exception {
        JsonNode node = json(OpenAiProtocol.modelsBody(List.of("deepseek-flash", "gpt-4o-mini")));

        assertEquals("list", node.get("object").asText());
        assertEquals(2, node.get("data").size());

        JsonNode first = node.get("data").get(0);
        assertEquals("deepseek-flash", first.get("id").asText());
        assertEquals("model", first.get("object").asText());
        assertEquals("llm-gateway", first.get("owned_by").asText());
        assertNotNull(first.get("created"));
    }
}
