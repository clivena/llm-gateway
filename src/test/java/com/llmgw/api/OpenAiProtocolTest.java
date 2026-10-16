package com.llmgw.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.api.dto.Usage;
import com.llmgw.core.quota.QuotaExceededException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

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
                "abc", "deepseek-flash", "你好", new Usage(10, 20)));

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

    @Test
    void usageKeysAreSnakeCase() throws Exception {
        // 这一条是本次修复的核心：Java DTO 是 camelCase，
        // 一旦漏出去客户端读到的 prompt_tokens 就是 undefined/0。
        JsonNode usage = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(11, 22))).get("usage");

        assertEquals(11, usage.get("prompt_tokens").asInt());
        assertEquals(22, usage.get("completion_tokens").asInt());
        assertEquals(33, usage.get("total_tokens").asInt());
    }

    @Test
    void camelCaseUsageKeysNeverLeak() throws Exception {
        JsonNode usage = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(1, 2))).get("usage");

        assertFalse(usage.has("promptTokens"), "不允许出现 camelCase 的 promptTokens");
        assertFalse(usage.has("completionTokens"), "不允许出现 camelCase 的 completionTokens");
    }

    @Test
    void cacheHitNeverEntersBody() throws Exception {
        // 规则 A：缓存命中只能走响应头。body 里出现 cacheHit
        // 就等于放弃"任何 OpenAI 客户端可直连"这个前提。
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", new Usage(1, 2)));

        assertFalse(node.has("cacheHit"));
        assertFalse(node.has("cache_hit"));
    }

    @Test
    void nullUsageFallsBackToZeros() throws Exception {
        JsonNode usage = json(OpenAiProtocol.chatCompletionBody(
                "abc", "m", "x", null)).get("usage");

        assertEquals(0, usage.get("prompt_tokens").asInt());
        assertEquals(0, usage.get("total_tokens").asInt());
    }

    @Test
    void nullContentBecomesEmptyStringNotNull() throws Exception {
        // 客户端对 null content 的处理千奇百怪，统一给空串。
        JsonNode node = json(OpenAiProtocol.chatCompletionBody("abc", "m", null, null));

        assertEquals("", node.get("choices").get(0).get("message").get("content").asText());
    }

    @Test
    void blankIdGetsPlaceholder() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionBody(null, "m", "x", null));

        assertEquals("chatcmpl-unknown", node.get("id").asText());
    }

    // --- 流式 ---

    @Test
    void chunkCarriesDeltaContentAndNullFinishReason() throws Exception {
        JsonNode node = json(OpenAiProtocol.chatCompletionChunk("cid", "m", "部分"));

        assertEquals("chat.completion.chunk", node.get("object").asText());
        JsonNode choice = node.get("choices").get(0);
        assertEquals("部分", choice.get("delta").get("content").asText());
        assertTrue(choice.get("finish_reason").isNull(), "分片的 finish_reason 应为 null，终止由 [DONE] 表达");
    }

    @Test
    void doneMarkerIsLiteralBracketDone() {
        assertEquals("[DONE]", OpenAiProtocol.DONE);
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

    @Test
    void unexpectedErrorMapsTo500() {
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR,
                OpenAiProtocol.statusOf(new RuntimeException("boom")));
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
