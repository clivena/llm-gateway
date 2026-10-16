package com.llmgw.api;

import com.llmgw.api.dto.Usage;
import com.llmgw.core.quota.QuotaExceededException;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenAI 协议形状的集中构造点。
 *
 * 为什么要把这些 Map 的拼装从 Controller 里抽出来：
 * 响应形状是**对外契约**，散在控制器的分支里最容易出现"某一支改了、另一支漏了"。
 * 之前就是这个状态 —— 请求侧已经是 OpenAI 形状，**错误响应**也是
 * （{"error":{"message","type"}}），唯独**成功响应**还是自家的扁平结构
 * （{id, model, content, usage:{promptTokens, completionTokens}, cacheHit}）。
 * 半兼容比完全不兼容更坏：客户端以为握手成功了，直到解析 choices 才炸。
 * 症状是一次真机联调里 agentkit 报"响应里没有 choices"，而网关侧
 * 其实已经完整跑通（上游真实调用、usage 真实、账本也落了账）。
 *
 * 两条硬规则：
 *   A. body 保持**纯 OpenAI 形状**。缓存命中这类网关私有信息一律走响应头
 *      （{@link #CACHE_HIT_HEADER}），塞进 body 就破坏了"任何 OpenAI 客户端可直连"这个前提。
 *   B. usage 字段名必须是 snake_case（prompt_tokens / completion_tokens / total_tokens）。
 *      Java DTO 的 camelCase 一旦漏出去，客户端读到的就是 0。
 */
public final class OpenAiProtocol {

    /** 缓存命中标记。放响应头而不是 body，见规则 A。 */
    public static final String CACHE_HIT_HEADER = "X-Cache-Hit";

    /** SSE 流的终止帧标记（它本身不是 JSON，是一段字面量）。 */
    public static final String DONE = "[DONE]";

    private static final String OBJECT_COMPLETION = "chat.completion";
    private static final String OBJECT_CHUNK = "chat.completion.chunk";
    private static final String OBJECT_MODEL = "model";
    private static final String OBJECT_LIST = "list";
    private static final String OWNED_BY = "llm-gateway";

    private OpenAiProtocol() {
    }

    /**
     * 非流式成功响应。
     *
     * 注意入参里**没有 cacheHit** —— 这是刻意的：命中信息由调用方写进响应头，
     * 让它进不了这个 body 构造器，就没法被顺手塞进去。
     */
    public static Map<String, Object> chatCompletionBody(String id, String model,
                                                         String content, Usage usage) {
        Map<String, Object> body = envelope(id, OBJECT_COMPLETION, model);
        List<Map<String, Object>> choices = new ArrayList<>();

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("role", "assistant");
        message.put("content", content == null ? "" : content);

        Map<String, Object> choice = new LinkedHashMap<>();
        choice.put("index", 0);
        choice.put("message", message);
        choice.put("finish_reason", "stop");
        choices.add(choice);

        body.put("choices", choices);
        body.put("usage", usageNode(usage));
        return body;
    }

    /**
     * 流式分片。增量放在 choices[0].delta.content，与 OpenAI 一致。
     * 分片的 finish_reason 一律为 null —— 终止由 {@link #DONE} 帧表达。
     */
    public static Map<String, Object> chatCompletionChunk(String id, String model, String delta) {
        Map<String, Object> body = envelope(id, OBJECT_CHUNK, model);
        List<Map<String, Object>> choices = new ArrayList<>();

        Map<String, Object> deltaNode = new LinkedHashMap<>();
        deltaNode.put("content", delta == null ? "" : delta);

        Map<String, Object> choice = new LinkedHashMap<>();
        choice.put("index", 0);
        choice.put("delta", deltaNode);
        choice.put("finish_reason", null);
        choices.add(choice);

        body.put("choices", choices);
        return body;
    }

    /**
     * 纯用量分片。对应上游在 include_usage=true 时回的那一帧：
     * choices 是**空数组**、usage 非空。
     *
     * 为什么单独一个方法而不是复用 chatCompletionChunk：后者会塞一个
     * delta.content="" 的 choices 进来。空 delta 分片在部分客户端上会被当成
     * "又收到一段空回复"，而正确的形状恰恰是没有任何 choices。
     */
    public static Map<String, Object> chatCompletionUsageChunk(String id, String model, Usage usage) {
        Map<String, Object> body = envelope(id, OBJECT_CHUNK, model);
        body.put("choices", new ArrayList<>());
        body.put("usage", usageNode(usage));
        return body;
    }

    /** GET /v1/models 的响应体。 */
    public static Map<String, Object> modelsBody(List<String> modelIds) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (String id : modelIds) {
            data.add(modelEntry(id));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("object", OBJECT_LIST);
        body.put("data", data);
        return body;
    }

    public static Map<String, Object> modelEntry(String id) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("id", id);
        entry.put("object", OBJECT_MODEL);
        entry.put("created", now());
        entry.put("owned_by", OWNED_BY);
        return entry;
    }

    /**
     * 异常 → HTTP 状态码。客户端靠状态码决定"退避还是重试"，
     * 把所有错误都写成 200 会让退避逻辑无从下手。
     *
     * 判断顺序有意义：把"请求不合法"和"上游挂了"分开是这里唯一容易做错的决定。
     * 旧实现只有 IllegalStateException → 502 一条，于是 messages 为空、model 不在
     * 服务范围这类**调用方错误**全部被报成 502，客户端会误以为是网关侧故障去退避重试，
     * 而重试一万次结果都一样。
     *   - IllegalArgumentException → 400：错在调用方（messages 为空、model 不在服务范围）
     *   - 配额超额              → 429：客户端应退避
     *   - IllegalStateException → 502：网关下游的问题（上游报错、空响应体）
     *   - 其他                  → 500
     */
    public static HttpStatus statusOf(Throwable e) {
        if (e instanceof QuotaExceededException) {
            return HttpStatus.TOO_MANY_REQUESTS;
        }
        if (e instanceof IllegalArgumentException) {
            return HttpStatus.BAD_REQUEST;
        }
        if (e instanceof IllegalStateException) {
            return HttpStatus.BAD_GATEWAY;
        }
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }

    /** OpenAI 形状的错误体：客户端按 error.type 分流比按字符串匹配可靠。 */
    public static Map<String, Object> errorPayload(Throwable e) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("message", String.valueOf(e.getMessage()));
        error.put("type", errorTypeOf(e));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", error);
        return body;
    }

    private static String errorTypeOf(Throwable e) {
        if (e instanceof QuotaExceededException) {
            return "rate_limit_exceeded";
        }
        if (e instanceof IllegalArgumentException) {
            return "invalid_request_error";
        }
        return "upstream_error";
    }

    // --- 内部 ---

    private static Map<String, Object> envelope(String id, String object, String model) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", (id == null || id.isBlank()) ? "chatcmpl-unknown" : id);
        body.put("object", object);
        body.put("created", now());
        body.put("model", model == null ? "" : model);
        return body;
    }

    /** usage 缺失时给 0，绝不让 null 漏进 body（客户端会当成解析失败）。 */
    private static Map<String, Object> usageNode(Usage usage) {
        int prompt = usage == null ? 0 : usage.promptTokens();
        int completion = usage == null ? 0 : usage.completionTokens();
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("prompt_tokens", prompt);
        node.put("completion_tokens", completion);
        node.put("total_tokens", prompt + completion);
        return node;
    }

    private static long now() {
        return Instant.now().getEpochSecond();
    }
}
