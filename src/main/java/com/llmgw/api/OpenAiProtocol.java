package com.llmgw.api;

import com.llmgw.api.dto.Usage;
import com.llmgw.core.quota.QuotaExceededException;
import com.llmgw.core.resilience.UpstreamCircuitOpenException;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientException;

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
 *   C. finish_reason 必须**原样透传**上游给的值。这里曾经硬编码 "stop"，
 *      于是上游因预算耗尽而 {@code finish_reason=length} + 空正文的响应，
 *      会被网关报成"正常结束"—— 客户端连"这次被截断了"都无从判断（F0 修）。
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
     *
     * @param finishReason 上游原样返回的 finish_reason。**不能硬编码成 "stop"**：
     *                     F0 之前这里写死是 "stop"，于是被截断的响应
     *                     （{@code finish_reason=length} + 空正文）会告诉客户端
     *                     "正常结束了"。这比不返回该字段更坏 —— 客户端连猜的机会都没有。
     *                     为 null 时回退 "stop"（网关自己合成的响应没有上游值可透传）。
     */
    public static Map<String, Object> chatCompletionBody(String id, String model,
                                                         String content, Usage usage,
                                                         String finishReason) {
        Map<String, Object> body = envelope(id, OBJECT_COMPLETION, model);
        List<Map<String, Object>> choices = new ArrayList<>();

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("role", "assistant");
        message.put("content", content == null ? "" : content);

        Map<String, Object> choice = new LinkedHashMap<>();
        choice.put("index", 0);
        choice.put("message", message);
        choice.put("finish_reason", normalizeFinishReason(finishReason));
        choices.add(choice);

        body.put("choices", choices);
        body.put("usage", usageNode(usage));
        return body;
    }

    /**
     * 流式收尾帧：{@code delta} 是**空对象**、{@code finish_reason} 非 null。
     *
     * 这是 OpenAI SSE 规范里"流正常结束"的表达方式。不塞 delta.content 是关键 ——
     * 空 delta 分片在部分客户端上会被当成"又收到一段空回复"，
     * 所以这里刻意不复用 {@link #chatCompletionChunk}。
     */
    public static Map<String, Object> chatCompletionFinishChunk(String id, String model,
                                                               String finishReason) {
        Map<String, Object> body = envelope(id, OBJECT_CHUNK, model);
        List<Map<String, Object>> choices = new ArrayList<>();

        Map<String, Object> choice = new LinkedHashMap<>();
        choice.put("index", 0);
        choice.put("delta", new LinkedHashMap<String, Object>());
        choice.put("finish_reason", normalizeFinishReason(finishReason));
        choices.add(choice);

        body.put("choices", choices);
        return body;
    }

    /**
     * 流式文本分片。增量放在 choices[0].delta.content，与 OpenAI 一致。
     *
     * 分片的 finish_reason 一律为 null —— 文本分片不是结束信号。
     * 流的结束由两件事表达：{@link #chatCompletionFinishChunk}（带 finish_reason 的
     * 收尾帧）与 {@link #DONE} 帧。
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
     *   - 熔断打开              → 503：网关主动拒绝，客户端应<b>退避更久</b>（F1）
     *   - 上游出网异常          → 502：连不上上游、或上游返回非 2xx（F1 补）
     *   - IllegalStateException → 502：网关下游的问题（上游报错、空响应体）
     *   - 其他                  → 500
     *
     * <p><b>为什么 503 必须排在 502 前面、且不能合并</b>：
     * 502 的含义是"我替你问了上游，上游答得不对"，503 是"我现在不打算替你去问"。
     * 对客户端而言这是两种完全不同的指令 —— 收到 502 该立刻重试（上游可能抽风），
     * 收到 503 该退避（我们已经主动熔断，猛打只会把恢复期拖得更长）。
     * 把熔断也报成 502，就等于在网关已经开始自我保护的时候，
     * 还在用状态码鼓励客户端加速重试。
     *
     * <p><b>{@link WebClientException} 那一条是 F1 补上的，它修的是一个既有缺陷</b>：
     * {@code WebClientException} 继承 {@code NestedRuntimeException}，既不是
     * {@code IllegalStateException} 也不是 {@code IllegalArgumentException}，
     * 于是<b>上游任何非 2xx 响应都会落到最后一行变成 500</b> ——
     * 客户端会看到"网关自己崩了"，而实际问题出在下游。
     * 这个缺陷一直在，只是 F1 之前网关从不重试、上游一次 500 就直接穿过去，
     * 没人深究过状态码。重试把它变成了主路径（重试耗尽后必然落在这里），
     * 才由 scripts/check-resilience.py 的断言抓出来。
     *
     * <p>覆盖范围：连接被拒 / DNS 失败 / TLS 失败（{@code WebClientRequestException}）
     * 与上游返回非 2xx（{@code WebClientResponseException}）—— 两者都是
     * "网关的<b>下游</b>没能给出可用答案"，语义上都是 502。
     * 注意这里刻意<b>不透传</b>上游的原始状态码（例如上游 429 不会变成网关 429）：
     * 我们的 429 有明确含义（本租户配额用尽），混进上游的限流会让客户端
     * 分不清该找谁，见 README 的已知边界。
     *
     * <p>判断顺序在三处不可颠倒：
     * ① 熔断必须在 IllegalStateException <b>之前</b>判 —— 不过 UpstreamCircuitOpenException
     *    继承的是 RuntimeException（不是 IllegalStateException），所以这条顺序目前
     *    靠继承关系而不是靠顺序保证，写成显式判断是为了不依赖那个隐含前提。
     */
    public static HttpStatus statusOf(Throwable e) {
        if (e instanceof QuotaExceededException) {
            return HttpStatus.TOO_MANY_REQUESTS;
        }
        if (e instanceof UpstreamCircuitOpenException) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        if (e instanceof WebClientException) {
            return HttpStatus.BAD_GATEWAY;
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
        if (e instanceof UpstreamCircuitOpenException) {
            // 独立类型而不是复用 upstream_error：客户端据此区分"上游答错了(502)"
            // 与"网关主动熔断(503)"，两者的退避策略不同。
            return "circuit_open";
        }
        if (e instanceof WebClientException) {
            // 上游出网异常统一归到 upstream_error —— 与 statusOf 的 502 配对。
            // 显式写出来而不是靠兜底：这两处必须成对演进，分开写在两个地方
            // 却都不写，将来改一个忘一个是最容易发生的事。
            return "upstream_error";
        }
        if (e instanceof IllegalArgumentException) {
            return "invalid_request_error";
        }
        return "upstream_error";
    }

    // --- 内部 ---

    /**
     * finish_reason 兜底为 "stop"。
     *
     * 只在**上游没给**时兜底（网关自己合成的响应，如缓存命中、流式快照）。
     * 上游给了就原样透传 —— 包括 "length" 这种"不圆满"的值。
     * 把 length 也统一美化成 stop 是 F0 修掉的那个静默失效。
     */
    private static String normalizeFinishReason(String finishReason) {
        return (finishReason == null || finishReason.isBlank()) ? "stop" : finishReason;
    }

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
