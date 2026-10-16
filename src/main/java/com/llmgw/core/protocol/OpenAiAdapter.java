package com.llmgw.core.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.resilience.UpstreamEmptyBodyException;
import com.llmgw.core.token.TokenEstimator;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenAI 兼容适配器 —— 一份实现服务所有"兼容 OpenAI 协议"的供应商
 * （智谱 / 通义 / 豆包 / DeepSeek / Ollama…）。
 *
 * 因此它**不是 Spring bean**，而是由 {@link ProviderRegistry} 按配置为每家 provider
 * 各创建一个实例。旧实现是单例 @Component，构造时 findFirst() 只认第一个名为 openai
 * 的 provider —— 配置里加第二家在运行时完全不生效，且不报错。
 *
 * 协议一致性说明：这些供应商的请求体、响应体、鉴权头（Authorization: Bearer）格式一致，
 * 差异只在 base-url 与模型名。所以不需要为每家写一个适配器类，那只会制造几个内容
 * 几乎相同的类。真正需要独立适配器的是协议不同的情形（Claude 原生 API、
 * Gemini 原生 API、不起 HTTP 服务的本地推理）。
 *
 * 本类不阻塞任何线程：三个出网方法全部返回 Mono/Flux。
 *
 * 流式解析的三个坑（真机联调暴露）：
 *   ① Reactor 的 map 禁止返回 null。SSE 首个分片往往只有 delta.role 没有 content，
 *      path("content") 是 missing node，若用 .map(extractDelta) 会直接抛
 *      "The mapper ... returned a null value"。必须用 handle 过滤。
 *   ② SSE 里的注释/ping 事件 data 为 null，同理不能让 map 返回 null。
 *   ③ 末帧的 usage 出现在一个 choices 为空的分片里。旧实现把"没有 content 的分片"
 *      一律跳过，于是真实用量被丢掉 —— 这也是流式账本只能靠估算的直接原因。
 */
public class OpenAiAdapter implements ProviderAdapter {

    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE_TYPE =
            new ParameterizedTypeReference<>() {
            };

    /** ObjectMapper 是线程安全的，做成静态常量复用；按分片 new 一个非常浪费。 */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final GatewayProperties.Provider config;
    private final WebClient webClient;
    private final TokenEstimator tokenEstimator;

    /**
     * @param config  该 provider 的配置片段
     * @param builder 共享的 WebClient.Builder。**必须 clone 后再改**：
     *                DefaultWebClientBuilder.baseUrl() 是 mutating 的（设置字段后返回 this），
     *                多 provider 循环里直接复用会让后一次覆盖前一次，
     *                结果是所有适配器都指向同一个 base-url —— 且不会有任何报错。
     * @param tokenEstimator 估算口径由外部统一提供，避免"每家适配器各算一套"。
     */
    public OpenAiAdapter(GatewayProperties.Provider config, WebClient.Builder builder,
                         TokenEstimator tokenEstimator) {
        this.config = config;
        this.tokenEstimator = tokenEstimator;
        this.webClient = builder.clone()
                .baseUrl(config.getBaseUrl())
                .defaultHeader("Authorization", "Bearer " + config.getApiKey())
                .build();
    }

    @Override
    public String name() {
        return config.getName();
    }

    @Override
    public List<String> models() {
        return config.getModels();
    }

    @Override
    public boolean supports(String model) {
        return config.getModels().contains(model);
    }

    @Override
    public Mono<ChatResponse> chat(String model, ChatRequest request) {
        return webClient.post().uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body(model, false, request))
                .retrieve()
                .bodyToMono(JsonNode.class)
                // 空响应体是上游异常，不能当成"内容为空"放行 —— 那样会写进一条金额为 0 的假账
                // 用专用异常而不是 IllegalStateException("...")：
                // 重试谓词（F1）需要识别"这次失败值得再试一次"，靠 message 文本
                // 判断控制流，改一句文案就会静默改变重试行为。
                .switchIfEmpty(Mono.error(new UpstreamEmptyBodyException(model)))
                .map(node -> toChatResponse(node, model));
    }

    /**
     * 请求体。chat 与 chatStream 共用一份，避免"两条路径支持的参数集不一致"这种
     * 只能靠对照代码才发现的问题。
     *
     * 采样参数只在客户端真的传了时才进 payload：给上游塞一堆显式 null
     * 在部分实现上会被当作非法参数直接 400。
     */
    private Map<String, Object> body(String model, boolean stream, ChatRequest request) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", model);
        payload.put("stream", stream);
        payload.put("temperature", request.temperature() == null ? 0.2 : request.temperature());
        payload.put("messages", request.messages());

        if (request.maxTokens() != null) {
            payload.put("max_tokens", request.maxTokens());
        }
        if (request.topP() != null) {
            payload.put("top_p", request.topP());
        }
        if (request.stop() != null && !request.stop().isEmpty()) {
            payload.put("stop", request.stop());
        }
        if (request.seed() != null) {
            payload.put("seed", request.seed());
        }

        // 只有流式才需要：向末帧索取真实 usage。
        // 这是流式账本从"按字符数估算"变成"事实"的唯一途径。
        if (stream && config.isStreamIncludeUsage()) {
            payload.put("stream_options", Map.of("include_usage", true));
        }
        return payload;
    }

    /**
     * 响应归一。异常在 map 内抛出会被 Reactor 自动转成 onError，
     * 不需要手工 try/catch —— 这也是改成响应式后代码变短的原因之一。
     */
    private static ChatResponse toChatResponse(JsonNode node, String model) {
        if (node.has("error")) {
            JsonNode error = node.path("error");
            throw new IllegalStateException("upstream error: "
                    + error.path("message").asText(error.toString()));
        }
        String content = node.path("choices").path(0).path("message").path("content").asText("");
        // finish_reason 必须带出来（F0）。上游对"预算耗尽"的正式表达就是
        // finish_reason=length + content=""，丢掉它等于把"被截断"伪装成"正常结束"。
        String finishReason = node.path("choices").path(0).path("finish_reason").asText(null);
        JsonNode usage = node.path("usage");
        Usage u = new Usage(usage.path("prompt_tokens").asInt(0),
                usage.path("completion_tokens").asInt(0));
        String id = node.path("id").asText("chatcmpl");
        return new ChatResponse(id, model, content, u, false, finishReason);
    }

    @Override
    public Flux<StreamChunk> chatStream(String model, ChatRequest request) {
        return webClient.post().uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body(model, true, request))
                .retrieve()
                .bodyToFlux(SSE_TYPE)
                // 用 handle 而不是 map：data 为 null 的 ping/注释事件要丢弃，不能返回 null
                .handle((ServerSentEvent<String> sse, SynchronousSink<String> sink) -> {
                    String data = sse.data();
                    if (data != null) {
                        sink.next(data);
                    }
                })
                .takeUntil("[DONE]"::equals)
                .filter(d -> !"[DONE]".equals(d))
                // 用 flatMapIterable 而不是 handle：
                // **一帧上游分片可能同时携带多种信息**，必须允许一次映射出多个
                // StreamChunk。用 handle 的话，SynchronousSink 每次只允许发一个 ——
                // 第二个 sink.next() 会直接抛 "Cannot emit more than one data"。
                //
                // 这个坑真实踩到了：DeepSeek 的**末帧同时带 usage 与 finish_reason**
                // （include_usage=true 时），于是"用量分支 + 收尾分支"各发一次就把
                // 整条流打成 502。而在此之前它一直是好的 —— 因为 Delta 帧没有 usage、
                // 用量帧没有 choices，两个分支从未在同一次调用里同时命中。
                // 也就是说旧写法是**靠数据形状的巧合在运行**。
                .flatMapIterable(json -> parseChunk(json, model));
    }

    /**
     * 把一帧上游 SSE 的 JSON 解析成 0~2 个 {@link StreamChunk}。
     *
     * <p>抽成独立静态方法是为了**可测**：这个方法的唯一难点是"一帧可能同时是
     * 用量帧与收尾帧"，而它曾经因为"一次只能发一个"把整条流打成 502。
     * 埋在 {@code flatMapIterable} 的 lambda 里就没法为它写单测 ——
     * 而这个坑值得一条永久的回归断言。见 {@code OpenAiAdapterChunkTest}。
     *
     * @return 可能为空（该帧没有任何要转发的信息，例如 role-only / reasoning-only 帧）
     */
    static List<StreamChunk> parseChunk(String json, String model) {
        JsonNode node = readTreeOrNull(json);
        if (node == null) {
            return List.of();
        }
        List<StreamChunk> out = new ArrayList<>(2);

        // 用量帧：choices 为空、usage 有值。必须单独带出来 ——
        // 旧实现把它当"没有 content 的分片"丢掉，流式账本因此只能估算。
        JsonNode usage = node.path("usage");
        if (usage.has("prompt_tokens") || usage.has("total_tokens")) {
            out.add(StreamChunk.usage(model, new Usage(
                    usage.path("prompt_tokens").asInt(0),
                    usage.path("completion_tokens").asInt(0))));
        }
        // 文本增量帧。role-only / reasoning-only 分片在这里被跳过。
        JsonNode delta = node.path("choices").path(0).path("delta").path("content");
        if (!delta.isMissingNode() && !delta.isNull() && !delta.asText().isEmpty()) {
            out.add(StreamChunk.delta(model, delta.asText()));
        }
        // 收尾帧（F0）：上游在流末尾发的那个"带 finish_reason"的分片。
        // 必须单独带出来 —— 旧实现把它连同 reasoning-only 分片一起
        // 当成"没有 content 的分片"丢掉了，于是流式路径上客户端同样无法分辨
        // "正常结束"和"被 length 截断"（非流式那边的对应问题是
        // OpenAiProtocol 把 finish_reason 硬编码成 stop）。
        JsonNode finish = node.path("choices").path(0).path("finish_reason");
        if (!finish.isMissingNode() && !finish.isNull() && !finish.asText().isEmpty()) {
            out.add(StreamChunk.finish(model, finish.asText()));
        }
        return out;
    }

    private static JsonNode readTreeOrNull(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Mono<List<float[]>> embed(List<String> texts) {
        if (config.getEmbeddingModel() == null || config.getEmbeddingModel().isBlank()) {
            // 返回 onError 而不是空列表：空列表会被上层当成"取到了 0 个向量"继续走，
            // 让缓存静默失效；onError 至少能在日志里看见"这家没配 embedding"。
            return Mono.error(new IllegalStateException(
                    "embedding-model not configured for provider " + config.getName()));
        }
        return webClient.post().uri("/embeddings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("model", config.getEmbeddingModel(), "input", texts))
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(OpenAiAdapter::toVectors)
                .defaultIfEmpty(List.of());
    }

    private static List<float[]> toVectors(JsonNode node) {
        List<float[]> vectors = new ArrayList<>();
        for (JsonNode item : node.path("data")) {
            JsonNode embedding = item.path("embedding");
            float[] vec = new float[embedding.size()];
            for (int i = 0; i < embedding.size(); i++) {
                vec[i] = (float) embedding.get(i).asDouble();
            }
            vectors.add(vec);
        }
        return vectors;
    }

    @Override
    public Usage estimateUsage(String model, ChatRequest request, String answer) {
        // 交给统一的 TokenEstimator（P5）。旧实现是 messages 的 length/4 求和 ——
        // 英文公式 + 只看字符数，中文场景低估数倍。
        return tokenEstimator.estimateUsage(request.messages(), answer);
    }
}
