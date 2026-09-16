package com.llmgw.core.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OpenAI 兼容适配器。覆盖三件事：非流式调用、SSE 流式调用、embedding。
 * 关键点：SSE 用 ServerSentEvent 解码，避免手工按行切分导致分片错位。
 *
 * 流式解析的两个坑（真机联调暴露）：
 *   ① Reactor 的 map 禁止返回 null。SSE 首个分片往往只有 delta.role 没有 content，
 *      path("content") 是 missing node，若用 .map(extractDelta) 会直接抛
 *      "The mapper ... returned a null value"。必须用 handle 过滤。
 *   ② SSE 里的注释/ping 事件 data 为 null，同理不能让 map 返回 null。
 */
@Component
public class OpenAiAdapter implements ProviderAdapter {

    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE_TYPE =
            new ParameterizedTypeReference<>() {
            };

    /** ObjectMapper 是线程安全的，做成静态常量复用；按分片 new 一个非常浪费。 */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final GatewayProperties.Provider config;
    private final WebClient webClient;

    public OpenAiAdapter(GatewayProperties properties, WebClient.Builder builder) {
        this.config = properties.getProviders().stream()
                .filter(p -> "openai".equalsIgnoreCase(p.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("provider 'openai' not configured"));
        this.webClient = builder.baseUrl(config.getBaseUrl())
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
    public ChatResponse chat(String model, ChatRequest request) {
        Map<String, Object> payload = Map.of(
                "model", model,
                "stream", false,
                "temperature", request.temperature() == null ? 0.2 : request.temperature(),
                "messages", request.messages());

        JsonNode node = webClient.post().uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (node == null) {
            throw new IllegalStateException("empty response from upstream");
        }
        if (node.has("error")) {
            throw new IllegalStateException("upstream error: " + node.path("error").path("message").asText(node.path("error").toString()));
        }
        String content = node.path("choices").path(0).path("message").path("content").asText("");
        JsonNode usage = node.path("usage");
        Usage u = new Usage(usage.path("prompt_tokens").asInt(0), usage.path("completion_tokens").asInt(0));
        String id = node.path("id").asText("chatcmpl");
        return new ChatResponse(id, model, content, u, false);
    }

    @Override
    public Flux<String> chatStream(String model, ChatRequest request) {
        Map<String, Object> payload = Map.of(
                "model", model,
                "stream", true,
                "temperature", request.temperature() == null ? 0.2 : request.temperature(),
                "messages", request.messages());

        return webClient.post().uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(payload)
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
                // 同理：没有 content 的分片（role-only、reasoning-only）直接跳过
                .handle((String json, SynchronousSink<String> sink) -> {
                    String delta = extractDelta(json);
                    if (delta != null && !delta.isEmpty()) {
                        sink.next(delta);
                    }
                });
    }

    private String extractDelta(String json) {
        try {
            JsonNode delta = MAPPER.readTree(json).path("choices").path(0).path("delta").path("content");
            return delta.isMissingNode() || delta.isNull() ? null : delta.asText();
        } catch (Exception ignored) {
            return null;
        }
    }

    @Override
    public List<float[]> embed(List<String> texts) {
        if (config.getEmbeddingModel() == null || config.getEmbeddingModel().isBlank()) {
            throw new IllegalStateException("embedding-model not configured for provider " + config.getName());
        }
        JsonNode node = webClient.post().uri("/embeddings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("model", config.getEmbeddingModel(), "input", texts))
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        List<float[]> vectors = new ArrayList<>();
        if (node != null) {
            for (JsonNode item : node.path("data")) {
                JsonNode embedding = item.path("embedding");
                float[] vec = new float[embedding.size()];
                for (int i = 0; i < embedding.size(); i++) {
                    vec[i] = (float) embedding.get(i).asDouble();
                }
                vectors.add(vec);
            }
        }
        return vectors;
    }

    @Override
    public Usage estimateUsage(String model, ChatRequest request, String answer) {
        int prompt = request.messages().stream()
                .mapToInt(m -> m.content() == null ? 0 : m.content().length() / 4)
                .sum();
        int completion = answer == null ? 0 : answer.length() / 4;
        return new Usage(prompt, completion);
    }
}
