package com.llmgw.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.core.pipeline.ChatPipeline;
import com.llmgw.observability.TraceIdFilter;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * OpenAI 兼容入口。同一个路径同时支持流式与非流式：
 * 手动写响应体，避免 Spring 因 produces 不同产生的映射歧义。
 */
@RestController
@RequestMapping("/v1")
public class ChatController {

    private final ChatPipeline pipeline;
    private final ObjectMapper mapper = new ObjectMapper();

    public ChatController(ChatPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @PostMapping("/chat/completions")
    public Mono<Void> chat(@RequestHeader(value = "X-Api-Key", required = false) String apiKey,
                           @RequestBody ChatRequest request,
                           ServerWebExchange exchange,
                           ServerHttpResponse response) {
        String key = (apiKey == null || apiKey.isBlank()) ? "anonymous" : apiKey;
        // traceId 由 TraceIdFilter 生成/透传并存在 exchange attribute 里。
        // 必须显式取出来往下传：cost_ledger.trace_id 是 NOT NULL，
        // 之前一路传 null，账本落库会直接失败（这也是"全链路可观测"的实际断点）。
        String traceId = (String) exchange.getAttributes().get(TraceIdFilter.HEADER);

        if (request.isStream()) {
            response.setStatusCode(HttpStatus.OK);
            response.getHeaders().setContentType(MediaType.TEXT_EVENT_STREAM);
            response.getHeaders().set("X-Accel-Buffering", "no");
            Flux<Flux<DataBuffer>> frames = pipeline.executeStream(key, request, traceId)
                    .map(this::toSseFrame)
                    .onErrorResume(e -> Flux.just(toSseFrame(errorJson(e))));
            return response.writeAndFlushWith(frames);
        }

        response.setStatusCode(HttpStatus.OK);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        return pipeline.execute(key, request, traceId)
                .flatMap(resp -> response.writeWith(Mono.fromSupplier(() -> toBuffer(response, resp))))
                .onErrorResume(e -> response.writeWith(Mono.fromSupplier(
                        () -> toBuffer(response, Map.of("error", String.valueOf(e.getMessage()))))));
    }

    private DataBuffer toBuffer(ServerHttpResponse response, Object payload) {
        try {
            byte[] bytes = mapper.writeValueAsBytes(payload);
            return response.bufferFactory().wrap(bytes);
        } catch (Exception e) {
            return response.bufferFactory().wrap("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    private String errorJson(Throwable e) {
        return "{\"error\":\"" + String.valueOf(e.getMessage()).replace("\"", "'") + "\"}";
    }

    private Flux<DataBuffer> toSseFrame(String payload) {
        byte[] bytes = ("data: " + payload + "\n\n").getBytes(StandardCharsets.UTF_8);
        return Flux.just(org.springframework.core.io.buffer.DefaultDataBufferFactory.sharedInstance.wrap(bytes));
    }
}
