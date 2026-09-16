package com.llmgw.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.core.pipeline.ChatPipeline;
import com.llmgw.core.quota.QuotaExceededException;
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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * OpenAI 兼容入口。同一个路径同时支持流式与非流式：
 * 手动写响应体，避免 Spring 因 produces 不同产生的映射歧义。
 *
 * 错误状态码约定（旧实现把所有错误都写成 HTTP 200 + 响应体里的 error 字段，
 * 客户端没法用状态码做退避决策，也不符合 OpenAI 兼容协议）：
 *   - 配额超额     → 429 Too Many Requests
 *   - 上游/适配异常 → 502 Bad Gateway
 *   - 其他         → 500
 */
@RestController
@RequestMapping("/v1")
public class ChatController {

    private final ChatPipeline pipeline;
    private final ObjectMapper mapper = new ObjectMapper();

    public ChatController(ChatPipeline pipeline) {
        this.pipeline = pipeline;
    }

    /** 异常 → HTTP 状态码的映射。抽成静态方法是为了能单测（见 ChatControllerStatusTest）。 */
    static HttpStatus statusOf(Throwable e) {
        if (e instanceof QuotaExceededException) {
            return HttpStatus.TOO_MANY_REQUESTS;
        }
        if (e instanceof IllegalStateException) {
            // 上游返回错误、没有可用适配器等，都归为"网关下游的问题"
            return HttpStatus.BAD_GATEWAY;
        }
        return HttpStatus.INTERNAL_SERVER_ERROR;
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
            response.getHeaders().setContentType(MediaType.TEXT_EVENT_STREAM);
            response.getHeaders().set("X-Accel-Buffering", "no");
            response.setStatusCode(HttpStatus.OK);
            Flux<Flux<DataBuffer>> frames = pipeline.executeStream(key, request, traceId)
                    .map(this::toSseFrame)
                    .onErrorResume(e -> {
                        // 首帧之前出错（配额、上游立刻拒绝）时响应还没提交，状态码还能改；
                        // 已经吐过分片就只能靠 SSE error 帧把错误带给客户端了。
                        setStatusIfUncommitted(response, statusOf(e));
                        return Flux.just(toSseFrame(encodeError(e)));
                    });
            return response.writeAndFlushWith(frames);
        }

        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.setStatusCode(HttpStatus.OK);
        return pipeline.execute(key, request, traceId)
                .flatMap(resp -> response.writeWith(Mono.fromSupplier(() -> toBuffer(response, resp))))
                .onErrorResume(e -> {
                    setStatusIfUncommitted(response, statusOf(e));
                    return response.writeWith(Mono.fromSupplier(() -> toBuffer(response, errorPayload(e))));
                });
    }

    private static void setStatusIfUncommitted(ServerHttpResponse response, HttpStatus status) {
        try {
            response.setStatusCode(status);
        } catch (IllegalStateException alreadyCommitted) {
            // 流式响应已经开始写出：状态码改不动了，忽略即可（错误已由 SSE 帧传递）
        }
    }

    private DataBuffer toBuffer(ServerHttpResponse response, Object payload) {
        try {
            byte[] bytes = mapper.writeValueAsBytes(payload);
            return response.bufferFactory().wrap(bytes);
        } catch (Exception e) {
            return response.bufferFactory().wrap("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** OpenAI 形状的错误体：客户端按 error.type 分流比按字符串匹配可靠。 */
    private Map<String, Object> errorPayload(Throwable e) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("message", String.valueOf(e.getMessage()));
        error.put("type", e instanceof QuotaExceededException ? "rate_limit_exceeded" : "upstream_error");
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", error);
        return body;
    }

    private String encodeError(Throwable e) {
        try {
            return mapper.writeValueAsString(errorPayload(e));
        } catch (Exception ignored) {
            return "{\"error\":{\"message\":\"internal error\",\"type\":\"upstream_error\"}}";
        }
    }

    private Flux<DataBuffer> toSseFrame(String payload) {
        byte[] bytes = ("data: " + payload + "\n\n").getBytes(StandardCharsets.UTF_8);
        return Flux.just(org.springframework.core.io.buffer.DefaultDataBufferFactory.sharedInstance.wrap(bytes));
    }
}
