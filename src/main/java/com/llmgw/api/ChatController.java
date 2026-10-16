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

/**
 * OpenAI 兼容入口。同一个路径同时支持流式与非流式：
 * 手动写响应体，避免 Spring 因 produces 不同产生的映射歧义。
 *
 * 响应形状全部由 {@link OpenAiProtocol} 构造，本类只负责
 * "取数 → 挑分支 → 写头 → 写体"。形状定义集中在协议类里，
 * 这里不再手拼任何字段名。
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

    /**
     * 异常 → HTTP 状态码的映射与错误体形状都已收归 {@link OpenAiProtocol}：
     * 状态码和 body 必须成对演进，分开放最容易出现"改了状态码、忘了改 type"。
     * 相应断言见 OpenAiProtocolTest（原 ChatControllerStatusTest 已并入该文件）。
     */

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
            return stream(key, request, traceId, response, exchange);
        }
        return complete(key, request, traceId, response);
    }

    private Mono<Void> complete(String key, ChatRequest request, String traceId,
                                ServerHttpResponse response) {
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.setStatusCode(HttpStatus.OK);
        return pipeline.execute(key, request, traceId)
                .flatMap(resp -> {
                    // 缓存命中走响应头，不进 body —— 见 OpenAiProtocol 规则 A。
                    // 必须在 writeWith 之前设置：首次写出后头部就冻结了。
                    response.getHeaders().set(OpenAiProtocol.CACHE_HIT_HEADER,
                            String.valueOf(resp.cacheHit()));
                    Object body = OpenAiProtocol.chatCompletionBody(
                            resp.id(), resp.model(), resp.content(), resp.usage());
                    return response.writeWith(Mono.fromSupplier(() -> toBuffer(response, body)));
                })
                .onErrorResume(e -> {
                    setStatusIfUncommitted(response, OpenAiProtocol.statusOf(e));
                    return response.writeWith(
                            Mono.fromSupplier(() -> toBuffer(response, OpenAiProtocol.errorPayload(e))));
                });
    }

    private Mono<Void> stream(String key, ChatRequest request, String traceId,
                              ServerHttpResponse response, ServerWebExchange exchange) {
        response.getHeaders().setContentType(MediaType.TEXT_EVENT_STREAM);
        response.getHeaders().set("X-Accel-Buffering", "no");
        response.setStatusCode(HttpStatus.OK);

        // 分片 id 在整条流里保持一致：OpenAI 客户端按它把分片归到同一次回复。
        // 用 traceId 派生，客户端可以拿它去账本对账。
        String chunkId = "chatcmpl-" + traceId;
        // 已知不精确：分片里的 model 只能是**请求的**模型名。
        // 级联升级后实际服务的是更强的模型，但 ChatPipeline.executeStream 目前
        // 只吐文本、不吐实际模型名，所以在流式路径上拿不到真实值。
        // 要修得先改 executeStream 的返回契约（吐 (model, chunk) 结构），
        // 那是另一个改动，这里如实留一个已知缺口而不是假装准确。
        String model = request.resolvedModel();

        // toSseFrame 只接受已序列化的字符串，调用点必须自己 asJson ——
        // 这是刻意的：DONE 帧是字面量 "[DONE]" 而不是 JSON，
        // 如果让 toSseFrame 内部统一序列化，会把它变成 data: "[DONE]"（带引号），
        // 客户端就认不出来了。
        Flux<Flux<DataBuffer>> frames = pipeline.executeStream(key, request, traceId)
                .map(chunk -> toSseFrame(asJson(OpenAiProtocol.chatCompletionChunk(chunkId, model, chunk))))
                // 正常收尾必须补 [DONE]：客户端靠它判断"流结束了"，
                // 只看连接关闭会把正常结束和网络中断混为一谈。
                .concatWith(Flux.just(toSseFrame(OpenAiProtocol.DONE)))
                .onErrorResume(e -> {
                    // 首帧之前出错（配额、上游立刻拒绝）时响应还没提交，状态码还能改；
                    // 已经吐过分片就只能靠 SSE error 帧把错误带给客户端了。
                    setStatusIfUncommitted(response, OpenAiProtocol.statusOf(e));
                    return Flux.just(toSseFrame(asJson(OpenAiProtocol.errorPayload(e))));
                });
        return response.writeAndFlushWith(frames);
    }

    private static void setStatusIfUncommitted(ServerHttpResponse response, HttpStatus status) {
        try {
            response.setStatusCode(status);
        } catch (IllegalStateException alreadyCommitted) {
            // 流式响应已经开始写出：状态码改不动了，忽略即可（错误已由 SSE 帧传递）
        }
    }

    private DataBuffer toBuffer(ServerHttpResponse response, Object payload) {
        return response.bufferFactory().wrap(asJson(payload).getBytes(StandardCharsets.UTF_8));
    }

    private String asJson(Object payload) {
        try {
            return mapper.writeValueAsString(payload);
        } catch (Exception e) {
            return "{}";
        }
    }

    private Flux<DataBuffer> toSseFrame(String payload) {
        byte[] bytes = ("data: " + payload + "\n\n").getBytes(StandardCharsets.UTF_8);
        return Flux.just(org.springframework.core.io.buffer.DefaultDataBufferFactory.sharedInstance.wrap(bytes));
    }
}
