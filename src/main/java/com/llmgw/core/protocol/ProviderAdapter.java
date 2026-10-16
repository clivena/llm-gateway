package com.llmgw.core.protocol;

import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 厂商适配：把内部统一请求翻译成各厂商协议，并把响应归一。
 *
 * 为什么出网方法返回 Mono/Flux 而不是同步值（P1 响应式化）：
 *
 *   旧签名是同步的，实现里各自 .block()，调用方再用
 *   Mono.fromCallable(...).subscribeOn(boundedElastic()) 把阻塞挪出事件循环线程。
 *   这带来三个后果：
 *     ① 并发上限被弹性线程池大小锁死（默认 10 × CPU 核数），而"少量线程扛高并发"
 *        恰恰是引入 WebFlux 的全部理由；
 *     ② 取消传播断裂 —— 客户端断开后上游请求仍在跑，费用照产生；
 *     ③ Reactor 的 timeout / retryWhen / circuitBreaker 是算子，必须挂在 Mono/Flux 上，
 *        接口是同步的就全都用不上 —— 这是后续重试与熔断的硬前提。
 *
 *   WebClient 本来就返回 Mono，.block() 是主动加上去的：去掉它代码不是变长，是变短。
 *
 * 契约：所有方法都不得阻塞调用线程。
 */
public interface ProviderAdapter {

    String name();

    List<String> models();

    boolean supports(String model);

    /** 非流式对话。上游错误以 onError 形式传播，不再靠抛同步异常。 */
    Mono<ChatResponse> chat(String model, ChatRequest request);

    /** 流式对话。分片带出"实际服务的模型"与"真实用量"，两者都是记账与回显的依据。 */
    Flux<StreamChunk> chatStream(String model, ChatRequest request);

    /** 批量取向量。上游不支持该能力时应返回 onError，而不是用空列表冒充成功。 */
    Mono<List<float[]>> embed(List<String> texts);

    /** 估算用量（纯计算，无 IO）。仅在拿不到上游真实 usage 时作为兜底。 */
    Usage estimateUsage(String model, ChatRequest request, String answer);
}
