package com.llmgw.core.protocol;

import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * 厂商适配：把内部统一请求翻译成各厂商协议，并把响应归一。
 */
public interface ProviderAdapter {

    String name();

    List<String> models();

    boolean supports(String model);

    ChatResponse chat(String model, ChatRequest request);

    Flux<String> chatStream(String model, ChatRequest request);

    List<float[]> embed(List<String> texts);

    Usage estimateUsage(String model, ChatRequest request, String answer);
}
