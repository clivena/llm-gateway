package com.llmgw.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * OpenAI 兼容的对话请求。
 *
 * 命名约定：对外用 OpenAI 的 snake_case，Java 侧保持 camelCase，靠 @JsonProperty 桥接。
 * 这个桥接不能省 —— Spring Boot 的 Jackson 默认不做 snake_case 转换，
 * 少了注解这些参数会被**静默丢弃**：客户端传了、网关收了、但没有转发给上游，
 * 表现为"参数不生效"，且没有任何报错可查。
 *
 * 采样参数（max_tokens / top_p / stop / seed）只做透传，网关不解释它们；
 * feature 是本网关的私有扩展（成本归因的业务线标签），不属于 OpenAI 协议。
 */
public record ChatRequest(
        String model,
        Boolean stream,
        Double temperature,
        @JsonProperty("max_tokens") Integer maxTokens,
        @JsonProperty("top_p") Double topP,
        List<String> stop,
        Integer seed,
        @JsonProperty("stream_options") StreamOptions streamOptions,
        List<ChatMessage> messages,
        String feature
) {
    public boolean isStream() {
        return Boolean.TRUE.equals(stream);
    }

    public String resolvedModel() {
        return (model == null || model.isBlank() || "auto".equals(model)) ? "auto" : model;
    }

    /**
     * 客户端声明的流式选项。网关接收它是为了保证兼容客户端传参不报错，
     * 但**是否真的向上游索取 usage 由服务端配置决定**（provider.stream-include-usage）——
     * 能否支持这个参数取决于上游实现，不该让客户端来试探。
     */
    public record StreamOptions(@JsonProperty("include_usage") Boolean includeUsage) {
    }
}
