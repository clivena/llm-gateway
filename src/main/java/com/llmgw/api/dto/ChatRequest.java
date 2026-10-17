package com.llmgw.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Locale;

/**
 * OpenAI 兼容的对话请求。
 *
 * 命名约定：对外用 OpenAI 的 snake_case，Java 侧保持 camelCase，靠 @JsonProperty 桥接。
 * 这个桥接不能省 —— Spring Boot 的 Jackson 默认不做 snake_case 转换，
 * 少了注解这些参数会被**静默丢弃**：客户端传了、网关收了、但没有转发给上游，
 * 表现为"参数不生效"，且没有任何报错可查。
 *
 * 采样参数（max_tokens / top_p / stop / seed）只做透传，网关不解释它们；
 * task 与 feature 都是本网关的私有扩展，不属于 OpenAI 协议 —— 但两者的语义**正交**：
 *
 *   task     = 这次请求需要什么能力（code / extract-json / summarize / chat），
 *              网关据此选模型。由调用方声明优先，没声明时由网关按规则推断。
 *   feature  = 这条流量属于哪条业务线（如 resume-tailor），只用于成本归因。
 *
 * 一个业务线里会有多种任务（同一条业务线既可能摘要也可能写代码），
 * 所以**不能**用 feature 顶替 task：那会让"按业务线分组看成本"和
 * "按任务选模型"两件事互相绑架，且已经落进 cost_ledger 的历史数据无法解释。
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
        String feature,
        String task
) {
    public boolean isStream() {
        return Boolean.TRUE.equals(stream);
    }

    public String resolvedModel() {
        return (model == null || model.isBlank() || "auto".equals(model)) ? "auto" : model;
    }

    /**
     * 归一化后的任务名：null / 空串 / 纯空白都视为"没声明"，返回 null。
     *
     * 三个归一化动作缺一不可，否则配置里写 {@code code}、客户端传 {@code CODE}
     * 或 {@code " code "} 就会被当成**未知任务** —— 而未知任务是要返回 400 的。
     * 用户眼里他明明写对了，这类失败排查成本最高。
     *
     * 用 {@link Locale#ROOT} 而不是默认 locale：土耳其语环境下
     * {@code "I".toLowerCase()} 会得到 {@code "ı"}，任务名匹配会莫名其妙失败。
     */
    public String resolvedTask() {
        if (task == null || task.isBlank()) {
            return null;
        }
        return task.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * 客户端声明的流式选项。网关接收它是为了保证兼容客户端传参不报错，
     * 但**是否真的向上游索取 usage 由服务端配置决定**（provider.stream-include-usage）——
     * 能否支持这个参数取决于上游实现，不该让客户端来试探。
     */
    public record StreamOptions(@JsonProperty("include_usage") Boolean includeUsage) {
    }
}
