package com.llmgw.api.dto;

/**
 * 一次对话的**归一化结果**（内部领域对象，不是对外响应形状 ——
 * 对外形状由 {@code OpenAiProtocol} 构造）。
 *
 * @param id           上游给的响应 id；缓存命中时为 "cache"
 * @param model        实际服务的模型名（不是请求里写的那个）
 * @param content      正文
 * @param usage        用量
 * @param cacheHit     是否缓存命中（网关私有信息，出站时只能进响应头）
 * @param finishReason 上游原样返回的 finish_reason（stop / length / ...）。
 *                     必须透传给客户端：F0 之前 {@code OpenAiProtocol} 把它
 *                     硬编码成 "stop"，于是被截断的响应也被报成"正常结束"，
 *                     客户端没有任何办法分辨。为 null 表示上游没给。
 */
public record ChatResponse(
        String id,
        String model,
        String content,
        Usage usage,
        boolean cacheHit,
        String finishReason
) {

    /**
     * 不带 finish_reason 的便捷构造器。
     *
     * <p>刻意保留：网关**自己合成**的响应（缓存命中、流式快照）没有上游的
     * finish_reason 可透传。这些场景一律填 {@code "stop"} —— 语义上是对的：
     * "网关已经把一份完整答案交出去了"就是 stop。
     *
     * <p>这里写字面量而不是引用 {@code UpstreamOutcome.STOP}，是因为
     * {@code core.protocol} 已经依赖 {@code api.dto}（StreamChunk 引用 Usage），
     * 反向引用会形成包环。协议字面量在 DTO 这层是稳定的，不值得为它破环。
     */
    public ChatResponse(String id, String model, String content, Usage usage, boolean cacheHit) {
        this(id, model, content, usage, cacheHit, "stop");
    }
}
