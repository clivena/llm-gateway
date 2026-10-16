package com.llmgw.core.protocol;

import com.llmgw.api.dto.Usage;

/**
 * 流式分片。
 *
 * 为什么不直接用裸 String（F4 / F5 合并改造）：
 *   流式路径上"这次到底是谁在服务"和"这次真实花了多少 token"只有上游知道，
 *   而且往往只在最后一帧出现。旧契约只吐文本，直接导致两个后果：
 *     ① 响应分片里的 model 只能填**请求的**模型名 —— 级联升级后就是错的，
 *        客户端看到与实际不符的模型，账本归因也跟着错；
 *     ② 账本只能按字符数估算 token，成本在流式场景失真 —— 而流式恰恰是主战场。
 *   把这两个值随分片一起带出来，两条问题一次解决，不用各自再打补丁。
 *
 * @param model        实际服务本次请求的模型名（不是请求里写的那个）
 * @param delta        文本增量；用量帧与收尾帧为 null
 * @param usage        上游回传的真实用量；仅在有值时有意义
 * @param finishReason 上游回传的 finish_reason；仅**收尾帧**有值（见 {@link #finish}）
 */
public record StreamChunk(String model, String delta, Usage usage, String finishReason) {

    public static StreamChunk delta(String model, String text) {
        return new StreamChunk(model, text, null, null);
    }

    /** 用量帧：通常是 choices 为空、usage 非空的收尾帧。网关用它记账，不转发给客户端。 */
    public static StreamChunk usage(String model, Usage usage) {
        return new StreamChunk(model, null, usage, null);
    }

    /**
     * 收尾帧：上游在流末尾发的那个"带 finish_reason、delta 为空"的分片。
     *
     * <p>F0 之前这类分片被当成"没有 content 的分片"直接丢掉，于是流式路径
     * 同样存在与非流式一样的盲区：客户端看到 {@code [DONE]}，却不知道这次是
     * 正常结束还是被 {@code length} 截断。**这个帧不能丢。**
     */
    public static StreamChunk finish(String model, String finishReason) {
        return new StreamChunk(model, null, null, finishReason);
    }

    public boolean hasDelta() {
        return delta != null && !delta.isEmpty();
    }

    /** 是否是收尾帧（带 finish_reason）。 */
    public boolean isFinish() {
        return finishReason != null && !finishReason.isBlank();
    }
}
