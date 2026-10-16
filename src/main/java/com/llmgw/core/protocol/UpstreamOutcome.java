package com.llmgw.core.protocol;

/**
 * 上游响应的**可用性判定**。
 *
 * <h3>为什么需要这个类型</h3>
 * 在这之前，"上游返回了一个响应"就等于"这次请求成功了"。这个等式在推理型模型上
 * 直接失效 —— 推理模型的 {@code max_tokens} 是**"推理 + 正文"的总预算**，
 * 预算被 reasoning 吃光时会返回 {@code finish_reason=length} + {@code content=""}。
 * 网关花了钱、拿到了 HTTP 200、还把空答案当成功返回给客户端，
 * 而客户端**无从分辨**"答案就是空的"和"被截断了"。
 *
 * 更糟的是 {@code OpenAiProtocol} 过去把 {@code finish_reason} 硬编码成 {@code "stop"} ——
 * 那不只是"没透传"，而是**主动告诉客户端"正常结束了"**。
 *
 * <h3>为什么把判定放在这里而不是散在管线里</h3>
 * 这个判定有两个消费者，且它们的口径必须一致：
 * <ul>
 *   <li><b>观测</b>：空响应与截断要分别计数，否则"上游开始抽风"和
 *       "有人在用极小的 max_tokens"混成一个数。</li>
 *   <li><b>重试与故障转移</b>（F1/F2）：需要一个明确的失败谓词来决定要不要换一家重试。
 *       谓词写两遍就会出现"指标认为失败、重试认为成功"这种最难查的不一致。</li>
 * </ul>
 *
 * @param empty        正文为空（空白也算）。空正文的回答**永远不可用** —— 客户端拿不到任何东西。
 * @param truncated    上游的 {@code finish_reason == "length"}，即因预算耗尽被截断。
 * @param finishReason 上游原样返回的 finish_reason，可为 null（上游没给）
 */
public record UpstreamOutcome(boolean empty, boolean truncated, String finishReason) {

    public static final String STOP = "stop";
    public static final String LENGTH = "length";

    public static UpstreamOutcome of(String content, String finishReason) {
        return new UpstreamOutcome(
                content == null || content.isBlank(),
                LENGTH.equals(finishReason),
                finishReason);
    }

    /**
     * 有正文、但被预算截断 —— 这是**客户端的 max_tokens 设小了**，不是上游故障。
     *
     * <p>刻意与 {@link #empty()} 分开：这一类的答案本身是可信的部分内容，
     * 值得原样返回；而"重试一次"对它毫无意义（预算不变就必然再截断一次）。
     * 反过来，{@code empty} 是**值得重试**的 —— 推理 token 数实测高度不确定
     * （同一问题在 512/1024/1935/1740 之间跳），换一次就可能正常出正文。
     */
    public boolean budgetTooSmall() {
        return truncated && !empty;
    }

    /** 给人看的短描述，直接进日志与指标标签旁。 */
    public String describe() {
        if (empty && truncated) {
            return "空正文且被截断（推理型模型预算耗尽）";
        }
        if (empty) {
            return "空正文";
        }
        if (budgetTooSmall()) {
            return "有正文但被截断（客户端 max_tokens 偏小）";
        }
        return "正常";
    }
}
