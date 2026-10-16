package com.llmgw.core.resilience;

import com.llmgw.api.dto.ChatResponse;

/**
 * 上游"回了一个响应，但里面没有可用的答案"（F0 的 {@code empty} 判定）。
 *
 * <h3>为什么要把"空正文"伪装成一个异常</h3>
 * Reactor 的重试算子（{@code retryWhen} / resilience4j 的 {@code RetryOperator}）
 * <b>只认错误信号</b>。而"HTTP 200 + 空正文 + finish_reason=length"在传输层
 * 是一次彻头彻尾的成功 —— 它压根不会变成 onError。所以想让它参与重试，
 * 就必须在进重试边界之前把它**翻译成异常**，出边界之后再翻译回响应。
 *
 * <p>这个来回翻译是本类存在的全部理由，也是它必须携带原始响应对象的原因：
 * 重试耗尽后要能原样交出上游那次真实响应（见 {@link #response()}），
 * 而不是把一个异常甩给客户端。<b>否则就破坏了 F0 刚建立的
 * "finish_reason 必须原样透传"</b>：客户端会从"收到 200 + finish_reason=length
 * + 空正文"退化成"收到 502"，反而更看不出发生了什么。
 *
 * <h3>为什么不直接抛 RuntimeException</h3>
 * 继承 {@link IllegalStateException} 是刻意的：万一这个异常真的逃到了最外层
 * （说明某个分支漏了兜底），{@code OpenAiProtocol.statusOf} 会把它映射成
 * <b>502 Bad Gateway</b> + {@code upstream_error} —— 语义正确。
 * 若继承 RuntimeException，同样的泄漏会变成 500（看起来像网关自己的 bug，
 * 而实际是上游的问题），把排查方向带偏。
 */
public class UpstreamEmptyResponseException extends IllegalStateException {

    /** 上游那次真实响应。transient：异常可能被序列化，而 ChatResponse 不需要跟着走。 */
    private final transient ChatResponse response;

    public UpstreamEmptyResponseException(ChatResponse response) {
        super("upstream returned an empty response [model=" + response.model()
                + " finish_reason=" + response.finishReason() + "]");
        this.response = response;
    }

    /** 重试耗尽后要交还给上层的原始响应。 */
    public ChatResponse response() {
        return response;
    }
}
