package com.llmgw.core.resilience;

/**
 * 熔断器处于打开状态，这次请求<b>根本没有发出去</b>（F1）。
 *
 * <h3>为什么要自己包一层，而不是直接把 resilience4j 的
 * {@code CallNotPermittedException} 抛出去</h3>
 * <ol>
 *   <li><b>分层</b>：{@code OpenAiProtocol} 是协议层（状态码 + 错误体形状），
 *       不该为了认一个状态码去依赖某个具体韧性库。项目里已有的做法是
 *       协议层只认自家 {@code core} 包下的异常类型（如 QuotaExceededException），
 *       这里保持一致。</li>
 *   <li><b>可携带语义</b>：真正需要告诉客户端的信息是"哪个 provider 的链路
 *       被判定为不可用"，而 {@code CallNotPermittedException} 身上只有熔断器名字
 *       （它恰好等于 provider 名，但那是约定不是契约）。显式带上 provider
 *       比让调用方去解析字符串可靠。</li>
 * </ol>
 *
 * <h3>为什么它对应 503 而不是 502</h3>
 * 502 的含义是"我替你问了上游，上游答得不对"；503 是"我现在不打算替你去问"。
 * 熔断打开属于后者 —— 上游<b>可能</b>已经恢复了，只是我们按策略不去试。
 * 客户端对这两者的正确反应也不同：502 该重试（上游可能抽风），
 * 503 该退避并读 {@code Retry-After}（我们主动限流）。混成 502 会
 * 让客户端在网关已经熔断时继续猛打，把恢复期又拖长。
 */
public class UpstreamCircuitOpenException extends RuntimeException {

    private final String provider;

    public UpstreamCircuitOpenException(String provider, Throwable cause) {
        super("circuit breaker is open for provider " + provider
                + "; upstream call rejected", cause);
        this.provider = provider;
    }

    public String provider() {
        return provider;
    }
}
