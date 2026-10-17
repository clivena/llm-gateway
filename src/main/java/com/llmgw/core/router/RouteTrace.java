package com.llmgw.core.router;

/**
 * 一次请求的路由回执：把"管线内部算出什么了"交给调用方（HTTP 层），
 * 而<b>不把路由结果塞进响应体</b>。
 *
 * <h3>为什么需要这个类（而不是给 ChatResponse 加两个字段）</h3>
 * {@code ChatResponse} 是 record，全仓有二十多处构造点（含大量测试）。
 * 为了多两个只在 HTTP 边界上有意义的字段去改它，等于让"协议形状"
 * 被"运维需求"牵着走 —— 而任务的<b>来源</b>（显式/推断/兜底）根本不属于
 * OpenAI 兼容协议，它是网关自己的诊断信息。
 *
 * <h3>为什么不做成 exchange attribute</h3>
 * {@code core} 层不能依赖 {@code ServerWebExchange}。一旦核心管线认识 HTTP，
 * "换一个接入协议（gRPC / 消息队列）"就要重写判断逻辑，而这正是
 * {@code dispatch} 要避免的那类耦合。这里只传一个极小的对象，方向仍是
 * 外层 → 内层，内层不知道它最终会被写到响应头上。
 *
 * <h3>为什么是可变对象而不是返回值</h3>
 * 因为路由要在管线<b>内部</b>才算得出来（它依赖任务判定与配置表），
 * 而响应头必须在管线<b>返回之前</b>就设置好（首次写出后头部就冻结了）。
 * 一个可写的回执是这两件事之间最短的桥。
 *
 * <p>线程安全：写发生在请求线程（路由解析是同步的纯函数），读可能发生在
 * Netty 事件循环上，所以字段用 volatile。它只被写一次，不是共享可变状态。
 */
public final class RouteTrace {

    private volatile Route route;

    /** 记录本次请求的路由结果。重复调用以最后一次为准（当前每条请求只解析一次）。 */
    public void record(Route route) {
        this.route = route;
    }

    /** 尚未解析（或解析失败）时返回 null —— 调用方据此决定"不设这个头"。 */
    public Route get() {
        return route;
    }

    /**
     * 一个不关心路由结果的回执。
     *
     * <p>给单元测试与内部调用用：让"只想跑通管线"的地方不必为了一个新参数
     * 改二十多处调用点。生产路径上控制器永远传一个真实实例。
     */
    public static RouteTrace ignored() {
        return new RouteTrace();
    }
}
