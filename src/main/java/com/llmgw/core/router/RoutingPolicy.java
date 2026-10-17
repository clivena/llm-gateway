package com.llmgw.core.router;

/**
 * 全局兜底策略 —— <b>只在任务路由关闭时生效</b>。
 *
 * <p>曾被删除的 {@code TASK_BASED} 值得记一笔：它的实现是
 * {@code candidates.get(0)}，是个从没接上任务信息的占位，也没有任何测试。
 * 之所以删而不是补全：任务路由是"先判任务、再查映射表"的前置判定，
 * 不是候选表上的一种排序方式。把它做成 policy 的一个取值，就等于让
 * {@code router.policy} 和 {@code task-routing.enabled} 两个开关表达同一件事 ——
 * 四种组合里必有两种语义不明（policy 说按任务、开关说关闭，听谁的？）。
 *
 * <p>现在的规则只有一条：任务路由开着就查表（未命中落 default-task），
 * 关着才看 policy。开关只有一个。
 */
public enum RoutingPolicy {
    COST_FIRST,
    QUALITY_FIRST
}
