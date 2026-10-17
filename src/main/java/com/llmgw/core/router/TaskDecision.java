package com.llmgw.core.router;

/**
 * 一次"任务判定"的结果。
 *
 * <p>注意它只是**候选任务**，不一定能真正生效：推断出 {@code extract-json}
 * 而映射表里没配这个任务时，RouterService 会把它落到 default-task，
 * 最终生效的任务以 {@link Route#task()} 为准。区分这两层是有意的 ——
 * 否则指标里会统计出一堆"用过但根本没生效"的任务名。
 *
 * @param task   判定出的任务名（已归一化）
 * @param source 判定来源，见下面的常量；用于指标与日志
 * @param reason 具体命中哪条规则，排错时第一个要看的东西
 */
public record TaskDecision(String task, String source, String reason) {

    /** 调用方显式声明。未知任务名要返回 400，不能静默兜底。 */
    public static final String EXPLICIT = "explicit";

    /** 网关按规则推断。推断失败只影响优化效果，不该影响请求成败。 */
    public static final String INFERRED = "inferred";

    /** 没判出来或判出来的任务表里没有，落 default-task。 */
    public static final String FALLBACK = "fallback";

    public static TaskDecision explicit(String task) {
        return new TaskDecision(task, EXPLICIT, "client-declared");
    }

    public static TaskDecision inferred(String task, String reason) {
        return new TaskDecision(task, INFERRED, reason);
    }

    public static TaskDecision fallback(String defaultTask, String reason) {
        return new TaskDecision(defaultTask, FALLBACK, reason);
    }

    public boolean explicit() {
        return EXPLICIT.equals(source);
    }
}
