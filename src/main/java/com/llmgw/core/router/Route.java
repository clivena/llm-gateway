package com.llmgw.core.router;

/**
 * 最终生效的路由结果：用哪个模型、因为什么。
 *
 * <p>这是 {@code resolveRoute} 的返回值，也是编排层唯一需要知道的东西。
 * 下游（候选链、重试熔断、级联升级、配额结算）全都只看 {@link #tier()} ——
 * 任务路由到这里就结束了，它不参与"怎么调这个模型"。
 *
 * @param tier   最终选中的档位
 * @param task   实际生效的任务名（可能与判定结果不同，见 TaskDecision 的说明）
 * @param source 最终来源：explicit-model / explicit / inferred / fallback
 * @param reason 诊断信息，写进日志
 */
public record Route(ModelTier tier, String task, String source, String reason) {

    /** 调用方直接点名了模型。任务判定整个跳过，质量分 1.0，永不升级。 */
    public static final String EXPLICIT_MODEL = "explicit-model";
}
