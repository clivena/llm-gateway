package com.llmgw.core.router;

import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/**
 * 路由与级联决策。纯逻辑，便于单测。
 * TODO: 从 model_route 表或配置加载候选模型，支持按任务类型路由。
 */
@Service
public class RouterService {

    private final List<ModelTier> candidates = List.of(
            new ModelTier("gpt-4o-mini", "CHEAP", 0.15, 0.72),
            new ModelTier("gpt-4o", "STRONG", 2.50, 0.90)
    );

    public ModelTier pick(RoutingPolicy policy) {
        return switch (policy) {
            case COST_FIRST -> candidates.stream()
                    .min(Comparator.comparingDouble(ModelTier::costPer1k))
                    .orElseThrow();
            case QUALITY_FIRST -> candidates.stream()
                    .max(Comparator.comparingDouble(ModelTier::qualityScore))
                    .orElseThrow();
            case TASK_BASED -> candidates.get(0);
        };
    }

    /** 级联：小模型答完后判断是否需要升级。TODO: 接真实判据（格式校验 / judge 抽样）。 */
    public boolean shouldEscalate(ModelTier used, String answer, boolean formatValid) {
        return "CHEAP".equals(used.tier()) && !formatValid;
    }

    public ModelTier escalateFrom(ModelTier used) {
        return candidates.stream()
                .filter(c -> c.qualityScore() > used.qualityScore())
                .max(Comparator.comparingDouble(ModelTier::qualityScore))
                .orElse(used);
    }
}
