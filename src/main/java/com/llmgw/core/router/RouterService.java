package com.llmgw.core.router;

import com.llmgw.config.GatewayProperties;
import com.llmgw.core.protocol.ProviderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/**
 * 路由与级联决策。纯逻辑，便于单测。
 *
 * P3 改动：候选模型表由硬编码常量改为配置项（gateway.router.candidates）。
 *
 *   旧实现把候选写死在字段初始化里（gpt-4o-mini / gpt-4o），而实际 provider 只配了
 *   deepseek —— 于是 model=auto 挑到的模型名没有任何适配器支持，100% 返回 502，
 *   而且只有真打一次请求才会发现。这是"配置与代码各说各话"的典型。
 *
 *   现在一致性由两道闸门守住：
 *     ① 候选表来自配置，模型名随部署环境一起走；
 *     ② 启动时校验每个候选都被某个 provider 支持，不一致直接启动失败 ——
 *        把运行时的 502 前移成启动期的报错。
 */
@Service
public class RouterService {

    private static final Logger log = LoggerFactory.getLogger(RouterService.class);

    private final List<ModelTier> candidates;

    public RouterService(GatewayProperties properties, ProviderRegistry providers) {
        this.candidates = properties.getRouter().getCandidates().stream()
                .map(c -> new ModelTier(c.getModel(), c.getTier(), c.getCostPer1k(), c.getQualityScore()))
                .toList();

        if (candidates.isEmpty()) {
            throw new IllegalStateException(
                    "gateway.router.candidates 为空：model=auto 无档位可选。"
                            + "请在配置里声明候选模型（模型名须与 providers[].models 一致）");
        }
        for (ModelTier tier : candidates) {
            if (!providers.supports(tier.model())) {
                throw new IllegalStateException("gateway.router.candidates 声明了模型 "
                        + tier.model() + "，但没有任何 provider 提供它 —— model=auto 会返回 502。"
                        + "请核对 gateway.providers[].models");
            }
        }
        log.info("路由候选表：{}", candidates.stream().map(ModelTier::model).toList());
    }

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

    /**
     * 级联判据：小模型答得不可用、且**还存在更强的档位**时升级。
     *
     * 这里刻意不按 tier 字符串（"CHEAP"）判断，而是问"还有没有 qualityScore 更高的候选"——
     * 这样三档（CHEAP / MEDIUM / STRONG）也能自然工作，不必回来改这段逻辑。
     * TODO: 接入真实判据（格式校验之外的 judge 抽样）。
     */
    public boolean shouldEscalate(ModelTier used, String answer, boolean formatValid) {
        if (formatValid) {
            return false;
        }
        return candidates.stream().anyMatch(c -> c.qualityScore() > used.qualityScore());
    }

    public ModelTier escalateFrom(ModelTier used) {
        return candidates.stream()
                .filter(c -> c.qualityScore() > used.qualityScore())
                .max(Comparator.comparingDouble(ModelTier::qualityScore))
                .orElse(used);
    }
}
