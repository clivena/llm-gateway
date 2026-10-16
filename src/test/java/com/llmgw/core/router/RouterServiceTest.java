package com.llmgw.core.router;

import com.llmgw.config.GatewayProperties;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.token.TokenEstimator;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 路由与级联判据。
 *
 * P3 之后 RouterService 依赖配置与 ProviderRegistry，所以这组测试同时覆盖
 * 两条**启动期一致性校验** —— 它们的价值在于把"配置写错"从运行时的 502
 * 前移成启动期的报错。最后一条测试就是老 bug 的形状。
 */
class RouterServiceTest {

    private static final String CHEAP = "cheap-model";
    private static final String STRONG = "strong-model";

    @Test
    void costFirstPicksCheapest() {
        assertEquals(CHEAP, router().pick(RoutingPolicy.COST_FIRST).model());
    }

    @Test
    void qualityFirstPicksStrongest() {
        assertEquals(STRONG, router().pick(RoutingPolicy.QUALITY_FIRST).model());
    }

    @Test
    void invalidFormatOnCheapModelEscalates() {
        RouterService router = router();
        ModelTier cheap = router.pick(RoutingPolicy.COST_FIRST);

        assertTrue(router.shouldEscalate(cheap, "bad", false));
        assertEquals(STRONG, router.escalateFrom(cheap).model());
    }

    @Test
    void validFormatDoesNotEscalate() {
        RouterService router = router();
        ModelTier cheap = router.pick(RoutingPolicy.COST_FIRST);

        assertFalse(router.shouldEscalate(cheap, "看起来正常的回答", true));
    }

    @Test
    void strongestModelHasNowhereToEscalate() {
        RouterService router = router();
        ModelTier strong = router.pick(RoutingPolicy.QUALITY_FIRST);

        assertFalse(router.shouldEscalate(strong, "bad", false),
                "已是最高档位，格式不合法也不该试图升级 —— 否则会重复调用同一个模型并重复计费");
        assertEquals(STRONG, router.escalateFrom(strong).model());
    }

    @Test
    void explicitModelNeverEscalates() {
        // 客户端显式点名了模型（ChatPipeline 给它 qualityScore=1.0），
        // 网关就不能"自作主张升级"：用户点的是便宜模型，静默换成贵的那就是
        // 未经同意的计费。这条不变量靠 qualityScore 表达，而不是靠 tier 字符串。
        RouterService router = router();
        ModelTier explicit = new ModelTier(CHEAP, "EXPLICIT", 0.0, 1.0);

        assertFalse(router.shouldEscalate(explicit, "bad", false),
                "显式指定的模型即使回答格式不合法，也不该升级");
    }

    // --- 启动期校验 ---

    @Test
    void emptyCandidatesFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> routerOf(List.of(), CHEAP, STRONG));

        assertTrue(e.getMessage().contains("candidates"), e.getMessage());
    }

    /**
     * 老 bug 的形状：候选表里有一个 provider 根本没提供的模型名。
     * 旧实现在运行时表现为 model=auto 必然 502，现在应该在启动时就拦住。
     */
    @Test
    void candidateNotServedByAnyProviderFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> routerOf(
                        List.of(candidate("gpt-4o-mini", "CHEAP", 0.0010, 0.70)),
                        CHEAP, STRONG));

        assertTrue(e.getMessage().contains("gpt-4o-mini"), e.getMessage());
    }

    // --- helpers ---

    private static RouterService router() {
        return routerOf(
                List.of(candidate(CHEAP, "CHEAP", 0.0015, 0.70),
                        candidate(STRONG, "STRONG", 0.0080, 0.90)),
                CHEAP, STRONG);
    }

    private static RouterService routerOf(List<GatewayProperties.Candidate> candidates, String... models) {
        GatewayProperties props = new GatewayProperties();
        props.getRouter().setPolicy("COST_FIRST");
        props.getRouter().setCandidates(candidates);
        props.setProviders(List.of(provider("test-provider", "https://x.example/v1", models)));

        return new RouterService(props,
                new ProviderRegistry(props, WebClient.builder(), new TokenEstimator(props)));
    }

    private static GatewayProperties.Candidate candidate(String model, String tier,
                                                         double costPer1k, double quality) {
        GatewayProperties.Candidate c = new GatewayProperties.Candidate();
        c.setModel(model);
        c.setTier(tier);
        c.setCostPer1k(costPer1k);
        c.setQualityScore(quality);
        return c;
    }

    private static GatewayProperties.Provider provider(String name, String baseUrl, String... models) {
        GatewayProperties.Provider p = new GatewayProperties.Provider();
        p.setName(name);
        p.setBaseUrl(baseUrl);
        p.setApiKey("test-key");
        p.setModels(List.of(models));
        return p;
    }
}
