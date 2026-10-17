package com.llmgw.core.router;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.token.TokenEstimator;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 路由与级联判据。
 *
 * <p>P3 之后 RouterService 依赖配置与 ProviderRegistry，所以这组测试同时覆盖
 * 两条<b>启动期一致性校验</b> —— 它们的价值在于把"配置写错"从运行时的 502
 * 前移成启动期的报错。最后一条测试就是老 bug 的形状。
 *
 * <p>任务路由（按 task 选模型）加进来后，又多了三类必须守的"静默坏法"：
 * 任务指定的模型不在候选表（级联永不升级）、default-task 没声明（兜底断裂）、
 * 显式任务名拼错（静默走了别的模型）。它们都不报错，只能靠断言发现。
 */
class RouterServiceTest {

    private static final String CHEAP = "cheap-model";
    private static final String STRONG = "strong-model";

    // --- 原有策略 ---

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

    // --- 任务路由 ---

    @Test
    void taskMappingRoutesToConfiguredModel() {
        RouterService router = withTasks("chat", Map.of("code", STRONG, "chat", CHEAP));

        Route route = router.resolve(request("auto", "code", "帮我实现一个排序函数"));

        assertEquals(STRONG, route.tier().model());
        assertEquals("code", route.task());
        assertEquals(TaskDecision.EXPLICIT, route.source());
    }

    @Test
    void taskNamesAreNormalisedOnBothSides() {
        // 配置写 CODE、请求传 " code "：两边都必须归一化，否则显式任务会莫名其妙 400。
        RouterService router = withTasks("chat", Map.of("CODE", STRONG, "chat", CHEAP));

        Route route = router.resolve(request("auto", "  Code ", "任意文本"));

        assertEquals(STRONG, route.tier().model());
        assertEquals("code", route.task());
    }

    @Test
    void inferredTaskRoutesWithoutClientDeclaringIt() {
        RouterService router = withTasks("chat", Map.of("code", STRONG, "chat", CHEAP));

        Route route = router.resolve(request("auto", null, "```java\nint a = 1;\n```"));

        assertEquals(STRONG, route.tier().model());
        assertEquals(TaskDecision.INFERRED, route.source());
    }

    @Test
    void inferredTaskMissingFromTableFallsBackToDefault() {
        // 推断出 summarize，但表里只配了 chat —— 这是网关自己的判断，不该让请求失败。
        RouterService router = withTasks("chat", Map.of("chat", CHEAP));

        Route route = router.resolve(request("auto", null, "帮我总结一下这段话"));

        assertEquals(CHEAP, route.tier().model());
        assertEquals("chat", route.task(), "生效的是 default-task，指标也必须记这个");
        assertEquals(TaskDecision.FALLBACK, route.source());
    }

    @Test
    void unknownExplicitTaskIsRejectedWithAvailableTasks() {
        // 静默兜底是最坏的选择：调用方以为 code 生效了，实际走了 chat，
        // 只有翻账本才能发现。必须显式失败并把可用任务名给出来。
        RouterService router = withTasks("chat", Map.of("chat", CHEAP));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> router.resolve(request("auto", "cdoe", "随便")));

        assertTrue(e.getMessage().contains("cdoe"), e.getMessage());
        assertTrue(e.getMessage().contains("chat"), "错误信息要列出可用任务：" + e.getMessage());
    }

    @Test
    void explicitModelBypassesTaskRouting() {
        RouterService router = withTasks("chat", Map.of("code", STRONG, "chat", CHEAP));

        Route route = router.resolve(request(STRONG, "code", "```\nint a;\n```"));

        assertEquals(STRONG, route.tier().model());
        assertEquals("EXPLICIT", route.tier().tier());
        assertEquals(Route.EXPLICIT_MODEL, route.source(),
                "显式模型必须最优先：网关不能因为任务不匹配就替换用户点名的模型");
    }

    @Test
    void disabledTaskRoutingUsesPolicy() {
        // 默认配置（enabled=false）：行为必须与引入任务路由之前完全一致。
        Route route = router().resolve(request("auto", "code", "```\nint a;\n```"));

        assertEquals(CHEAP, route.tier().model());
        assertEquals("policy", route.source());
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

    @Test
    void taskModelOutsideCandidatesFailsAtStartup() {
        // 不在候选表里 = 没有 qualityScore = 级联永远找不到更强档位。
        // 这个坏法运行时完全不报错，只会"怎么升级都升不上去"。
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> build(taskProps("chat", Map.of("chat", "ghost-model")), CHEAP, STRONG));

        assertTrue(e.getMessage().contains("ghost-model"), e.getMessage());
        assertTrue(e.getMessage().contains("candidates"), e.getMessage());
    }

    @Test
    void undeclaredDefaultTaskFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> build(taskProps("chat", Map.of("code", STRONG)), CHEAP, STRONG));

        assertTrue(e.getMessage().contains("default-task"), e.getMessage());
    }

    @Test
    void enabledButEmptyTableFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> build(taskProps("chat", Map.of()), CHEAP, STRONG));

        assertTrue(e.getMessage().contains("tasks 为空"), e.getMessage());
    }

    @Test
    void removedTaskBasedPolicyFailsLoudly() {
        // 删掉 TASK_BASED 之后，老配置必须得到一条说清缘由的启动错误，
        // 而不是一个看不出所以然的 IllegalArgumentException。
        GatewayProperties props = baseProps();
        props.getRouter().setPolicy("TASK_BASED");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> build(props, CHEAP, STRONG));

        assertTrue(e.getMessage().contains("TASK_BASED"), e.getMessage());
    }

    @Test
    void unsupportedClassifierFailsAtStartup() {
        // 配置里写 classifier=model 的人会以为"接了模型分类"。
        // 不校验的话它会静默走规则，行为与预期差一个数量级。
        GatewayProperties props = taskProps("chat", Map.of("chat", CHEAP));
        props.getRouter().getTaskRouting().setClassifier("model");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> build(props, CHEAP, STRONG));

        assertTrue(e.getMessage().contains("classifier"), e.getMessage());
    }

    // --- helpers ---

    private static RouterService router() {
        return routerOf(
                List.of(candidate(CHEAP, "CHEAP", 0.0015, 0.70),
                        candidate(STRONG, "STRONG", 0.0080, 0.90)),
                CHEAP, STRONG);
    }

    private static RouterService withTasks(String defaultTask, Map<String, String> mapping) {
        return build(taskProps(defaultTask, mapping), CHEAP, STRONG);
    }

    private static RouterService routerOf(List<GatewayProperties.Candidate> candidates, String... models) {
        GatewayProperties props = baseProps();
        props.getRouter().setCandidates(candidates);
        return build(props, models);
    }

    private static RouterService build(GatewayProperties props, String... models) {
        props.setProviders(List.of(provider("test-provider", "https://x.example/v1", models)));
        TokenEstimator estimator = new TokenEstimator(props);
        return new RouterService(props,
                new ProviderRegistry(props, WebClient.builder(), estimator),
                new TaskClassifier(estimator, props));
    }

    private static GatewayProperties baseProps() {
        GatewayProperties props = new GatewayProperties();
        props.getRouter().setPolicy("COST_FIRST");
        props.getRouter().setCandidates(List.of(
                candidate(CHEAP, "CHEAP", 0.0015, 0.70),
                candidate(STRONG, "STRONG", 0.0080, 0.90)));
        return props;
    }

    private static GatewayProperties taskProps(String defaultTask, Map<String, String> mapping) {
        GatewayProperties props = baseProps();
        GatewayProperties.TaskRouting routing = props.getRouter().getTaskRouting();
        routing.setEnabled(true);
        routing.setDefaultTask(defaultTask);

        Map<String, GatewayProperties.TaskRule> rules = new LinkedHashMap<>();
        mapping.forEach((task, model) -> {
            GatewayProperties.TaskRule rule = new GatewayProperties.TaskRule();
            rule.setModel(model);
            rules.put(task, rule);
        });
        routing.setTasks(rules);
        return props;
    }

    private static ChatRequest request(String model, String task, String userText) {
        return new ChatRequest(model, false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", userText)), "test", task);
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
