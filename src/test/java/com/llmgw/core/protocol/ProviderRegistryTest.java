package com.llmgw.core.protocol;

import com.llmgw.config.GatewayProperties;
import com.llmgw.core.token.TokenEstimator;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多 provider 装配的回归测试（P2）。
 *
 * 为什么这组测试必须存在：旧实现把适配器做成单例 @Component + findFirst()，
 * 只认第一个名为 openai 的 provider —— 配置里加第二家在运行时**完全不生效、
 * 也不报错**。这类静默失效无法靠真机联调发现（除非流量恰好打到第二家），
 * 只能靠装配层的断言守住。
 *
 * 本测试不启动 Spring 容器、不发起任何网络请求：ProviderRegistry 构造时只读配置、
 * 只创建 WebClient 对象（不连接），所以是纯 JUnit 测试，毫秒级。
 */
class ProviderRegistryTest {

    @Test
    void createsOneAdapterPerConfiguredProvider() {
        ProviderRegistry registry = registryOf(
                provider("deepseek", "https://api.deepseek.com/v1", "deepseek-flash"),
                provider("zhipu", "https://open.bigmodel.cn/api/paas/v4", "glm-4-flash"));

        assertEquals(2, registry.all().size(), "配置了几家就应该有几个适配器");
        assertTrue(registry.supports("deepseek-flash"));
        assertTrue(registry.supports("glm-4-flash"));
    }

    @Test
    void routesEachModelToItsDeclaringProvider() {
        ProviderRegistry registry = registryOf(
                provider("deepseek", "https://api.deepseek.com/v1", "deepseek-flash"),
                provider("zhipu", "https://open.bigmodel.cn/api/paas/v4", "glm-4-flash"));

        assertEquals("deepseek", registry.forModel("deepseek-flash").name());
        assertEquals("zhipu", registry.forModel("glm-4-flash").name());
    }

    @Test
    void oneProviderMayDeclareSeveralModels() {
        ProviderRegistry registry = registryOf(
                provider("deepseek", "https://api.deepseek.com/v1",
                        "deepseek-flash", "deepseek-v4-pro"));

        assertEquals(1, registry.all().size(), "一家 provider 只应产生一个适配器");
        assertTrue(registry.supports("deepseek-flash"));
        assertTrue(registry.supports("deepseek-v4-pro"));
    }

    /**
     * 同名模型出现在两家时，先声明者胜 —— 顺序即配置文件顺序，是显式可读的；
     * 若改成"依赖 Spring bean 加载顺序"，路由结果就变成不可预测的。
     */
    @Test
    void duplicateModelKeepsFirstDeclaration() {
        ProviderRegistry registry = registryOf(
                provider("primary", "https://a.example/v1", "shared-model"),
                provider("secondary", "https://b.example/v1", "shared-model"));

        assertEquals("primary", registry.forModel("shared-model").name());
    }

    // --- F2：同模型的其余声明不再是"被忽略的配置"，而是候选链 ---

    /**
     * 这是 F2 的数据结构前提：重复声明必须被<b>保留</b>成候选，顺序 = 声明顺序。
     *
     * <p>改之前（putIfAbsent + warn 忽略），这个断言写不出来 ——
     * 第二家 provider 在内存里根本不存在。那种失败特别隐蔽：
     * 配置写对了、启动日志也打了、请求也不报错，只是第二家永远不会被用到。
     * 故障转移写完了却"永远不生效"，第一嫌疑人就是这里。
     */
    @Test
    void duplicateModelFormsAFailoverChainInDeclarationOrder() {
        ProviderRegistry registry = registryOf(
                provider("primary", "https://a.example/v1", "shared-model"),
                provider("secondary", "https://b.example/v1", "shared-model"),
                provider("tertiary", "https://c.example/v1", "shared-model"));

        List<String> chain = registry.candidatesFor("shared-model").stream()
                .map(ProviderAdapter::name).toList();
        assertEquals(List.of("primary", "secondary", "tertiary"), chain,
                "候选链的顺序必须等于配置里的声明顺序 —— 谁优先必须是显式可读的");
        assertEquals("primary", registry.forModel("shared-model").name(),
                "forModel 仍然只回答'默认那一家'");
    }

    /**
     * 同一家 provider 在自己的 models 里把同一个模型名写两遍 → 去重。
     *
     * <p>不去重的话候选链里会出现两个一模一样的候选：故障转移会"换到同一家"再打一次，
     * 白烧一次调用，而且日志看起来像完成了一次正常的故障转移。
     */
    @Test
    void sameModelDeclaredTwiceByOneProviderIsDeduplicated() {
        ProviderRegistry registry = registryOf(
                provider("deepseek", "https://api.deepseek.com/v1",
                        "deepseek-flash", "deepseek-flash"));

        assertEquals(1, registry.candidatesFor("deepseek-flash").size());
    }

    /** 单一部署的模型，候选链就只有一家 —— 故障转移在这种模型上不会生效（也不该生效）。 */
    @Test
    void singleDeploymentYieldsASingleCandidateChain() {
        ProviderRegistry registry = registryOf(
                provider("deepseek", "https://api.deepseek.com/v1", "deepseek-flash"));

        assertEquals(List.of("deepseek"),
                registry.candidatesFor("deepseek-flash").stream().map(ProviderAdapter::name).toList());
    }

    @Test
    void unknownModelFailsWithRegistryContentInMessage() {
        ProviderRegistry registry = registryOf(
                provider("deepseek", "https://api.deepseek.com/v1", "deepseek-flash"));

        // 必须是 IllegalArgumentException 而不是 IllegalStateException：
        // 它是入口层把响应码映射成 400 而不是 502 的唯一依据（见 OpenAiProtocol.statusOf）。
        // 报成 502 会让客户端把"模型名写错了"当成网关故障去退避重试。
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> registry.forModel("gpt-4o-mini"));
        // 报错必须带上已注册清单：排查体验完全取决于这句话有没有把"实际有什么"说出来
        assertTrue(e.getMessage().contains("deepseek-flash"), e.getMessage());
    }

    // --- 启动期校验：配置错误应该在启动时炸，而不是变成运行时的 502 ---

    @Test
    void missingBaseUrlFailsAtStartup() {
        GatewayProperties.Provider broken = provider("broken", "", "some-model");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> registryOf(broken));
        assertTrue(e.getMessage().contains("base-url"), e.getMessage());
    }

    @Test
    void providerWithoutModelsFailsAtStartup() {
        GatewayProperties.Provider broken = provider("broken", "https://x.example/v1");

        assertThrows(IllegalStateException.class, () -> registryOf(broken));
    }

    @Test
    void providerWithoutNameFailsAtStartup() {
        GatewayProperties.Provider broken = provider("", "https://x.example/v1", "m");

        assertThrows(IllegalStateException.class, () -> registryOf(broken));
    }

    // --- helpers ---

    private static ProviderRegistry registryOf(GatewayProperties.Provider... providers) {
        GatewayProperties props = new GatewayProperties();
        props.setProviders(List.of(providers));
        // 适配器需要 TokenEstimator 才能构造（估算口径统一由它提供）。
        // 这里用 props 自带默认系数即可 —— 本测试只关心装配，不关心估算精度。
        return new ProviderRegistry(props, WebClient.builder(), new TokenEstimator(props));
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
