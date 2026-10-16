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
