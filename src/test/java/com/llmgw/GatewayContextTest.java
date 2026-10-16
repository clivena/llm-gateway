package com.llmgw;

import com.llmgw.api.ChatController;
import com.llmgw.api.OpenAiCompatController;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.LayeredCacheService;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.pipeline.ChatPipeline;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.router.RoutingPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用**出厂默认配置**把整个 Spring 上下文拉起来一次。
 *
 * 为什么需要这个测试（而不是"反正手动启动过就没问题"）：
 *   本轮改动引入了两条**启动期校验** —— 候选表不能为空、每个候选都必须被某个
 *   provider 提供。它们是好东西，但代价是"改配置"从此可以让应用**起不来**。
 *   而配置恰恰是最容易被单测漏掉的一层：ProviderRegistryTest / RouterServiceTest
 *   都是自己造配置对象，永远测不到 src/main/resources/application.yml 里那份真实配置。
 *   真出现"candidates 与 providers 对不上"时，只有真启动一次才会暴露 ——
 *   本测试把这个动作变成 CI 里的一条断言。
 *
 * 不启动 Web 服务器（webEnvironment=NONE）、不连任何外部服务：
 *   Redis 的 Lua 脚本只是读 classpath 资源；R2DBC 连接池与 KafkaTemplate
 *   都是惰性连接；上游 WebClient 只在真正发请求时才出网。
 *   所以这个测试是纯本地的。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class GatewayContextTest {

    @Autowired
    private GatewayProperties properties;

    @Autowired
    private ProviderRegistry providers;

    @Autowired
    private RouterService router;

    @Autowired
    private ChatPipeline pipeline;

    @Autowired
    private ChatController chatController;

    @Autowired
    private OpenAiCompatController compatController;

    @Autowired
    private SemanticCacheService cache;

    @Test
    void contextLoadsWithShippedDefaultConfig() {
        assertNotNull(pipeline, "ChatPipeline 未装配");
        assertNotNull(chatController, "ChatController 未装配");
        assertNotNull(compatController, "OpenAiCompatController 未装配");
        assertNotNull(router, "RouterService 未装配");
    }

    /**
     * 回归点（P2）：适配器曾经是单例 @Component + findFirst()，
     * 配置里加第二家 provider 在运行时完全不生效、也不报错。
     * 现在每家 provider 必须各有一个适配器。
     */
    @Test
    void oneAdapterPerConfiguredProvider() {
        assertEquals(properties.getProviders().size(), providers.all().size(),
                "配置了几家 provider 就应该有几个适配器");

        for (GatewayProperties.Provider provider : properties.getProviders()) {
            for (String model : provider.getModels()) {
                assertTrue(providers.supports(model),
                        "provider " + provider.getName() + " 声明的模型 " + model + " 未进入索引");
            }
        }
    }

    /**
     * 回归点（P3）：候选表与实际部署的模型必须一致。
     * 旧实现把候选硬编码成 gpt-4o-mini，而 provider 只配了 deepseek，
     * model=auto 100% 返回 502 —— 只有真打请求才发现。
     */
    @Test
    void shippedCandidateTableIsNonEmptyAndFullyRoutable() {
        assertFalse(properties.getRouter().getCandidates().isEmpty(),
                "gateway.router.candidates 不能为空，否则 model=auto 无档位可选");

        for (GatewayProperties.Candidate candidate : properties.getRouter().getCandidates()) {
            assertTrue(providers.supports(candidate.getModel()),
                    "候选模型 " + candidate.getModel() + " 没有任何 provider 提供");
        }
    }

    /** pick() 真的产出一个可路由的模型 —— 覆盖"候选非空但 pick 结果没适配器"的形态。 */
    @Test
    void pickedModelIsAlwaysRoutable() {
        for (RoutingPolicy policy : RoutingPolicy.values()) {
            String picked = router.pick(policy).model();
            assertNotNull(providers.forModel(picked),
                    "策略 " + policy + " 选出的模型 " + picked + " 必须能路由到适配器");
        }
    }

    /**
     * 回归点（P4）：缓存层必须真的被装配进集合。
     *
     * 这是本轮真机验证踩出来的：缓存配置全开（exact=true）、Redis 连通、
     * 主链路一切正常，但 Redis 里一个 {@code ex:} 键都没有 —— **缓存从未写入**。
     * 原因就藏在 Spring 对集合注入的规则里：{@code List<T>} 没有候选 bean 时
     * 注入的是<b>空集合而不是报错</b>。于是门面的 lookup/write 全部退化成
     * 直接返回空 Mono，主链路毫无异常。
     *
     * 这条断言的价值在于把"静默失效"变成"启动即失败"：
     * 哪天某个层漏了 @Service、或被移出组件扫描范围，这个测试会立刻红。
     */
    @Test
    void cacheLayersAreWiredInPriorityOrder() {
        assertTrue(cache instanceof LayeredCacheService,
                "SemanticCacheService 的实现应当是门面 LayeredCacheService");

        List<String> names = ((LayeredCacheService) cache).layerNames();
        assertEquals(List.of("L1-exact", "L2-semantic"), names,
                "缓存层装配结果不对（顺序即优先级）：实际 " + names);
    }
}
