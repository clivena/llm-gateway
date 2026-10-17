package com.llmgw.core.pipeline;

import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cache.SemanticCacheService;
import com.llmgw.core.cost.CostLedgerService;
import com.llmgw.core.protocol.ProviderRegistry;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.RouterService;
import com.llmgw.core.token.TokenEstimator;
import com.llmgw.observability.GatewayMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 计费口径：一次调用该记多少钱。
 *
 * <h3>为什么单独测这一层</h3>
 * 账本写入走 {@code recordAsync}（fire-and-forget + 独立线程），从测试里去"等它落下来"
 * 本身就是不可靠的；而且"算得对不对"和"写得进不进得去"是两件事 ——
 * 后者由真机脚本对着 Postgres 断言，前者必须在这里钉死。
 *
 * <h3>这一组守的是什么</h3>
 * 回退路径算错钱不会报任何错：主链路正常、账本正常写入、日志一片干净，
 * 只是每条记录少一点。它只会在月底对账时表现为"总比账单少一些"，
 * 而那时已经没有任何线索能追到具体的代码行。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ChatPipelineCostTest {

    private static final String CHEAP = "deepseek-flash";
    private static final String STRONG = "deepseek-v4-pro";

    @Mock private QuotaService quotaService;
    @Mock private SemanticCacheService cacheService;
    @Mock private RouterService routerService;
    @Mock private ProviderRegistry providers;
    @Mock private CostLedgerService ledger;
    @Mock private UpstreamResilience resilience;

    private final GatewayProperties props = new GatewayProperties();
    private ChatPipeline pipeline;

    @BeforeEach
    void setUp() {
        pipeline = new ChatPipeline(quotaService, cacheService, routerService, providers,
                ledger, new GatewayMetrics(new SimpleMeterRegistry()), props,
                new TokenEstimator(props), resilience);
    }

    private static ModelTier tier(String model, double costPer1k) {
        return new ModelTier(model, "CHEAP", costPer1k, 0.5);
    }

    private static GatewayProperties.Candidate candidate(String model, String tier, double costPer1k) {
        var c = new GatewayProperties.Candidate();
        c.setModel(model);
        c.setTier(tier);
        c.setCostPer1k(costPer1k);
        c.setQualityScore("STRONG".equals(tier) ? 0.9 : 0.72);
        return c;
    }

    /**
     * 配了单价就按输入/输出分开算。输出通常贵 3~8 倍，
     * 混成一个单价会系统性低估 —— 这条是计费的基本口径。
     */
    @Test
    void configuredPricingSplitsInputAndOutput() {
        var price = new GatewayProperties.Price();
        price.setInputPer1k(0.0005);
        price.setOutputPer1k(0.0015);
        props.getPricing().put(CHEAP, price);

        // 41/1000 × 0.0005 + 576/1000 × 0.0015 = 0.0000205 + 0.000864 = 0.0008845
        double cost = pipeline.estimateCost(CHEAP, tier(CHEAP, 0.001), new Usage(41, 576));
        assertEquals(0.0008845, cost, 1e-9);
    }

    /**
     * <b>回归：级联升级后的调用不能按原档位计价。</b>
     *
     * <p>升级之后 {@code model} 已经是强模型，而调用方手上的 {@code tier} 还是
     * 请求开始时选的便宜档。旧实现用 {@code tier.costPer1k()} 兜底，
     * 于是"因为便宜模型答不好所以换强模型"这条路径上，账本按便宜模型记账 ——
     * 恰好是最该算准的那类请求被算少了。
     *
     * <p>这里刻意<b>不配 pricing</b>，逼它走候选表兜底，才能暴露问题。
     */
    @Test
    void unconfiguredModelFallsBackToItsOwnTierNotTheCallersOne() {
        props.getRouter().setCandidates(List.of(
                candidate(CHEAP, "CHEAP", 0.0015),
                candidate(STRONG, "STRONG", 0.06)));

        double cost = pipeline.estimateCost(STRONG, tier(CHEAP, 0.0015), new Usage(20, 30));
        // 50 tokens × 0.06 / 1000 = 0.003 —— 用旧 tier 会算成 0.000075，差 40 倍
        assertEquals(0.003, cost, 1e-9,
                "必须按实际服务的模型查档位；用调用方的旧 tier 会把成本低估几十倍");
    }

    /**
     * 客户端显式指定了一个不在候选表里的模型：这时没有任何档位信息可用，
     * 只能退回调用方给的那个。这条路径存在是合理的（候选表是给 model=auto 用的），
     * 断言它是为了固定"兜底不是随便取一个默认值"这个语义。
     */
    @Test
    void modelOutsideCandidatesFallsBackToTheProvidedTier() {
        props.getRouter().setCandidates(List.of(candidate(CHEAP, "CHEAP", 0.0015)));

        double cost = pipeline.estimateCost("some-explicit-model", tier("some-explicit-model", 0.01),
                new Usage(100, 100));
        assertEquals(0.002, cost, 1e-9);
    }
}
