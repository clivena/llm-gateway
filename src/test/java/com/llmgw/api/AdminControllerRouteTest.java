package com.llmgw.api;

import com.llmgw.config.GatewayProperties;
import com.llmgw.core.cost.CostLedgerRepository;
import com.llmgw.core.quota.QuotaService;
import com.llmgw.core.resilience.UpstreamResilience;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.RouterService;
import com.llmgw.observability.GatewayMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code /admin/route} 的响应形状。
 *
 * <h3>为什么这个端点值得测</h3>
 * 它是排障入口：人被叫起来看"为什么这批请求走了贵模型"，第一眼看的就是它。
 * 如果它返回的映射表与运行时用的不是同一份（比如从配置文件原样回显，
 * 而不是从 RouterService 反查候选表得到的真实档位），那么它会在最需要准确的
 * 时刻给出一个<b>看起来合理但错误</b>的答案 —— 比起不提供这个端点更糟。
 */
class AdminControllerRouteTest {

    @SuppressWarnings("unchecked")
    private final DatabaseClient db = mock(DatabaseClient.class);
    private final CostLedgerRepository ledger = mock(CostLedgerRepository.class);
    private final QuotaService quota = mock(QuotaService.class);
    private final UpstreamResilience resilience = mock(UpstreamResilience.class);
    private final RouterService router = mock(RouterService.class);
    private final GatewayProperties props = new GatewayProperties();
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());

    private AdminController controller() {
        return new AdminController(ledger, quota, props, db, resilience, router, metrics);
    }

    @Test
    void routePayloadContainsConfigRealTiersAndHitDistribution() {
        props.getRouter().setPolicy("COST_FIRST");
        props.getRouter().getTaskRouting().setEnabled(true);
        props.getRouter().getTaskRouting().setDefaultTask("chat");
        props.getRouter().getTaskRouting().setClassifier("rule");
        // 注意这里 mock 出来的是 RouterService 的真实输出（ModelTier 带档位与质量分），
        // 不是配置里的原始字符串 —— 端点必须暴露"实际生效的"而不是"配置里写的"。
        when(router.taskTable()).thenReturn(Map.of(
                "code", new ModelTier("deepseek-pro", "STRONG", 0.008, 0.9),
                "chat", new ModelTier("deepseek-flash", "CHEAP", 0.0015, 0.72)));

        metrics.onRoute("code", "inferred");
        metrics.onRoute("code", "inferred");
        metrics.onRoute("chat", "fallback");

        Map<String, Object> out = controller().route().block();

        assertEquals(true, out.get("enabled"));
        assertEquals("chat", out.get("defaultTask"));
        assertEquals("rule", out.get("classifier"));
        assertEquals("COST_FIRST", out.get("policy"));

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> tasks = (Map<String, Map<String, Object>>) out.get("tasks");
        assertEquals("deepseek-pro", tasks.get("code").get("model"));
        assertEquals("STRONG", tasks.get("code").get("tier"));
        assertEquals(0.9, tasks.get("code").get("qualityScore"));

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Long>> hits = (Map<String, Map<String, Long>>) out.get("hits");
        assertEquals(Map.of("inferred", 2L), hits.get("code"));
        assertEquals(Map.of("fallback", 1L), hits.get("chat"));

        // 口径必须写在响应里：拿到这份数据的人未必读过 README，
        // 而"累计还是当前"决定了他会不会拿它去做趋势判断。
        assertEquals("since-process-start", out.get("hitsScope"));
    }

    /** 任务路由关着的时候，端点也要能打开 —— 排障时"为什么没生效"同样要查。 */
    @Test
    void routePayloadIsStillReadableWhenTaskRoutingIsDisabled() {
        props.getRouter().getTaskRouting().setEnabled(false);
        when(router.taskTable()).thenReturn(Map.of());

        Map<String, Object> out = controller().route().block();

        assertFalse((Boolean) out.get("enabled"));
        assertTrue(((Map<?, ?>) out.get("tasks")).isEmpty());
        assertTrue(((Map<?, ?>) out.get("hits")).isEmpty());
    }
}
