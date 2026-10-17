package com.llmgw.core.cost;

import com.llmgw.config.GatewayProperties;
import com.llmgw.observability.GatewayMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 账本落库时 {@code task} 的去向。
 *
 * <h3>为什么要专门测这一层</h3>
 * task 从前是被"顺手带过"的字段：加列、改 SQL、改 DTO 三处只要漏一处，
 * 表现都是<b>账照写、金额对、只是任务列恒为空</b>。而空值不会让任何断言失败，
 * 只会让一个月后的"code 任务成本涨了吗"这个问题无法回答。
 *
 * <p>所以这里不做端到端，只钉住两个交接点：
 * service 收到的 task 有没有原样交给仓储、有没有进 Kafka 事件。
 * 真正写进 Postgres 的那一步由真机脚本（{@code /admin/ledger/recent}）验。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CostLedgerServiceTest {

    @Mock private KafkaTemplate<String, String> kafka;
    @Mock private CostLedgerRepository repository;

    private final GatewayProperties props = new GatewayProperties();
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());
    private CostLedgerService service;

    @BeforeEach
    void setUp() {
        service = new CostLedgerService(kafka, repository, props, metrics);
        // feature / task 用 any() 而不是 anyString()：task 允许为 null，
        // 而 anyString() 不匹配 null —— 桩会静默不命中、返回 null，然后在 subscribe 处 NPE。
        when(repository.insert(anyString(), anyString(), any(), any(), anyString(),
                anyBoolean(), anyInt(), anyInt(), anyDouble(), anyLong()))
                .thenReturn(Mono.just(1L));

        CompletableFuture<SendResult<String, String>> sent =
                CompletableFuture.completedFuture(null);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(sent);
    }

    @Test
    void taskIsPassedToThePostgresRow() {
        props.getCost().setSink("postgres");

        service.record("trace-1", "tenant-a", "resume-builder", "code",
                "deepseek-flash", false, 41, 576, 0.0008845, 1200);

        verify(repository).insert("trace-1", "tenant-a", "resume-builder", "code",
                "deepseek-flash", false, 41, 576, 0.0008845, 1200L);
    }

    /**
     * 没有发生任务判定时必须是 null，不能是空串。
     *
     * <p>这一条的后果只在聚合查询里显形：空串会被 GROUP BY 变成一个
     * "名字叫空字符串的任务"，而 NULL 才是"未标注"。两者在 SQL 里都能跑，
     * 但看到报表的人会得出不同结论。
     */
    @Test
    void absentTaskStaysNullSoGroupingDoesNotInventABucket() {
        props.getCost().setSink("postgres");

        service.record("trace-2", "tenant-a", null, null,
                "deepseek-flash", false, 1, 1, 0.0, 10);

        verify(repository).insert(eq("trace-2"), eq("tenant-a"), eq(""), isNull(),
                eq("deepseek-flash"), eq(false), eq(1), eq(1), eq(0.0), eq(10L));
    }

    /** Kafka 事件是生产链路的主要消费者，字段少了等于下游永远看不到任务维度。 */
    @Test
    void taskIsIncludedInTheKafkaEvent() {
        props.getCost().setSink("kafka");
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);

        service.record("trace-3", "tenant-a", "resume-builder", "summarize",
                "deepseek-pro", false, 10, 20, 0.001, 300);

        verify(kafka).send(eq(CostLedgerService.TOPIC), eq("tenant-a"), payload.capture());
        assertTrue(payload.getValue().contains("\"task\":\"summarize\""),
                "事件里必须有 task，实际：" + payload.getValue());
    }
}
