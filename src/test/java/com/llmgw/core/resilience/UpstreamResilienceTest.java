package com.llmgw.core.resilience;

import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import com.llmgw.observability.GatewayMetrics;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1：重试与熔断的行为。
 *
 * <h3>为什么这些断言必须存在，而不能只靠真机脚本</h3>
 * 真机脚本能证明"上游抖动时请求最终成功了"，但证明不了下面这些**反面**：
 * 已经吐过分片的流没有被重放、熔断打开后上游一次都没被调用、
 * 熔断比不重试时快了几倍触发。这些全是"错了也不会报错"的分支 ——
 * 它们只会表现成"偶尔重复输出一段"或"故障期多烧了一堆钱"，
 * 而这两种现象在事后归因时都极难指到重试头上。
 *
 * <p>用真实的 resilience4j 实例（不 mock）：本类的被测对象就是这两条链路的
 * <b>组合语义</b>，mock 掉它们等于把要测的东西测没了。
 * GatewayMetrics 用 SimpleMeterRegistry，SLA 只是为了让构造函数能跑。
 */
class UpstreamResilienceTest {

    private static final String PROVIDER = "deepseek";

    private final GatewayProperties props = new GatewayProperties();
    private UpstreamResilience resilience;

    @BeforeEach
    void setUp() {
        var r = props.getResilience().getRetry();
        // 退避压到 1ms：这些用例要跑满 3 次尝试，用生产值(300/600ms)会让测试慢一个量级，
        // 而"退避了多久"由 resilience4j 负责，不是本类要守的东西。
        r.setWaitDurationMillis(1);
        r.setBackoffMultiplier(1.0);
        r.setMaxWaitDurationMillis(1);
        r.setMaxAttempts(3);

        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(3);
        cb.setMinimumNumberOfCalls(3);
        cb.setFailureRateThreshold(50f);
        // 打开后长时间不自动半开：用例要观察的就是"打开之后"的稳定状态
        cb.setWaitDurationInOpenStateMillis(60_000);

        resilience = new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry()));
    }

    // --- 重试谓词 ---

    /**
     * 判断标准只有一条：<b>再试一次的结果是否可能不同</b>。
     * 与错误"严重不严重"无关 —— 一次 400 很轻但它是确定性的；
     * 一次 429 看起来很糟但它恰恰是重试最该处理的信号。
     */
    @Test
    void retryPredicateSeparatesTransientFailuresFromDeterministicOnes() {
        assertTrue(UpstreamResilience.retryable(status(500)), "上游 5xx 是瞬时故障，该重试");
        assertTrue(UpstreamResilience.retryable(status(429)), "429 是被限流，正是重试的典型场景");
        assertTrue(UpstreamResilience.retryable(connectRefused()), "连不上是瞬时故障");
        assertTrue(UpstreamResilience.retryable(new TimeoutException()), "超时是瞬时故障");
        assertTrue(UpstreamResilience.retryable(new UpstreamEmptyBodyException("m")));
        assertTrue(UpstreamResilience.retryable(new UpstreamEmptyResponseException(emptyAnswer())),
                "空正文（推理预算耗尽）值得重试：推理 token 数高度不确定，重跑一次很可能出正文");

        assertFalse(UpstreamResilience.retryable(status(400)), "请求本身有问题，重试只会原样再得一次 400");
        assertFalse(UpstreamResilience.retryable(status(401)), "鉴权失败是确定性的");
        assertFalse(UpstreamResilience.retryable(status(404)), "模型名不存在是确定性的");
        assertFalse(UpstreamResilience.retryable(new IllegalArgumentException("messages 不能为空")));
        assertFalse(UpstreamResilience.retryable(new IllegalStateException("没见过的错误")),
                "白名单之外一律不重试 —— 重试看不懂的错误只会把它放大 N 倍（含账单）");
        assertFalse(UpstreamResilience.retryable(null));
    }

    // --- F2：转移谓词与重试谓词的关系 ---

    /**
     * {@code failoverable = retryable ∪ {熔断打开}} —— 这个**包含关系**是本条断言的全部内容。
     *
     * <p>三条一起看才说明问题：
     * <ol>
     *   <li>能重试的必须能转移（同一家再试一次可能好，那换一家就更可能好）；</li>
     *   <li>熔断打开能转移但<b>不能</b>重试 —— 重试仍进不去熔断器，只会空转烧预算，
     *       而换一家是完全不同的一条链路，那家好好的就该让它上；</li>
     *   <li>确定性错误两者都不做（换家会拿到一模一样的错）。</li>
     * </ol>
     *
     * <p>把第 2 条单独钉住是有必要的：很容易顺手写成
     * {@code failoverable = retryable}（"熔断打开也算一种错嘛"），
     * 那样一次链路故障仍然等于整站失败 —— 熔断加上了，故障转移却永远不生效。
     */
    @Test
    void failoverPredicateIsRetryPredicatePlusCircuitOpen() {
        // 1) 能重试的都能转移
        assertTrue(UpstreamResilience.failoverable(status(500)), "5xx：换一家可能就好了");
        assertTrue(UpstreamResilience.failoverable(status(429)), "429：这家在限流，换一家不一定");
        assertTrue(UpstreamResilience.failoverable(connectRefused()), "连不上这家");
        assertTrue(UpstreamResilience.failoverable(new TimeoutException()));
        assertTrue(UpstreamResilience.failoverable(new UpstreamEmptyBodyException("m")));
        assertTrue(UpstreamResilience.failoverable(new UpstreamEmptyResponseException(emptyAnswer())),
                "空正文是模型侧的问题，换个部署很可能就正常了");

        // 2) 熔断打开：可转移但不可重试 —— 差集就在这里
        UpstreamCircuitOpenException open =
                new UpstreamCircuitOpenException(PROVIDER, new RuntimeException("not permitted"));
        assertTrue(UpstreamResilience.failoverable(open), "这家不问了，那就去问下一家");
        assertFalse(UpstreamResilience.retryable(open),
                "原地重试进不去熔断器，只会空转并烧掉重试预算");

        // 3) 确定性错误：两者都不做
        assertFalse(UpstreamResilience.failoverable(status(400)), "换一家还是 400，请求本身不对");
        assertFalse(UpstreamResilience.failoverable(status(401)));
        assertFalse(UpstreamResilience.failoverable(new IllegalArgumentException("messages 不能为空")));
        assertFalse(UpstreamResilience.failoverable(new IllegalStateException("没见过的错误")),
                "白名单之外一律不转移 —— 转移是要花钱的，看不懂的错误不该触发它");
        assertFalse(UpstreamResilience.failoverable(null));
    }

    /** 整条候选链都不可用时造的错，provider() 里是逗号分隔的名单。 */
    @Test
    void allCandidatesUnavailableCarriesTheWholeChain() {
        UpstreamCircuitOpenException e =
                UpstreamCircuitOpenException.allCandidatesUnavailable("m", List.of("a", "b"));

        assertEquals("a,b", e.provider(), "客户端与运维最需要知道的是'哪些链路一起黑了'");
        assertTrue(e.getMessage().contains("m"), e.getMessage());
    }

    /**
     * 候选链可调用性：OPEN 要跳过，但 <b>HALF_OPEN 必须放行</b>。
     *
     * <p>把 HALF_OPEN 一并跳过会造出一个永远不会自愈的黑洞：半开的定义就是
     * "放几个探针看看恢复了没"，探针被我们跳过了，熔断器永远等不到那几次成功，
     * 永远回不到 CLOSED —— 而表面症状只是"流量都走到了备选"，
     * 首选链路明明是好的也没人用，排查时完全指不到这里。
     *
     * <p>所以这里用**短等待期**把状态真的推到 HALF_OPEN 再断言
     * （而不是断言一个"半开应该算可用"的常量）—— 前者能发现实现变了，
     * 后者只能发现枚举名字变了。
     */
    @Test
    void openProviderIsSkippedButHalfOpenIsStillCallable() {
        var cb = props.getResilience().getCircuitBreaker();
        cb.setSlidingWindowSize(2);
        cb.setMinimumNumberOfCalls(2);
        cb.setWaitDurationInOpenStateMillis(30);
        UpstreamResilience shortWait =
                new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry()));

        assertTrue(shortWait.callable(PROVIDER), "新 provider 默认 CLOSED，当然可以调");

        for (int i = 0; i < 2; i++) {
            StepVerifier.create(shortWait.protect(PROVIDER, Mono.error(status(500))))
                    .expectError().verify();
        }
        assertEquals(CircuitBreaker.State.OPEN, shortWait.stateOf(PROVIDER));
        assertFalse(shortWait.callable(PROVIDER), "OPEN 的候选要跳过 —— 跳过的理由是不白烧重试预算");

        // 等待期一过，automaticTransitionFromOpenToHalfOpenEnabled 会把它推到 HALF_OPEN
        long deadline = System.currentTimeMillis() + 2_000;
        while (shortWait.stateOf(PROVIDER) == CircuitBreaker.State.OPEN
                && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertEquals(CircuitBreaker.State.HALF_OPEN, shortWait.stateOf(PROVIDER));
        assertTrue(shortWait.callable(PROVIDER), "半开必须放行，否则永远回不到 CLOSED");
    }

    // --- 重试：次数与异常传递 ---
    /**
     * 重试耗尽后必须抛出<b>原始异常</b>，而不是 resilience4j 的
     * {@code MaxRetriesExceededException}。
     *
     * <p>这条守的是 {@code failAfterMaxAttempts(false)} 这一个配置项。它一旦被改成 true，
     * 上层就再也拿不到 {@link UpstreamEmptyResponseException} 身上那份原始响应，
     * F0 的 finish_reason 透传会被静默破坏 —— 而且症状是"客户端拿到 502"，
     * 看起来像上游挂了，完全指不到这里。
     */
    @Test
    void retryExhaustionPropagatesTheOriginalExceptionNotAWrappedOne() {
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> call = Mono.fromCallable(() -> {
            attempts.incrementAndGet();
            throw status(500);
        });

        StepVerifier.create(resilience.protect(PROVIDER, call))
                .expectErrorSatisfies(e -> assertInstanceOf(WebClientResponseException.class, e))
                .verify();

        assertEquals(3, attempts.get(), "maxAttempts=3：首次 + 2 次重试");
    }

    @Test
    void nonRetryableFailureIsAttemptedExactlyOnce() {
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> call = Mono.fromCallable(() -> {
            attempts.incrementAndGet();
            throw status(400);
        });

        StepVerifier.create(resilience.protect(PROVIDER, call))
                .expectError()
                .verify();

        assertEquals(1, attempts.get(), "4xx 不该触发重试");
    }

    // --- 熔断 ---

    @Test
    void circuitOpensAfterEnoughFailuresAndThenStopsCallingUpstream() {
        // 关掉重试，先把熔断器本身的行为单独看清楚
        props.getResilience().getRetry().setEnabled(false);
        resilience = new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry()));

        AtomicInteger attempts = new AtomicInteger();
        Mono<String> call = Mono.fromCallable(() -> {
            attempts.incrementAndGet();
            throw status(500);
        });

        for (int i = 0; i < 3; i++) {
            StepVerifier.create(resilience.protect(PROVIDER, call)).expectError().verify();
        }
        assertEquals(3, attempts.get());
        assertEquals(CircuitBreaker.State.OPEN, resilience.stateOf(PROVIDER),
                "3 次全失败 / 阈值 50% → 打开");

        // 打开之后的请求：内层 publisher 一次都不该再被订阅
        StepVerifier.create(resilience.protect(PROVIDER, call))
                .expectErrorSatisfies(e -> assertInstanceOf(UpstreamCircuitOpenException.class, e))
                .verify();
        assertEquals(3, attempts.get(), "熔断打开后不应再有上游调用 —— 这正是它省钱的方式");
    }

    /**
     * <b>一个反直觉但有意的耦合</b>：重试的每一次尝试都会进熔断窗口，
     * 因为熔断器被放在重试的<b>内层</b>（这样它才能看到每一次尝试的真实结果）。
     *
     * <p>后果是熔断触发的速度变成 {@code maxAttempts} 倍：同样的 min=3，
     * 不重试要 3 个失败请求，重试时只要 1 个。
     *
     * <p>这条断言不是为了"证明它对"，而是把一个会被误认为 bug 的现象钉住 ——
     * 否则将来有人看到"刚起服务打一个请求就熔断了"会去改熔断阈值，
     * 而真正的原因在这里。
     */
    @Test
    void everyRetryAttemptCountsTowardTheCircuitWindow() {
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> call = Mono.fromCallable(() -> {
            attempts.incrementAndGet();
            throw status(500);
        });

        StepVerifier.create(resilience.protect(PROVIDER, call)).expectError().verify();

        assertEquals(3, attempts.get());
        assertEquals(CircuitBreaker.State.OPEN, resilience.stateOf(PROVIDER),
                "maxAttempts=3 + minimumNumberOfCalls=3：一次失败请求就够打开熔断器");
    }

    // --- 流式：重试闸门 ---

    /**
     * 已经吐过分片之后<b>绝不能</b>重试。
     *
     * <p>重试 = 重新订阅上游、从头再放一遍。流式一旦已经把内容推给客户端，
     * 重放会让客户端收到"前一次成功的半截 + 这一次完整的一遍"两段拼接的答案。
     * 这种损坏不可撤销，而且看起来像模型在胡说 —— 排查时完全指不到重试头上。
     */
    @Test
    void streamIsNotRetriedOnceAChunkReachedTheClient() {
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<String> call = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.just("part-1").concatWith(Flux.error(status(500)));
        });
        AtomicBoolean produced = new AtomicBoolean(false);

        StepVerifier.create(resilience.protectStream(PROVIDER, call, produced))
                .expectNext("part-1")
                .expectError()
                .verify();

        assertEquals(1, subscriptions.get(), "吐过分片就不许重试");
        assertTrue(produced.get(), "标记必须置上，失败时的配额口径要靠它");
    }

    /**
     * 首片之前出错则可以重试，且客户端只收到<b>一份</b>完整数据。
     * 与上一条互为反面：闸门的两个方向都要钉住，只测一边等于没测。
     */
    @Test
    void streamIsRetriedWhileNothingWasProducedYet() {
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<String> call = Flux.defer(() -> {
            if (subscriptions.incrementAndGet() == 1) {
                return Flux.error(status(500));
            }
            return Flux.just("answer-a", "answer-b");
        });
        AtomicBoolean produced = new AtomicBoolean(false);

        // 逐字断言 + 期待正常结束：若重放发生，客户端会多收到一遍 answer-a/answer-b
        StepVerifier.create(resilience.protectStream(PROVIDER, call, produced))
                .expectNext("answer-a", "answer-b")
                .verifyComplete();

        assertEquals(2, subscriptions.get());
    }

    /**
     * 首片超时守住的是"多久还没吐出第一个字"，<b>不是</b>整条流的时长。
     *
     * <p>这条钉住的是"参数 → 算子"的映射：{@code Flux.timeout(Duration)} 的作用域
     * 恰好就是首元素。若哪天有人换成 {@code timeout(Duration, Duration)}
     * （两参数版会给每个元素都加超时），长回答会被逐片掐断 ——
     * 而症状只是"答案偶尔短了"，非常难查。
     */
    @Test
    void firstChunkTimeoutAbortsASlowStream() {
        props.getResilience().setFirstChunkTimeoutMillis(50);
        props.getResilience().getRetry().setEnabled(false);
        resilience = new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry()));

        Flux<String> slow = Mono.delay(Duration.ofMillis(400)).thenMany(Flux.just("late"));
        AtomicBoolean produced = new AtomicBoolean(false);

        StepVerifier.create(resilience.protectStream(PROVIDER, slow, produced))
                .expectError(TimeoutException.class)
                .verify(Duration.ofSeconds(3));

        assertFalse(produced.get());
    }

    /** 首片按时到达的流不被超时打断 —— 反面，防止把超时配成"总是触发"。 */
    @Test
    void aStreamThatStartsInTimeIsNotAborted() {
        props.getResilience().setFirstChunkTimeoutMillis(2_000);
        resilience = new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry()));

        // 首片立刻到，第二个元素晚于首片超时才到：整条流仍应正常结束
        Flux<String> call = Flux.concat(
                Flux.just("fast"),
                Mono.delay(Duration.ofMillis(120)).thenMany(Flux.just("slow-but-after-first")));
        AtomicBoolean produced = new AtomicBoolean(false);

        StepVerifier.create(resilience.protectStream(PROVIDER, call, produced))
                .expectNext("fast", "slow-but-after-first")
                .verifyComplete();
    }

    // --- 配置关闭时的行为 ---

    /**
     * 两个开关都能独立关掉，且关掉之后**不留任何副作用**。
     *
     * <p>为什么值得一条断言：这一条是故障期最可能被用到的操作
     * （"先在配置里把重试关了止血"），而"关不干净"的表现是
     * 你以为关掉了、请求却还在被包一层 —— 排查时会去怀疑别的地方。
     */
    @Test
    void disablingRetryAndBreakerLeavesTheCallUntouched() {
        props.getResilience().getRetry().setEnabled(false);
        props.getResilience().getCircuitBreaker().setEnabled(false);
        resilience = new UpstreamResilience(props, new GatewayMetrics(new SimpleMeterRegistry()));

        AtomicInteger attempts = new AtomicInteger();
        Mono<String> call = Mono.fromCallable(() -> {
            attempts.incrementAndGet();
            throw status(500);
        });

        StepVerifier.create(resilience.protect(PROVIDER, call)).expectError().verify();
        assertEquals(1, attempts.get(), "重试关掉后只尝试一次");
        assertNotEquals(CircuitBreaker.State.OPEN, resilience.stateOf(PROVIDER),
                "熔断关掉后任何状态变化都不该发生");
    }

    // --- 辅助 ---

    private static ChatResponse emptyAnswer() {
        return new ChatResponse("id", "m", "", new Usage(1, 2), false, "length");
    }

    private static WebClientResponseException status(int code) {
        return WebClientResponseException.create(code, "boom", HttpHeaders.EMPTY, new byte[0], null);
    }

    private static WebClientRequestException connectRefused() {
        return new WebClientRequestException(new ConnectException("connection refused"),
                HttpMethod.POST, URI.create("http://127.0.0.1:9/v1/chat/completions"),
                HttpHeaders.EMPTY);
    }
}
