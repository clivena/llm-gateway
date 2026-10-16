package com.llmgw.core.cache;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 两级缓存的组合行为（P4）。
 *
 * <p>这里守的是三件在真机上不容易观察到的事：
 * <ol>
 *   <li><b>L1 命中后不能再碰 L2</b> —— L2 的检索会调一次<b>要花钱的</b> embedding。
 *       为了一个已经命中的请求去调它，等于把缓存省下的钱又花回去。
 *       这条只能靠断言"L2 的调用次数为 0"来守，日志里看不出来。</li>
 *   <li><b>某层失败不能中断整条链</b> —— L2 在无 embedding 能力的供应商下必然失败，
 *       那是预期情况，不该让整次请求失败。</li>
 *   <li><b>写入时每层各写各的</b> —— 与查询不同，写入不做短路。</li>
 * </ol>
 */
class LayeredCacheServiceTest {

    /** 记录调用次数的假层，用来断言"谁被调了、调了几次"。 */
    private static final class FakeLayer implements CacheLayer {

        private final String name;
        private final Mono<CacheEntry> lookupResult;
        private int lookups;
        private int writes;

        FakeLayer(String name, Mono<CacheEntry> lookupResult) {
            this.name = name;
            this.lookupResult = lookupResult;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Mono<CacheEntry> lookup(String namespace, String question) {
            lookups++;
            return lookupResult;
        }

        @Override
        public Mono<Void> write(String namespace, String question, String answer) {
            writes++;
            return Mono.empty();
        }
    }

    @Test
    void firstLayerHitShortCircuitsTheRest() {
        FakeLayer l1 = new FakeLayer("L1-exact", Mono.just(new CacheEntry("from-L1", 1.0)));
        FakeLayer l2 = new FakeLayer("L2-semantic", Mono.just(new CacheEntry("from-L2", 0.95)));
        LayeredCacheService cache = new LayeredCacheService(List.of(l1, l2));

        StepVerifier.create(cache.lookup("ns", "问题"))
                .expectNextMatches(entry -> "from-L1".equals(entry.answer()))
                .verifyComplete();

        assertEquals(1, l1.lookups);
        // 关键断言：L1 命中后 L2 一次都不该被调。
        // L2 的 lookup 内部会发起 embedding 调用 —— 那是要计费的。
        assertEquals(0, l2.lookups, "L1 命中后不应再查 L2：那会白花一次 embedding 调用");
    }

    @Test
    void fallsThroughToNextLayerOnMiss() {
        FakeLayer l1 = new FakeLayer("L1-exact", Mono.empty());
        FakeLayer l2 = new FakeLayer("L2-semantic", Mono.just(new CacheEntry("from-L2", 0.95)));
        LayeredCacheService cache = new LayeredCacheService(List.of(l1, l2));

        StepVerifier.create(cache.lookup("ns", "问题"))
                .expectNextMatches(entry -> "from-L2".equals(entry.answer()))
                .verifyComplete();

        assertEquals(1, l1.lookups);
        assertEquals(1, l2.lookups);
    }

    /**
     * 无 embedding 能力的供应商下，L2 会抛 "embedding-model not configured"。
     * 这必须表现为"该层 MISS"，而不是让整条缓存链（乃至整次请求）失败。
     */
    @Test
    void failingLayerDegradesToMissInsteadOfPropagating() {
        FakeLayer l1 = new FakeLayer("L1-exact", Mono.empty());
        FakeLayer l2 = new FakeLayer("L2-semantic",
                Mono.error(new IllegalStateException("embedding-model not configured")));
        LayeredCacheService cache = new LayeredCacheService(List.of(l1, l2));

        // 整体应是"未命中"（空 Mono 正常完成），不是 onError
        StepVerifier.create(cache.lookup("ns", "问题"))
                .verifyComplete();
    }

    @Test
    void writeReachesEveryLayer() {
        FakeLayer l1 = new FakeLayer("L1-exact", Mono.empty());
        FakeLayer l2 = new FakeLayer("L2-semantic", Mono.empty());
        LayeredCacheService cache = new LayeredCacheService(List.of(l1, l2));

        StepVerifier.create(cache.write("ns", "问题", "答案"))
                .verifyComplete();

        // 写入不做短路：两层各自决定要不要写（L2 没有 embedding 能力时自行跳过）
        assertEquals(1, l1.writes);
        assertEquals(1, l2.writes);
    }

    @Test
    void writeFailureInOneLayerDoesNotBreakTheOthers() {
        FakeLayer broken = new FakeLayer("L2-semantic",
                Mono.error(new IllegalStateException("redis down")));
        FakeLayer l1 = new FakeLayer("L1-exact", Mono.empty());
        LayeredCacheService cache = new LayeredCacheService(List.of(l1, broken));

        StepVerifier.create(cache.write("ns", "问题", "答案"))
                .verifyComplete();

        assertEquals(1, l1.writes);
    }

    /** 没有任何层时（缓存全关）不能抛异常，也不能挂住。 */
    @Test
    void noLayersIsSafe() {
        LayeredCacheService cache = new LayeredCacheService(List.of());

        StepVerifier.create(cache.lookup("ns", "问题")).verifyComplete();
        StepVerifier.create(cache.write("ns", "问题", "答案")).verifyComplete();
    }
}
