package com.llmgw.core.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.protocol.ProviderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * L2 语义缓存：向量相似度检索，能命中"意思相同但字面不同"的问题。
 *
 * <p><b>它是可选的增强层，不是缓存的全部</b>（P4）。这一层依赖 embedding 能力，
 * 而上游可能没有 {@code /embeddings} 端点（DeepSeek 实测 404）。那种情况下
 * 本层会静默退化为"恒 MISS" —— 这正是 P4 要把它降为 L2、并在前面加上
 * 不依赖任何外部能力的 {@link ExactCacheLayer} 的原因：
 * 让"缓存开着却一次都没命中"变成不可能发生的事。
 *
 * 设计取舍：不依赖 RediSearch 的向量索引，改为「Redis List 存条目 + 应用内余弦计算」。
 *  - 优点：任意 Redis 都能跑，条目数量按 namespace 截断（默认 500），成本可控；
 *  - 缺点：O(n) 扫描，只适合小规模；后续可无痛切到 FT.CREATE ... VECTOR + KNN。
 *  - 防误命中：低相似度直接 MISS（不返回"最像的一条"），命中还要过阈值。
 *
 * P1 响应式化改动：
 *   ① 两个方法都改为返回 Mono，内部不再有 .block()；
 *   ② write 的三步 Redis 操作由"三个独立 subscribe 并发提交"改为**串行链**。
 *      旧实现里 trim 若先于 leftPush 抵达 Redis，截断的就是上一批数据 ——
 *      这类问题不报错、只在数据层面留下偏差，属于最难排查的一类。
 */
@Service
@Order(2)
public class RedisSemanticCacheService implements CacheLayer {

    private static final Logger log = LoggerFactory.getLogger(RedisSemanticCacheService.class);

    /** ObjectMapper 线程安全，静态复用。 */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ReactiveStringRedisTemplate redis;
    private final ProviderRegistry providers;
    private final GatewayProperties properties;

    public RedisSemanticCacheService(ReactiveStringRedisTemplate redis,
                                     ProviderRegistry providers,
                                     GatewayProperties properties) {
        this.redis = redis;
        this.providers = providers;
        this.properties = properties;
    }

    @Override
    public String name() {
        return "L2-semantic";
    }

    /**
     * 总开关 + 本层开关都要满足。把语义层单独做成开关（而不只看总开关）的理由：
     * 没有 embedding 能力时这层是纯粹的空转 —— 每次请求都要走一遍
     * "遍历所有 adapter 试 embed"，全部失败才放弃。白白多一次 IO 等待。
     * 显式关掉它比依赖"调用失败了自然会降级"更诚实。
     */
    private boolean enabled() {
        return properties.getCache().isEnabled() && properties.getCache().isSemanticEnabled();
    }

    @Override
    public Mono<CacheEntry> lookup(String namespace, String question) {
        if (!enabled() || question == null || question.isBlank()) {
            return Mono.empty();
        }
        double threshold = properties.getCache().getSimilarityThreshold();
        long maxEntries = properties.getCache().getMaxEntriesPerNamespace();

        return embedOne(question)
                .flatMapMany(query -> redis.opsForList()
                        .range(key(namespace), 0, maxEntries - 1L)
                        .map(raw -> score(raw, query))
                        // 格式损坏的条目直接丢弃，不能让它中断整轮扫描。
                        .filter(Objects::nonNull)
                        // 阈值之下直接 MISS，不返回"最像的一条"：
                        // 语义缓存最坏的失败模式是**错答案**，不是漏命中。
                        .filter(entry -> entry.similarity() >= threshold))
                // 取相似度最高的一条。用 reduce 而不是 collectSortedList().next()：
                // 后者要先把整段缓冲下来排序，而这里只需要一个极值。
                .reduce((a, b) -> a.similarity() >= b.similarity() ? a : b);
    }

    @Override
    public Mono<Void> write(String namespace, String question, String answer) {
        if (!enabled() || question == null || question.isBlank()
                || answer == null || answer.isBlank()) {
            return Mono.empty();
        }
        String key = key(namespace);
        int maxEntries = properties.getCache().getMaxEntriesPerNamespace();
        long ttlSeconds = properties.getCache().getTtlSeconds();

        return embedOne(question)
                .flatMap(vector -> {
                    String payload;
                    try {
                        payload = MAPPER.writeValueAsString(Map.of(
                                "question", question,
                                "answer", answer,
                                "vector", vector));
                    } catch (Exception e) {
                        return Mono.error(e);
                    }
                    // 三步必须串行：trim 依赖 push 已经完成，否则会截错数据。
                    return redis.opsForList().leftPush(key, payload)
                            .then(redis.opsForList().trim(key, 0, maxEntries - 1L))
                            .then(redis.expire(key, Duration.ofSeconds(ttlSeconds)))
                            .then();
                })
                // 写缓存失败不能影响调用方已经拿到的回答，吞掉并记 debug。
                .onErrorResume(e -> {
                    log.debug("cache write skipped: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    private String key(String namespace) {
        return "sem:" + namespace;
    }

    /**
     * 依次尝试各适配器，取第一个成功返回的向量。
     * 某个 provider 没有 embedding 能力（或调用失败）时跳到下一家，
     * 全部失败则返回空 Mono，由调用方按"缓存不可用"处理 —— 不抛异常，主链路不受影响。
     */
    private Mono<float[]> embedOne(String text) {
        return Flux.fromIterable(providers.all())
                .concatMap(adapter -> adapter.embed(List.of(text))
                        .flatMap(vectors -> vectors.isEmpty()
                                ? Mono.<float[]>empty()
                                : Mono.just(vectors.get(0)))
                        .onErrorResume(e -> {
                            log.debug("embed via {} failed: {}", adapter.name(), e.getMessage());
                            return Mono.empty();
                        }))
                .next();
    }

    /** 单条缓存的打分。条目格式损坏时返回 null（由调用方 filter 掉），不让它中断整轮扫描。 */
    private static CacheEntry score(String raw, float[] query) {
        try {
            JsonNode node = MAPPER.readTree(raw);
            JsonNode vecNode = node.path("vector");
            float[] vec = new float[vecNode.size()];
            for (int i = 0; i < vecNode.size(); i++) {
                vec[i] = (float) vecNode.get(i).asDouble();
            }
            return new CacheEntry(node.path("answer").asText(""), cosine(query, vec));
        } catch (Exception e) {
            return null;
        }
    }

    private static double cosine(float[] a, float[] b) {
        int n = Math.min(a.length, b.length);
        double dot = 0;
        double na = 0;
        double nb = 0;
        for (int i = 0; i < n; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) {
            return 0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }
}
