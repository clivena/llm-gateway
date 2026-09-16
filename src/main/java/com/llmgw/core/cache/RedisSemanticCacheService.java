package com.llmgw.core.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.protocol.ProviderAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 语义缓存。
 *
 * 设计取舍：不依赖 Redis Stack 的向量索引，改为「Redis List 存条目 + 应用内余弦计算」。
 *  - 优点：任意 Redis 都能跑，条目数量按 namespace 截断（默认 500），成本可控；
 *  - 缺点：O(n) 扫描，只适合小规模；后续可无痛切到 FT.CREATE ... VECTOR + KNN。
 *  - 防误命中：低相似度直接 MISS（不返回"最像的一条"），命中还要过阈值。
 */
@Service
public class RedisSemanticCacheService implements SemanticCacheService {

    private static final Logger log = LoggerFactory.getLogger(RedisSemanticCacheService.class);

    private final ReactiveStringRedisTemplate redis;
    private final List<ProviderAdapter> adapters;
    private final GatewayProperties properties;
    private final ObjectMapper mapper = new ObjectMapper();

    public RedisSemanticCacheService(ReactiveStringRedisTemplate redis,
                                     List<ProviderAdapter> adapters,
                                     GatewayProperties properties) {
        this.redis = redis;
        this.adapters = adapters;
        this.properties = properties;
    }

    @Override
    public Optional<CacheEntry> lookup(String namespace, String question) {
        if (!properties.getCache().isEnabled() || question.isBlank()) {
            return Optional.empty();
        }
        float[] query = embedOne(question);
        if (query == null) {
            return Optional.empty();
        }
        double threshold = properties.getCache().getSimilarityThreshold();

        List<String> rawEntries = redis.opsForList().range(key(namespace), 0,
                properties.getCache().getMaxEntriesPerNamespace() - 1L).collectList().block();
        if (rawEntries == null || rawEntries.isEmpty()) {
            return Optional.empty();
        }

        CacheEntry best = null;
        double bestSimilarity = -1;
        for (String raw : rawEntries) {
            try {
                JsonNode node = mapper.readTree(raw);
                JsonNode vecNode = node.path("vector");
                float[] vec = new float[vecNode.size()];
                for (int i = 0; i < vecNode.size(); i++) {
                    vec[i] = (float) vecNode.get(i).asDouble();
                }
                double similarity = cosine(query, vec);
                if (similarity > bestSimilarity) {
                    bestSimilarity = similarity;
                    best = new CacheEntry(node.path("answer").asText(""), similarity);
                }
            } catch (Exception e) {
                log.debug("skip malformed cache entry: {}", e.getMessage());
            }
        }
        if (best == null || bestSimilarity < threshold) {
            return Optional.empty();
        }
        return Optional.of(best);
    }

    @Override
    public void write(String namespace, String question, String answer) {
        if (!properties.getCache().isEnabled() || question.isBlank()) {
            return;
        }
        float[] vector = embedOne(question);
        if (vector == null) {
            return;
        }
        try {
            String payload = mapper.writeValueAsString(Map.of(
                    "question", question,
                    "answer", answer,
                    "vector", vector));
            String key = key(namespace);
            redis.opsForList().leftPush(key, payload).subscribe();
            redis.opsForList().trim(key, 0, properties.getCache().getMaxEntriesPerNamespace() - 1L).subscribe();
            redis.expire(key, Duration.ofSeconds(properties.getCache().getTtlSeconds())).subscribe();
        } catch (Exception e) {
            log.warn("cache write skipped: {}", e.getMessage());
        }
    }

    private String key(String namespace) {
        return "sem:" + namespace;
    }

    private float[] embedOne(String text) {
        for (ProviderAdapter adapter : adapters) {
            try {
                List<float[]> vectors = adapter.embed(List.of(text));
                if (!vectors.isEmpty()) {
                    return vectors.get(0);
                }
            } catch (Exception e) {
                log.debug("embed via {} failed: {}", adapter.name(), e.getMessage());
            }
        }
        return null;
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
