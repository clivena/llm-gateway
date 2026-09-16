package com.llmgw.core.quota;

import com.llmgw.config.GatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 分布式配额：脚本在 Redis 侧原子执行，避免"先查后写"的竞态导致超发。
 * 失败策略：Redis 不可用时 fail-open（放行）+ 记日志，保证可用性优先；
 * 生产可改为 fail-close 或降级到本地令牌桶（见 DESIGN.md）。
 *
 * 实现说明：用 ReactiveStringRedisTemplate.execute(RedisScript, keys, args)，
 * 由模板负责脚本缓存与参数序列化；直接操作 ReactiveRedisConnection.eval
 * 需要处理 ByteBuffer 参数与 ReturnType，容易踩签名不匹配的坑。
 */
@Service
public class QuotaService {

    private static final Logger log = LoggerFactory.getLogger(QuotaService.class);

    private final ReactiveStringRedisTemplate redis;
    private final GatewayProperties properties;
    private final DefaultRedisScript<Long> script;

    public QuotaService(ReactiveStringRedisTemplate redis, GatewayProperties properties) {
        this.redis = redis;
        this.properties = properties;
        this.script = new DefaultRedisScript<>(loadScript(), Long.class);
    }

    public Mono<Boolean> tryConsume(String apiKey, long tokens) {
        String key = quotaKey(apiKey);
        long limit = properties.getQuota().getDefaultLimitTokens();
        long ttl = properties.getQuota().getPeriodTtlSeconds();

        return redis.execute(script, List.of(key),
                        List.of(String.valueOf(tokens),
                                String.valueOf(limit),
                                String.valueOf(ttl)))
                .singleOrEmpty()
                .map(code -> code != null && code == 0L)
                .defaultIfEmpty(false)
                .doOnNext(allowed -> {
                    if (!allowed) {
                        log.info("quota exceeded: key={} need={}", key, tokens);
                    }
                })
                .onErrorResume(e -> {
                    log.warn("quota check failed, fail-open: {}", e.getMessage());
                    return Mono.just(true);
                });
    }

    /**
     * 只读：查询当前周期已用配额，供运维面板展示，不做任何扣减。
     * 与 tryConsume 用同一套 key 规则，否则读出来的数和实际扣的对不上。
     */
    public Mono<Map<String, Object>> currentUsage(String apiKey) {
        String key = quotaKey(apiKey);
        long limit = properties.getQuota().getDefaultLimitTokens();

        return redis.opsForValue().get(key)
                .map(QuotaService::parseLongOrZero)
                .defaultIfEmpty(0L)
                .map(used -> usage(apiKey, key, used, limit, false))
                .onErrorResume(e -> {
                    // 面板查询失败不该让页面报错，降级返回"读不到"，并标记 degraded 供前端提示
                    log.warn("quota read failed: {}", e.getMessage());
                    return Mono.just(usage(apiKey, key, 0L, limit, true));
                });
    }

    /** 只读：Redis 连通性探测。取不存在的 key 也会返回空 Mono，能走完就说明连接可用。 */
    public Mono<Boolean> ping() {
        return redis.opsForValue().get("__llmgw_ping__")
                .map(v -> true)
                .defaultIfEmpty(true);
    }

    private static String quotaKey(String apiKey) {
        return "q:" + apiKey + ":" + YearMonth.now();
    }

    private static long parseLongOrZero(String raw) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static Map<String, Object> usage(String apiKey, String key, long used, long limit, boolean degraded) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("apiKey", apiKey);
        m.put("key", key);
        m.put("period", YearMonth.now().toString());
        m.put("usedTokens", used);
        m.put("limitTokens", limit);
        m.put("remainingTokens", Math.max(0L, limit - used));
        m.put("degraded", degraded);
        return m;
    }

    private static String loadScript() {
        try {
            return new String(new ClassPathResource("lua/quota.lua")
                    .getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot load lua/quota.lua", e);
        }
    }
}
