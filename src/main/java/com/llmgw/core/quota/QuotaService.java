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
 *
 * <h3>两段式：预扣 + 结算（F0）</h3>
 * 配额不是一次扣完的，而是分两段：
 * <ol>
 *   <li><b>预扣</b>（{@link #tryConsume}）在请求**之前**。此时只有 prompt 的
 *       估算值可用 —— 真实用量还不存在。它的作用是"在花钱之前拦住超额请求"。</li>
 *   <li><b>结算</b>（{@link #settle}）在响应**之后**，按真实用量补扣差额。
 *       差额可以是负的（缓存命中时全额退还预扣）。</li>
 * </ol>
 *
 * 只有预扣没有结算时，配额记的就只是"prompt 估算之和"：实测 completion 是
 * prompt 的 20~100 倍（推理型模型把 reasoning token 也计进 completion_tokens），
 * 于是 {@code default-limit-tokens} 实际约束的东西与它名字宣称的不是一回事。
 *
 * <p><b>已知代价</b>：reserve-then-settle 天然允许小幅超发 —— 真实值要等上游
 * 返回才知道，那时钱已经花了。这是选这个口径的代价，不是缺陷；
 * 要收紧应改用"按 max_tokens 上界预留"，而不是在结算时补一道闸。
 */
@Service
public class QuotaService {

    private static final Logger log = LoggerFactory.getLogger(QuotaService.class);

    private final ReactiveStringRedisTemplate redis;
    private final GatewayProperties properties;
    private final DefaultRedisScript<Long> script;
    private final DefaultRedisScript<Long> settleScript;

    public QuotaService(ReactiveStringRedisTemplate redis, GatewayProperties properties) {
        this.redis = redis;
        this.properties = properties;
        this.script = new DefaultRedisScript<>(loadScript("lua/quota.lua"), Long.class);
        this.settleScript = new DefaultRedisScript<>(loadScript("lua/quota_settle.lua"), Long.class);
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
     * 按真实用量补扣差额（F0）。
     *
     * @param apiKey   租户
     * @param reserved 预扣时用的估算值（{@link TokenEstimator#estimatePrompt}）
     * @param actual   上游回传的真实总用量（prompt + completion）。**必须含 completion** ——
     *                 漏掉它正是 F0 要修的那个缺口。缓存命中传 0（全额退还预扣）。
     * @return 结算后的已用量；-1 表示本周期键已不存在、已跳过
     */
    public Mono<Long> settle(String apiKey, long reserved, long actual) {
        String key = quotaKey(apiKey);
        long ttl = properties.getQuota().getPeriodTtlSeconds();
        long delta = actual - reserved;

        return redis.execute(settleScript, List.of(key),
                        List.of(String.valueOf(delta), String.valueOf(ttl)))
                .singleOrEmpty()
                .defaultIfEmpty(-1L)
                .doOnNext(finalUsed -> {
                    if (finalUsed < 0) {
                        // 周期翻篇，跳过。debug 而不是 warn —— 这是每月会发生一次的
                        // 正常边界，不是异常。
                        log.debug("quota settle skipped (period rolled over): key={} delta={}", key, delta);
                    } else {
                        log.debug("quota settled: key={} reserved={} actual={} delta={} used={}",
                                key, reserved, actual, delta, finalUsed);
                    }
                });
        // 刻意**不在这里吞异常**：结算失败该如何处置是调用方的决定
        // （ChatPipeline 要计数并让请求照常成功）。服务层吞掉的话，
        // 调用方连"结算失败了"都不知道，只能看到一个 -1。
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

    private static String loadScript(String path) {
        try {
            return new String(new ClassPathResource(path)
                    .getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot load " + path, e);
        }
    }
}
