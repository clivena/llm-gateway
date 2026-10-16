package com.llmgw.core.cache;

import com.llmgw.config.GatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;

/**
 * L1 精确缓存：规范化问题文本 → SHA-256 → 直接查 Redis 的单值。
 *
 * <h3>为什么需要它</h3>
 * L2 语义缓存依赖 embedding，而当前上游（DeepSeek）没有 {@code /embeddings} 端点 ——
 * 缓存因此长期处于"配置里开着、运行时一次都没命中"的状态。而"同一个问题问两遍"
 * 在任何真实业务里都非常常见（用户重试、页面刷新、批量任务重跑），
 * 这类请求根本不需要向量检索就能命中：字面完全相同。
 *
 * <p>L1 的能力边界正是它的价值：<b>零外部依赖、零误命中</b>。
 * 精确匹配不存在"意思相近但不是同一个问题"的风险，所以它可以在任何部署下默认开启。
 *
 * <h3>规范化到什么程度：刻意保守</h3>
 * 每一步规范化都在<b>扩大命中面</b>，而缓存最坏的失败模式是<b>错答案</b>（不是漏命中）。
 * 所以这里只做语义上绝对安全的三种处理：
 * <ol>
 *   <li>{@code trim} —— 首尾空白不携带语义；</li>
 *   <li>连续空白折叠为单个空格 —— 换行/多空格不携带语义；</li>
 *   <li>ASCII 大小写归一 —— "Java" 与 "java" 指同一个东西（对中文无影响）。</li>
 * </ol>
 * <b>不做</b>的事同样重要：不剥标点（"这是对的" vs "这是对的？"是两个问题）、
 * 不做词形还原、不做同义词替换。那些属于 L2 的职责 —— 而 L2 有相似度阈值兜着。
 * 把模糊匹配塞进精确层，等于把两层合并成一层却丢掉了阈值保护。
 */
@Service
@Order(1)
public class ExactCacheLayer implements CacheLayer {

    private static final Logger log = LoggerFactory.getLogger(ExactCacheLayer.class);

    /** key 前缀与 L2 的 {@code sem:} 区分开，便于直接扫 Redis 定位命中来自哪层。 */
    private static final String PREFIX = "ex:";

    private final ReactiveStringRedisTemplate redis;
    private final GatewayProperties properties;

    public ExactCacheLayer(ReactiveStringRedisTemplate redis, GatewayProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    @Override
    public String name() {
        return "L1-exact";
    }

    @Override
    public Mono<CacheEntry> lookup(String namespace, String question) {
        if (!enabled() || question == null || question.isBlank()) {
            return Mono.empty();
        }
        return redis.opsForValue().get(key(namespace, question))
                // 相似度恒为 1.0：字面相同就是同一个问题，没有"像不像"的余地。
                // 填 1.0 而不是 0 是为了让日志/调试时一眼看出这是精确命中。
                .map(answer -> new CacheEntry(answer, 1.0));
    }

    @Override
    public Mono<Void> write(String namespace, String question, String answer) {
        if (!enabled() || question == null || question.isBlank()
                || answer == null || answer.isBlank()) {
            // 与 ChatPipeline.writeCache 同理：跳过要留痕，否则"缓存从不写入"
            // 这种现象在日志里完全看不到。
            log.warn("L1 write skipped [ns={}]: enabled={} questionBlank={} answerBlank={}",
                    namespace, enabled(), question == null || question.isBlank(),
                    answer == null || answer.isBlank());
            return Mono.empty();
        }
        long ttl = properties.getCache().getTtlSeconds();
        return redis.opsForValue()
                .set(key(namespace, question), answer, Duration.ofSeconds(ttl))
                .then()
                // 写失败不能影响调用方已经拿到的回答。
                .onErrorResume(e -> {
                    log.debug("L1 cache write skipped: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    private boolean enabled() {
        return properties.getCache().isEnabled() && properties.getCache().isExactEnabled();
    }

    /**
     * L1 的 key。
     *
     * 用 SHA-256 而不是"把问题直接拼进 key"：问题文本长度不可控，长 prompt 会让
     * key 变得很大（Redis key 有实际内存成本，也在日志里很难看）；而且文本里可能出现
     * 空格、换行、冒号等字符，拼进 key 会破坏 key 的结构。
     *
     * <p>用完整 64 位十六进制而不是截断：截断虽然省几个字节，但把"理论无碰撞"
     * 降级成"概率无碰撞"，而这里省下的空间毫无意义。
     */
    static String key(String namespace, String question) {
        return PREFIX + namespace + ":" + sha256(normalize(question));
    }

    /** 规范化：只做语义上绝对安全的处理，见类注释。包可见以便单测直接断言。 */
    static String normalize(String question) {
        if (question == null) {
            return "";
        }
        return question.trim()
                .replaceAll("\\s+", " ")
                .toLowerCase(Locale.ROOT);
    }

    private static String sha256(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 是 JDK 必须实现的算法，走不到这里；真到了这里说明运行环境异常，
            // 不能静默降级成"用原文当 key"（那会让 key 结构失控）。
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
