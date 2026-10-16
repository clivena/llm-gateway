package com.llmgw.core.cache;

import reactor.core.publisher.Mono;

/**
 * 单层缓存。门面 {@link LayeredCacheService} 按 {@code @Order} 顺序串联各层，
 * 前一层命中即返回，不再产生后续层的开销。
 *
 * <h3>为什么要拆出"层"这个概念（P4）</h3>
 * 旧实现只有一层，而那一层依赖 embedding —— 供应商没有 {@code /embeddings} 端点时，
 * 整条缓存路径恒 MISS（取不到向量 → 空 Mono），{@code write} 也恒不落数据。
 * 于是"缓存开着"却一次都没命中过。
 *
 * <p>这类<b>静默失效</b>比"没做缓存"更糟：没做缓存时人知道每次都花钱，
 * 而缓存开着会让人以为有保护、不再关注。真机验证时那条 {@code cache_hit=false}
 * 的账本行，就是这么来的。
 *
 * <p>拆层之后各自的能力边界是显式的：
 * <ul>
 *   <li><b>L1 精确</b>：规范化问题 → 哈希 → 直接查 Redis。不依赖任何外部能力，
 *       任何供应商下都能工作，也没有错命中的可能。</li>
 *   <li><b>L2 语义</b>：向量检索，能命中"意思相同但字面不同"的问题。
 *       是可选增强 —— 没有 embedding 能力时自行跳过，不影响 L1。</li>
 * </ul>
 *
 * <p>契约：方法不得阻塞调用线程；单层失败不得让整条链失败（由实现或门面吞掉）。
 */
public interface CacheLayer {

    /** 层名，用于日志与"命中了哪一层"的归属判断。 */
    String name();

    /** 命中返回该条目；未命中返回空 Mono。 */
    Mono<CacheEntry> lookup(String namespace, String question);

    /** 写入。调用方通常 fire-and-forget 订阅。 */
    Mono<Void> write(String namespace, String question, String answer);
}
