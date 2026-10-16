package com.llmgw.core.cache;

import reactor.core.publisher.Mono;

/**
 * 语义缓存。
 *
 * 为什么返回 Mono 而不是 Optional（P1 响应式化）：
 *   lookup 内部要走 embedding HTTP 调用与 Redis 读取，两者都是 IO。
 *   旧签名返回 Optional，意味着实现里必须 .block() 才能拿到同步结果 ——
 *   而被占住的那个线程，正是 Netty 的事件循环线程。
 *
 *   未命中改由**空 Mono** 表达，不再需要 Optional 这一层包装：
 *   调用方原本要写 flatMap(Mono::justOrEmpty).switchIfEmpty(...)，
 *   现在直接 flatMap(...).switchIfEmpty(...) 即可。
 *
 * 契约：两个方法都不得阻塞调用线程；写入失败不得影响调用方（由实现内部记日志）。
 */
public interface SemanticCacheService {

    /** 命中返回该条目；未命中返回空 Mono。 */
    Mono<CacheEntry> lookup(String namespace, String question);

    /** 写入。调用方通常 fire-and-forget 订阅，不需要等待完成。 */
    Mono<Void> write(String namespace, String question, String answer);

    record CacheEntry(String answer, double similarity) {
    }
}
