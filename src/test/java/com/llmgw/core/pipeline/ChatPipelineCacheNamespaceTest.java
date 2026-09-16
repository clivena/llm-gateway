package com.llmgw.core.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ChatPipelineCacheNamespaceTest {

    /**
     * 回归：缓存 namespace 曾经只有 apiKey（sem:{apiKey}），
     * 导致同一租户下强模型的答案会被弱模型请求命中，路由策略被缓存架空。
     */
    @Test
    void cacheNamespaceIsScopedByModel() {
        assertNotEquals(ChatPipeline.cacheNamespace("tenant-a", "gpt-4o"),
                ChatPipeline.cacheNamespace("tenant-a", "gpt-4o-mini"));
    }

    @Test
    void cacheNamespaceIsScopedByTenant() {
        assertNotEquals(ChatPipeline.cacheNamespace("tenant-a", "gpt-4o"),
                ChatPipeline.cacheNamespace("tenant-b", "gpt-4o"));
    }

    @Test
    void cacheNamespaceIsStableForSameInputs() {
        assertEquals(ChatPipeline.cacheNamespace("tenant-a", "gpt-4o"),
                ChatPipeline.cacheNamespace("tenant-a", "gpt-4o"));
    }
}
