package com.llmgw.core.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * L1 精确缓存的 key 规范化（P4）。
 *
 * <p>为什么要专门测规范化：它是唯一一个"改错了不会报错、只会悄悄返回错答案"的地方。
 * 每放宽一步规范化，命中率上升、同时错命中概率也上升 —— 而缓存最坏的失败模式
 * 恰恰是<b>错答案</b>（漏命中只是没省钱）。所以这里既断言"该归一的归一了"，
 * 也断言"不该归一的没有归一"。
 */
class ExactCacheLayerTest {

    // --- 应该被归一的差异 ---

    @Test
    void whitespaceDifferencesCollapse() {
        assertEquals(ExactCacheLayer.normalize("  你好  世界  "),
                ExactCacheLayer.normalize("你好 世界"));
        assertEquals(ExactCacheLayer.normalize("你好\n世界"),
                ExactCacheLayer.normalize("你好  世界"));
    }

    @Test
    void asciiCaseIsIgnored() {
        assertEquals(ExactCacheLayer.normalize("What Is JVM"),
                ExactCacheLayer.normalize("what is jvm"));
    }

    // --- 不应该被归一的差异（守"不误命中"这一侧） ---

    /**
     * 刻意不剥标点："这是对的" 与 "这是对的？" 是两个问题。
     * 语义相近是 L2 的职责，而 L2 有相似度阈值兜着 —— 把模糊匹配塞进精确层，
     * 等于把两层合并成一层却丢掉了阈值保护。
     */
    @Test
    void punctuationIsDeliberatelyNotStripped() {
        assertNotEquals(ExactCacheLayer.normalize("这是对的"),
                ExactCacheLayer.normalize("这是对的？"));
    }

    @Test
    void differentWordsStayDifferent() {
        assertNotEquals(ExactCacheLayer.normalize("什么是缓存"),
                ExactCacheLayer.normalize("什么是缓存失效"));
    }

    // --- key 的性质 ---

    @Test
    void keyIsScopedByNamespace() {
        assertNotEquals(ExactCacheLayer.key("tenant-a:gpt-4o", "问题"),
                ExactCacheLayer.key("tenant-b:gpt-4o", "问题"));
        assertNotEquals(ExactCacheLayer.key("tenant-a:gpt-4o", "问题"),
                ExactCacheLayer.key("tenant-a:gpt-4o-mini", "问题"));
    }

    @Test
    void keyIsStableForEquivalentQuestions() {
        assertEquals(ExactCacheLayer.key("ns", "  什么是 缓存？ "),
                ExactCacheLayer.key("ns", "什么是 缓存？"));
    }

    /** 与 L2 的 {@code sem:} 前缀必须区分开，否则两层会互相覆盖。 */
    @Test
    void keyUsesItsOwnPrefixAndIsHashed() {
        String key = ExactCacheLayer.key("ns", "你好");

        assertTrue(key.startsWith("ex:ns:"), key);
        assertFalse(key.startsWith("sem:"), "L1 不能与 L2 共用前缀：" + key);
        // 原文不应出现在 key 里（长 prompt 会把 key 撑大，且特殊字符会破坏结构）
        assertFalse(key.contains("你好"), key);
    }

    @Test
    void keyHasFixedLengthRegardlessOfQuestionLength() {
        String shortKey = ExactCacheLayer.key("ns", "短");
        String longKey = ExactCacheLayer.key("ns", "长".repeat(2000));

        // 64 位 SHA-256 十六进制 —— 差一个字符说明用了截断或拼了原文
        assertEquals(shortKey.length(), longKey.length());
    }

    /** null 不能抛异常（调用方在缓存未启用时可能传空）。 */
    @Test
    void normalizeHandlesNull() {
        assertEquals("", ExactCacheLayer.normalize(null));
    }
}
