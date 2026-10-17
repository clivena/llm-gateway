package com.llmgw.observability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 凭证脱敏的边界。
 *
 * <p>这一组断言的重点不是"格式对不对"，而是<b>任何情况下都不能出现完整原文</b>。
 * 脱敏函数最危险的失败形态是"看起来遮了"：比如按固定下标切，
 * 结果短 key 被整体保留；或者 trim 之后长度判断用错了变量。
 */
class LogMaskTest {

    @Test
    void keepsOnlyTheEdgesOfALongSecret() {
        assertEquals("sk-1***cdef", LogMask.of("sk-1234567890abcdef"));
    }

    /** 长度不足 8 时保留首尾就等于保留全部，必须整体遮掉。 */
    @Test
    void shortSecretIsFullyMasked() {
        assertEquals("***", LogMask.of("sk-123"));
    }

    /** 边界值：正好 8 位。写成 {@code < 8} 时这条会露出 8 位原文。 */
    @Test
    void exactlyEightCharsIsFullyMasked() {
        assertEquals("***", LogMask.of("12345678"));
    }

    @Test
    void absentSecretIsExplicit() {
        assertEquals("(none)", LogMask.of(null));
        assertEquals("(none)", LogMask.of(""));
        assertEquals("(none)", LogMask.of("   "));
    }

    /**
     * 最重要的一条：结果里不能出现完整原文。
     *
     * <p>上一组断言已经隐含了它，但这里单独写 —— 将来有人为了"更好排障"
     * 把中段从 3 个星号改成留一半，这条会立刻失败。
     */
    @Test
    void maskedValueNeverContainsTheWholeSecret() {
        for (String secret : new String[]{
                "sk-1234567890abcdef", "a-very-long-token-value-here", "123456789"}) {
            String masked = LogMask.of(secret);
            assertFalse(masked.contains(secret), "脱敏后仍含完整原文：" + masked);
            assertTrue(masked.contains("***"), "必须真的遮了点什么：" + masked);
        }
    }

    /** 同一个 key 的多次脱敏结果必须一致，否则日志里没法把同一租户的行串起来。 */
    @Test
    void sameSecretAlwaysMasksToTheSameValue() {
        String a = LogMask.of("sk-abcdefghijklmnop");
        String b = LogMask.of("  sk-abcdefghijklmnop  ");
        assertEquals(a, b);
    }
}
