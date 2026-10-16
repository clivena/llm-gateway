package com.llmgw.core.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F0：上游响应可用性判定。
 *
 * 这组测试守的是一个**把"失败"与"成功"分开**的谓词。它的实现只有两行，
 * 但它的边界决定了后面两件事的正确性：
 * ① 指标怎么计数（empty 是故障信号、truncated 是用量信号，处置动作相反）；
 * ② F1/F2 要不要重试（换一家重试只有对 empty 才有意义）。
 *
 * 所以边界必须一条条钉死，不能靠"看起来对"。
 */
class UpstreamOutcomeTest {

    // --- 四种组合的完整覆盖 ---

    @Test
    void normalResponseIsNeitherEmptyNorTruncated() {
        UpstreamOutcome o = UpstreamOutcome.of("语义缓存是一种...", "stop");

        assertFalse(o.empty());
        assertFalse(o.truncated());
        assertFalse(o.budgetTooSmall());
        assertEquals("正常", o.describe());
    }

    /**
     * 推理型模型的典型失败形态：预算被 reasoning 吃光。
     * 这是 F0 存在的**唯一理由** —— 实测 flash 在 max_tokens=1024 时
     * completion_tokens=1024、reasoning_tokens=1024、content 为空。
     */
    @Test
    void emptyWithLengthIsTheReasoningModelFailureShape() {
        UpstreamOutcome o = UpstreamOutcome.of("", "length");

        assertTrue(o.empty(), "空正文 = 客户端什么也没拿到");
        assertTrue(o.truncated(), "finish_reason=length 说明是预算耗尽，不是模型主动收尾");
        assertFalse(o.budgetTooSmall(), "这一类的成因是「推理吃光预算」，不是客户端 max_tokens 小");
    }

    /**
     * 有正文但被截断 —— 与上一类**必须分开**。
     *
     * 这一类是客户端把 max_tokens 设小了，答案本身可信、值得原样返回；
     * 重试一次毫无意义（预算不变就必然再截断一次）。
     * 而 empty 那一类恰恰相反：推理 token 数实测高度不确定，换一次就可能正常出正文。
     */
    @Test
    void truncatedWithContentIsClientBudgetProblemNotUpstreamFailure() {
        UpstreamOutcome o = UpstreamOutcome.of("语义缓存是一种基于语义相似度的", "length");

        assertFalse(o.empty());
        assertTrue(o.truncated());
        assertTrue(o.budgetTooSmall(), "有正文 + length = 客户端的 max_tokens 偏小");
    }

    /**
     * 空正文但 finish_reason=stop：模型主动说"我说完了，内容是空的"。
     * 罕见，但同样不可用 —— 所以 failure 谓词用的是 empty 而不是 truncated。
     * 这一条正是"不能把谓词写成 truncated"的原因。
     */
    @Test
    void emptyWithStopIsStillUnusable() {
        UpstreamOutcome o = UpstreamOutcome.of("", "stop");

        assertTrue(o.empty(), "finish_reason=stop 不能把一个空答案洗成可用");
        assertFalse(o.truncated());
    }

    // --- 空白与 null 的边界 ---

    @Test
    void nullContentCountsAsEmpty() {
        // 上游省略 content 字段时，Jackson 的 asText("") 会给空串，
        // 但适配器层也可能真的传来 null。两条路径都必须判成 empty。
        assertTrue(UpstreamOutcome.of(null, "stop").empty());
    }

    @Test
    void whitespaceOnlyContentCountsAsEmpty() {
        // 只有换行/空格等于什么都没说。用 isBlank 而不是 isEmpty，
        // 就是为了这一条 —— "\n\n" 的 isEmpty() 是 false，客户端拿到的却是一个空答案。
        assertTrue(UpstreamOutcome.of("   \n\t ", "length").empty());
    }

    @Test
    void missingFinishReasonIsNotTruncated() {
        // 上游没给 finish_reason（有些兼容实现不返回该字段）。
        // 不能因此就判定"被截断" —— 那会让 truncated 指标全是噪音。
        UpstreamOutcome o = UpstreamOutcome.of("答案", null);

        assertFalse(o.truncated());
        assertFalse(o.empty());
    }

    // --- describe 的输出被日志直接使用 ---

    @Test
    void describeDistinguishesTheThreeFailureShapes() {
        // describe() 是进日志的，三类必须能一眼分开 ——
        // 否则查日志时看到三行一样的字，等于没有信息。
        String a = UpstreamOutcome.of("", "length").describe();
        String b = UpstreamOutcome.of("", "stop").describe();
        String c = UpstreamOutcome.of("半句", "length").describe();

        assertFalse(a.equals(b), "「空正文且被截断」与「空正文」不能是同一句话");
        assertFalse(b.equals(c), "「空正文」与「有正文但被截断」不能是同一句话");
        assertFalse(a.equals(c));
    }

    @Test
    void finishReasonIsCarriedVerbatim() {
        // 原样保留：它是透传给客户端的东西，不能在判定层被改写。
        assertEquals("length", UpstreamOutcome.of("", "length").finishReason());
        assertEquals("content_filter", UpstreamOutcome.of("", "content_filter").finishReason());
    }
}
