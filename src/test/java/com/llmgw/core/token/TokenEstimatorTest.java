package com.llmgw.core.token;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Token 估算（P5）的回归测试。
 *
 * <p>这组断言守的是两个具体的旧缺陷，两者都会让配额失真：
 * <ol>
 *   <li>{@code length/4} 是英文公式，中文被低估数倍（真机实测 6.43×）；</li>
 *   <li>只算最后一条 user 消息，system prompt 与多轮历史不计入预算。</li>
 * </ol>
 * 第二条尤其重要：它意味着<b>只把公式改对是不够的</b> —— 只要估算的输入
 * 仍然只有一条消息，多轮场景下配额依旧是错的。
 */
class TokenEstimatorTest {

    private static TokenEstimator estimator() {
        return new TokenEstimator(new GatewayProperties());
    }

    /**
     * 核心回归：中文不能按「4 字符 = 1 token」算。
     *
     * 用 {@code length / 2} 作为分界线而不是某个精确值：断言要能挡住
     * "退回英文公式"这类回归，但不该把某个系数值焊死在测试里 ——
     * 系数会随上游分词器变化而校准，测试跟着一起改就失去了守门意义。
     */
    @Test
    void chineseIsNotEstimatedByTheEnglishFormula() {
        String chinese = "请用一句话解释什么是语义缓存，不超过四十个字";
        int tokens = estimator().estimateText(chinese);

        int englishFormula = Math.max(1, chinese.length() / 4);
        assertTrue(tokens > chinese.length() / 2,
                "中文估算仍接近英文公式：tokens=" + tokens + "，旧公式=" + englishFormula);
    }

    /** 同样字符数下，中文必须比 ASCII 贵。这是"分语言计权"的直接体现。 */
    @Test
    void chineseCostsMoreThanAsciiOfSameLength() {
        int chinese = estimator().estimateText("一二三四五六七八九十");
        int ascii = estimator().estimateText("abcdefghij");

        assertTrue(chinese > ascii,
                "同等长度下中文应比 ASCII 贵：chinese=" + chinese + ", ascii=" + ascii);
    }

    /**
     * 核心回归：估算必须覆盖全部消息。
     *
     * 构造两组请求，最后一轮用户输入完全相同（"你好"），差异只在前面有没有历史。
     * 旧实现只取最后一条 user 消息 —— 这两组会被算成<b>一样大</b>。
     */
    @Test
    void promptEstimateCoversAllMessagesNotJustTheLastOne() {
        List<ChatMessage> onlyLastTurn = List.of(new ChatMessage("user", "你好"));

        List<ChatMessage> withHistory = List.of(
                new ChatMessage("system", "你是一个有帮助的助手，请用中文回答，回答尽量简短。"),
                new ChatMessage("user", "第一个问题：什么是语义缓存？"),
                new ChatMessage("assistant", "语义缓存是按问题含义检索历史答案的缓存。"),
                new ChatMessage("user", "你好"));

        int bare = estimator().estimatePrompt(onlyLastTurn);
        int withCtx = estimator().estimatePrompt(withHistory);

        // 断言用 2 倍而不是 3 倍：一次性模板开销（base-overhead）把"极短请求"的
        // 估算下限抬高了，所以带历史的相对差距看起来变小 —— 但绝对差距仍然显著。
        assertTrue(withCtx > bare * 2,
                "带历史的请求没有被算进去：bare=" + bare + ", withCtx=" + withCtx);
    }

    /**
     * 一次性模板开销：整个请求只付一次，<b>不是每条消息一次</b>。
     *
     * 这条守的是实测挖出来的那个建模缺口 —— "Explain caching" 只有 15 个 ASCII 字符，
     * 上游却回传 32 prompt_tokens，差额与内容无关。把它误当成"每条消息各付一次"，
     * 多轮对话就会被严重高估。
     */
    @Test
    void baseOverheadIsChargedOncePerRequestNotPerMessage() {
        GatewayProperties props = new GatewayProperties();
        props.getTokenEstimate().setBaseOverhead(100.0);
        props.getTokenEstimate().setPerMessageOverhead(0.0);
        props.getTokenEstimate().setCjkPerChar(0.0);
        props.getTokenEstimate().setAsciiPerChar(0.0);
        props.getTokenEstimate().setSafetyFactor(1.0);
        TokenEstimator estimator = new TokenEstimator(props);

        int one = estimator.estimatePrompt(List.of(new ChatMessage("user", "x")));
        int three = estimator.estimatePrompt(List.of(
                new ChatMessage("user", "x"),
                new ChatMessage("assistant", "y"),
                new ChatMessage("user", "z")));

        assertEquals(100, one, "只有一个固定开销");
        assertEquals(100, three, "固定开销与消息条数无关：三条消息也还是 100");
    }

    /** 与之对照：每条消息的那份开销，确实要按条数累加。 */
    @Test
    void perMessageOverheadScalesWithMessageCount() {
        GatewayProperties props = new GatewayProperties();
        props.getTokenEstimate().setBaseOverhead(0.0);
        props.getTokenEstimate().setPerMessageOverhead(10.0);
        props.getTokenEstimate().setCjkPerChar(0.0);
        props.getTokenEstimate().setAsciiPerChar(0.0);
        props.getTokenEstimate().setSafetyFactor(1.0);
        TokenEstimator estimator = new TokenEstimator(props);

        assertEquals(10, estimator.estimatePrompt(List.of(new ChatMessage("user", "x"))));
        assertEquals(30, estimator.estimatePrompt(List.of(
                new ChatMessage("user", "x"),
                new ChatMessage("assistant", "y"),
                new ChatMessage("user", "z"))));
    }

    /** 每条消息都要付一次结构开销（role 标记与模板分隔符）。 */
    @Test
    void perMessageOverheadIsChargedForEveryMessage() {
        List<ChatMessage> oneMessage = List.of(new ChatMessage("user", "你好你好"));
        List<ChatMessage> twoMessages = List.of(
                new ChatMessage("user", "你好"),
                new ChatMessage("user", "你好"));

        assertTrue(estimator().estimatePrompt(twoMessages)
                        > estimator().estimatePrompt(oneMessage),
                "把同样内容拆成两条消息应算出更多 token（每条都有固定的结构开销）");
    }

    /** 空输入返回 0，而不是被下限 1 抬起来 —— 否则"没有请求"也会占掉配额。 */
    @Test
    void emptyInputsEstimateToZero() {
        assertEquals(0, estimator().estimateText(""));
        assertEquals(0, estimator().estimateText(null));
        assertEquals(0, estimator().estimatePrompt(List.of()));
        assertEquals(0, estimator().estimatePrompt(null));
    }

    /** 非空输入至少有 1 token：返回 0 会让"已用 0"与"没有请求"无法区分。 */
    @Test
    void nonEmptyInputEstimatesAtLeastOne() {
        assertTrue(estimator().estimateText("a") >= 1);
    }

    /** 估算与实际使用同一套口径，且 prompt / completion 分开。 */
    @Test
    void usageSplitsPromptAndCompletion() {
        TokenEstimator estimator = estimator();
        List<ChatMessage> messages = List.of(new ChatMessage("user", "解释一下缓存"));

        Usage usage = estimator.estimateUsage(messages, "缓存用于复用结果。");

        assertEquals(estimator.estimatePrompt(messages), usage.promptTokens());
        assertEquals(estimator.estimateText("缓存用于复用结果。"), usage.completionTokens());
    }

    /** 系数可配置 —— 这正是"实测校准 → 回填配置"这条路的前提。 */
    @Test
    void coefficientsAreConfigurable() {
        GatewayProperties tight = new GatewayProperties();
        tight.getTokenEstimate().setCjkPerChar(1.0);
        GatewayProperties loose = new GatewayProperties();
        loose.getTokenEstimate().setCjkPerChar(2.0);

        int tightTokens = new TokenEstimator(tight).estimateText("一二三四五六七八九十");
        int looseTokens = new TokenEstimator(loose).estimateText("一二三四五六七八九十");

        assertTrue(looseTokens > tightTokens, "改 cjk-per-char 应当改变估算结果");
    }

    /**
     * 补充平面字符（emoji / 扩展汉字）不能被数成两个字符。
     *
     * 用 10 个字符而不是 1 个来断言：单个字符会被"下限 1 token"掩盖差异，
     * 让"按 char 遍历两遍"这个 bug 测不出来。
     */
    @Test
    void supplementaryPlaneCharactersAreCountedOnce() {
        // U+2000B 属于补充平面，在 String 里占两个 char
        String supplementary = "\uD840\uDC0B".repeat(10);
        String ascii = "a".repeat(10);

        assertEquals(estimator().estimateText(ascii), estimator().estimateText(supplementary),
                "补充平面字符应按 codePoint 计一次；按 char 计会算成两倍");
    }
}
