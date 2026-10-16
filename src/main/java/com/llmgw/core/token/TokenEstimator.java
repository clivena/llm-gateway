package com.llmgw.core.token;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.Usage;
import com.llmgw.config.GatewayProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Token 估算器 —— 配额预扣与兜底计费的统一口径。
 *
 * <h3>为什么要独立成组件（P5）</h3>
 * 旧实现是 `Math.max(1, question.length() / 4)`，两处独立的缺陷叠在一起：
 *
 * <ol>
 *   <li><b>公式是英文的经验公式。</b>「4 字符 = 1 token」对 ASCII 大致成立，
 *       对中文明显低估。</li>
 *   <li><b>只算了一条消息。</b>ChatPipeline 取的是「最后一条 user 消息」的文本长度，
 *       system prompt、few-shot 示例、多轮历史全部没进预算。</li>
 * </ol>
 *
 * <h3>真正的主导项：一次性模板开销（实测挖出来的）</h3>
 * 按上面两条改造完，估算还是不准 —— 因为缺了最大的一项。
 * 实测数据（deepseek-flash，2026-10-16）：
 * <pre>
 *   "Explain caching"（15 个 ASCII 字符）→ 上游回传 32 prompt_tokens
 * </pre>
 * 15 个字符无论怎么换算都到不了 32。<b>差额来自上游把请求套进自家对话模板的固定成本</b>：
 * BOS/EOS 标记、角色分隔符等，与内容无关、整个请求只付一次。
 * 用多组探针解方程后模型是：
 * <pre>
 *   tokens ≈ 28（一次性） + 1.2 × 消息数 + 0.54 × 汉字数 + 0.20 × 其他字符数
 * </pre>
 * 六组探针逐条回验，误差都在 1 token 内。这解释了为什么<b>越短的请求旧实现估得越离谱</b>：
 * 28 token 的固定开销对一条 5 字提问占 85%，对 2000 字长请求只占 2.5%。
 *
 * <p>因此估算公式是三段式：一次性开销 + 每条消息开销 + 按字符类别加权的内容量。
 * 少了第一段，短请求会被系统性地低估数倍。
 *
 * <h3>为什么系数是配置项而不是常量</h3>
 * token 数由<b>上游的分词器与模板</b>共同决定，两者都随供应商和模型版本变化
 * （同一句中英混合文本在不同词表下的压缩率可以差 2 倍）。把它写成常量，
 * 等于把某一次实测的结论固化成永久假设。所以系数走配置，并用
 * {@code scripts/calibrate-token-estimator.py} 实测校准 ——
 * 那个脚本是这套系数唯一的正当来源。
 *
 * <h3>偏差方向的选择：宁可高估</h3>
 * 估算有两个使用场景，而它们对偏差的容忍方向不同：
 * <ul>
 *   <li><b>配额预扣</b>：低估 = 允许超支（钱漏了），高估 = 提前拦截（用户抱怨）。
 *       两害相权，钱漏掉是不可逆的，所以这里偏保守（{@code safety-factor} 略大于 1）。</li>
 *   <li><b>兜底计费</b>：仅在拿不到上游真实 usage 时启用（正常路径用真实值）。
 *       高估会让账本金额偏大 —— 但同样地，少记钱比多记钱更难发现。</li>
 * </ul>
 * 这个取舍是刻意的：一旦引入「估算」，就必须选一个偏差方向，而不选方向等于默认低估。
 *
 * <p>本类无状态、无 IO，可安全并发调用。
 */
@Component
public class TokenEstimator {

    private final GatewayProperties.TokenEstimate config;

    public TokenEstimator(GatewayProperties properties) {
        this.config = properties.getTokenEstimate();
    }

    /**
     * 估算一次请求的 prompt 侧 token 数 —— <b>覆盖全部消息</b>，不只最后一条。
     *
     * @param messages 请求里的完整消息列表
     * @return 估算 token 数；空列表返回 0
     */
    public int estimatePrompt(List<ChatMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }
        // 一次性模板开销先计入：它由上游决定，与消息条数、内容长短都无关。
        // 实测（deepseek-flash）约 28 token —— 对一条 5 字提问占 85%，
        // 是短请求的主导项。漏掉它的后果是"越短的请求估得越离谱"。
        double weighted = config.getBaseOverhead();
        for (ChatMessage m : messages) {
            // 每条消息的额外结构开销（在一次性开销之上再付一次）
            weighted += config.getPerMessageOverhead();
            weighted += weightedChars(m.content());
            // role 字段本身也要占 token（"user" / "assistant" 会被分词）。
            weighted += weightedChars(m.role());
        }
        return applySafety(weighted);
    }

    /**
     * 估算单段文本的 token 数（用于 completion 侧与流式兜底）。
     * 不含消息结构开销 —— 输出侧没有 role 模板。
     */
    public int estimateText(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        return applySafety(weightedChars(text));
    }

    /**
     * prompt + completion 的合并估算。用作拿不到上游真实 usage 时的兜底，
     * 与 {@link com.llmgw.core.protocol.ProviderAdapter#estimateUsage} 同语义但口径更准。
     */
    public Usage estimateUsage(List<ChatMessage> messages, String answer) {
        return new Usage(estimatePrompt(messages), estimateText(answer));
    }

    /** 按字符类别加权求和。CJK 与 ASCII 的 token 密度差数倍，必须分开算。 */
    private double weightedChars(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        double sum = 0;
        int i = 0;
        int len = text.length();
        while (i < len) {
            int cp = text.codePointAt(i);
            // 用 codePoint 而不是 char：补充平面字符（emoji、部分扩展汉字）占两个
            // char，按 char 遍历会把它们数成两个字符，凭空放大估算值。
            sum += isCjkOrWide(cp) ? config.getCjkPerChar() : config.getAsciiPerChar();
            i += Character.charCount(cp);
        }
        return sum;
    }

    private int applySafety(double tokens) {
        double withFactor = tokens * config.getSafetyFactor();
        // 下限 1：一条非空请求最少也要花 1 token，返回 0 会让"配额已用 0"
        // 与"没有请求"无法区分，也让 ACCEPT 判断里的 used+need>limit 失去意义。
        return Math.max(1, (int) Math.ceil(withFactor));
    }

    /**
     * 是否按"CJK / 全角"的高密度档计算。
     *
     * 覆盖范围刻意比"汉字"宽：中日韩标点、全角字母数字、假名、谚文都是
     * 高密度 token 的来源。漏掉全角标点会让带中文标点的句子被低估 ——
     * 而中文句子几乎必然带中文标点，所以这不是边缘情况。
     */
    private static boolean isCjkOrWide(int cp) {
        return (cp >= 0x4E00 && cp <= 0x9FFF)     // CJK 统一表意文字
                || (cp >= 0x3400 && cp <= 0x4DBF) // 扩展 A
                || (cp >= 0xF900 && cp <= 0xFAFF) // 兼容表意文字
                || (cp >= 0x3000 && cp <= 0x303F) // CJK 标点（、。「」等）
                || (cp >= 0xFF00 && cp <= 0xFFEF) // 全角形式（，！？ＡＢ等）
                || (cp >= 0x3040 && cp <= 0x30FF) // 平假名 / 片假名
                || (cp >= 0xAC00 && cp <= 0xD7AF); // 谚文
    }
}
