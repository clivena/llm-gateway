package com.llmgw.core.router;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.token.TokenEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

/**
 * 任务判定：回答"这一条请求需要什么能力"。
 *
 * <p>三级优先，逐级兜底：
 * <pre>
 *   显式 task（客户端声明，最准）
 *     ↓ 没有
 *   规则推断（零延迟、零成本、确定性）
 *     ↓ 判不出
 *   default-task
 * </pre>
 *
 * <h3>为什么在线链路不用模型做分类</h3>
 * 每个请求先发一次分类调用，等于给全部流量加一笔固定税：多一次往返延迟、
 * 多一份费用，还多一个可能失败的点。更麻烦的是它<b>不可静态解释</b> ——
 * 线上出现"这个请求为什么走了贵模型"，规则版能指着代码说清命中了哪一条，
 * 模型版只能说"它当时觉得像"。模型分类适合离线标注与抽样校准，
 * 不适合放在请求路径上。
 *
 * <h3>三条纪律（都有对应测试）</h3>
 * <ol>
 *   <li><b>确定性</b>：同样的输入永远同样的输出，否则离线基线不可复现。</li>
 *   <li><b>永不抛异常</b>：分类失败是网关自己的问题，不能让用户的请求失败。
 *       任何运行时异常都回落 default-task 并打 warn。</li>
 *   <li><b>只看最后一条 user 消息</b>，不读全量历史 —— 否则 100 轮对话的
 *       分类开销不再是 O(1)。system 消息也刻意不看：那通常是被测应用注入的
 *       提示词模板，代表"它希望模型怎么答"，不代表"用户想做什么"。</li>
 * </ol>
 */
@Component
public class TaskClassifier {

    private static final Logger log = LoggerFactory.getLogger(TaskClassifier.class);

    /** 超过这个量视为长文，默认归到摘要类任务。2000 是经验值：再长的输入基本都在做提炼。 */
    private static final int LONG_INPUT_TOKENS = 2000;

    /**
     * 代码特征的<b>分组</b>。命中组数 ≥ {@link #CODE_MARKER_KINDS} 才算像代码。
     *
     * <p>为什么分组而不是逐个统计：{@code (a) (b)} 这种写法在自然语言里很常见，
     * 若把 {@code (} 和 {@code )} 各算一种特征，两个括号就能贡献 2 分，一段
     * 带括号的正常提问会被误判成代码。分组之后"一对括号"只算 1 种，
     * 要凑够 3 种就必须同时出现分号、大括号、赋值或关键字这类真正属于代码的东西。
     *
     * <p>为什么用"种类"而不是"出现次数"：代码会重复用同一个符号（十个分号），
     * 自然语言也可能重复（十个逗号）。次数没有区分度，种类有。
     */
    private static final String[][] CODE_MARKER_GROUPS = {
            {";"},
            {"{", "}"},
            {"(", ")"},
            {"->", "=>"},
            {"def ", "class ", "import ", "package "},
            {"function ", "public ", "private ", "return ", "const ", "var "},
            {"select ", "insert into", "update ", "delete from", "where "},
    };

    private static final int CODE_MARKER_KINDS = 3;

    /**
     * 强特征关键词。命中一个就算 —— 这些词在非技术语境里几乎不出现，
     * 所以单个词的信噪比已经够高，不需要再叠加条件。
     */
    private static final String[] CODE_KEYWORDS = {
            "重构", "编译", "报错", "堆栈", "stack trace", "stacktrace",
            "单元测试", "断点", "调试", "这段代码", "这段程序", "源码",
            "空指针", "异常栈", "正则表达式", "sql 语句",
    };

    /** JSON 任务要求 <b>json</b> + 一个"在说输出格式"的提示词，两个条件同时满足才算。 */
    private static final String[] JSON_FORMAT_HINTS = {
            "返回", "输出", "格式", "schema", "结构", "字段", "抽取", "解析",
    };

    private static final String[] SUMMARY_KEYWORDS = {
            "总结", "摘要", "提炼", "概括", "归纳", "summarize", "summary",
    };

    private final TokenEstimator tokens;
    private final GatewayProperties properties;

    public TaskClassifier(TokenEstimator tokens, GatewayProperties properties) {
        this.tokens = tokens;
        this.properties = properties;
    }

    public TaskDecision decide(ChatRequest request) {
        String defaultTask = defaultTask();
        try {
            String explicit = request.resolvedTask();
            if (explicit != null) {
                return TaskDecision.explicit(explicit);
            }
            if (classifierOff()) {
                // classifier=off：只认客户端声明的任务，网关不猜。
                // 这不是"关闭任务路由"，而是"关闭推断"—— 显式 task 仍然生效，
                // 两者是独立开关（enabled 管整体、classifier 管推断）。
                return TaskDecision.fallback(defaultTask, "classifier-off");
            }
            return infer(request, defaultTask);
        } catch (RuntimeException e) {
            // 纪律 2：分类失败只降级，不传播。这里捕获 RuntimeException 而不是
            // Throwable —— Error（OOM 等）应当照常上抛，吞掉它会掩盖真实故障。
            log.warn("任务推断失败，回落 default-task [{}]：{}", defaultTask, e.toString());
            return TaskDecision.fallback(defaultTask, "classifier-error");
        }
    }

    private TaskDecision infer(ChatRequest request, String defaultTask) {
        String text = lastUserMessage(request);
        if (text.isBlank()) {
            return TaskDecision.fallback(defaultTask, "empty-input");
        }
        // 归一化一次，后面所有规则都用同一份小写文本，避免大小写各判一遍。
        String lower = text.toLowerCase(Locale.ROOT);

        // 顺序有讲究：输出格式约束 > 输入内容类型 > 输入长度。
        //
        // 但"格式约束优先"有个必须处理的例外：贴了一段 ```json 的请求，
        // 既像代码（有围栏）又像 JSON（在说格式）。这时以围栏的语言标记为准 ——
        // 客户端已经明说是 json，再判成"写代码"就是把最硬的信号扔掉了。
        // 反过来，"把这段 Python 代码整理成 JSON" 只含裸围栏，应判 code：
        // 它真正需要的能力是读懂代码，而不是套一个输出模板。
        if (lower.contains("```json")) {
            return TaskDecision.inferred("extract-json", "json-fence");
        }
        if (looksLikeCode(lower)) {
            return TaskDecision.inferred("code", codeReason(lower));
        }
        if (looksLikeJsonOutput(lower)) {
            return TaskDecision.inferred("extract-json", "json-output");
        }
        if (isLongInput(request)) {
            return TaskDecision.inferred("summarize", "long-input");
        }
        if (containsAny(lower, SUMMARY_KEYWORDS)) {
            return TaskDecision.inferred("summarize", "summary-keyword");
        }
        return TaskDecision.fallback(defaultTask, "no-rule-matched");
    }

    private static boolean looksLikeJsonOutput(String lower) {
        // 直接给出 JSON 模板（{" 或 {\n）：这个信号比任何关键词都硬。
        if (lower.contains("{\"") || lower.contains("{\n")) {
            return true;
        }
        // 光说 json 不够 —— "json 是什么" 是个普通提问，不该走抽取任务。
        return lower.contains("json") && containsAny(lower, JSON_FORMAT_HINTS);
    }

    private static boolean looksLikeCode(String lower) {
        // 围栏是最强的信号：客户端明确在贴代码。
        if (lower.contains("```")) {
            return true;
        }
        int kinds = 0;
        for (String[] group : CODE_MARKER_GROUPS) {
            if (containsAny(lower, group)) {
                kinds++;
            }
        }
        return kinds >= CODE_MARKER_KINDS || containsAny(lower, CODE_KEYWORDS);
    }

    private static String codeReason(String lower) {
        if (lower.contains("```")) {
            return "code-fence";
        }
        return containsAny(lower, CODE_KEYWORDS) ? "code-keyword" : "code-symbols";
    }

    private boolean isLongInput(ChatRequest request) {
        return tokens.estimatePrompt(request.messages()) >= LONG_INPUT_TOKENS;
    }

    /**
     * 最后一条 user 消息。刻意跳过 system（见类注释）与 assistant（那是历史回答）。
     *
     * <p>为什么取"最后一条"而不是拼接全部：多轮对话里前几轮可能已经换了话题，
     * 拼接会让历史把当前意图淹没，还会让成本随轮数增长。
     */
    private static String lastUserMessage(ChatRequest request) {
        List<ChatMessage> messages = request.messages();
        if (messages == null || messages.isEmpty()) {
            return "";
        }
        for (int i = messages.size() - 1; i >= 0; i--) {
            ChatMessage m = messages.get(i);
            if (m != null && "user".equalsIgnoreCase(m.role())
                    && m.content() != null && !m.content().isBlank()) {
                return m.content();
            }
        }
        return "";
    }

    private static boolean containsAny(String haystack, String[] needles) {
        for (String needle : needles) {
            if (haystack.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private String defaultTask() {
        String configured = properties.getRouter().getTaskRouting().getDefaultTask();
        return (configured == null || configured.isBlank()) ? "chat" : configured;
    }

    private boolean classifierOff() {
        return "off".equalsIgnoreCase(properties.getRouter().getTaskRouting().getClassifier());
    }
}
