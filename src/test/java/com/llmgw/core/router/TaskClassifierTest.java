package com.llmgw.core.router;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.token.TokenEstimator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 任务判定。
 *
 * <h3>这一组守着的坏法</h3>
 * 分类器的错误几乎都是<b>静默</b>的：判错了不会报错，只会让请求走了不该走的模型 ——
 * 要么贵模型被滥用（账单涨），要么便宜模型答不好（质量降）。两种情况都不会
 * 在日志里留下异常，只能靠"判定结果是否符合预期"的断言发现。
 *
 * <ol>
 *   <li><b>括号被当成代码</b>（{@link #bracketsAloneAreNotEnough}）：
 *       "这个函数 (a) 和 (b) 哪个先算" 是自然语言提问，不该走代码模型。
 *       这是把代码特征按"出现次数"统计时最典型的误判。</li>
 *   <li><b>提到 json 就抽取</b>（{@link #bareJsonMentionIsChat}）：
 *       "json 是什么" 是知识问答，不是格式抽取。</li>
 *   <li><b>判定抛异常把请求带崩</b>（{@link #classifierNeverThrows}）：
 *       网关自己的判断失误，不该变成用户的 500。</li>
 *   <li><b>推断不受 default-task 控制</b>（{@link #blankTaskFallsBackToInference}）：
 *       空白任务名必须当成"没声明"，否则客户端多打一个空格就会 400。</li>
 * </ol>
 */
class TaskClassifierTest {

    private final GatewayProperties props = new GatewayProperties();
    private final TaskClassifier classifier = new TaskClassifier(new TokenEstimator(props), props);

    // --- 显式声明 ---

    @Test
    void explicitTaskIsNormalisedAndWinsOverInference() {
        // 文本明显像代码，但客户端说的是 summarize —— 客户端最清楚自己在干什么。
        TaskDecision d = classifier.decide(request("CODE", "```java\nint a = 1;\n```"));

        assertEquals("code", d.task());
        assertEquals(TaskDecision.EXPLICIT, d.source());
    }

    @Test
    void blankTaskIsTreatedAsAbsent() {
        TaskDecision d = classifier.decide(request("   ", "```java\nint a = 1;\n```"));

        assertEquals("code", d.task());
        assertEquals(TaskDecision.INFERRED, d.source(),
                "空白任务名必须当成没声明，否则客户端多打一个空格就变成未知任务 400");
    }

    // --- 代码判定 ---

    @Test
    void codeFenceIsTheStrongestSignal() {
        TaskDecision d = classifier.decide(request(null, "看看这段：\n```\nfoo()\n```"));

        assertEquals("code", d.task());
        assertEquals("code-fence", d.reason());
    }

    @Test
    void bracketsAloneAreNotEnough() {
        // 只有一对括号 → 1 种特征，远不到 3 种。自然语言里的括号不该触发代码任务。
        TaskDecision d = classifier.decide(request(null, "这个函数 (a) 和 (b) 哪个先算"));

        assertEquals("chat", d.task(), "一对括号不是代码特征：" + d.reason());
        assertEquals(TaskDecision.FALLBACK, d.source());
    }

    @Test
    void threeMarkerKindsMakeItCode() {
        TaskDecision d = classifier.decide(request(null, "if (a) { return b; }"));

        assertEquals("code", d.task());
        assertEquals("code-symbols", d.reason());
    }

    @Test
    void codeKeywordIsAStrongSignal() {
        TaskDecision d = classifier.decide(request(null, "这段代码为什么报错"));

        assertEquals("code", d.task());
        assertEquals("code-keyword", d.reason());
    }

    // --- JSON 判定 ---

    @Test
    void jsonFenceBeatsPlainCodeFence() {
        // 围栏带语言标记时，客户端已经明说是 json —— 再判成"写代码"就是丢掉最硬的信号。
        TaskDecision d = classifier.decide(request(null, "按这个格式给我：\n```json\n{\"a\":1}\n```"));

        assertEquals("extract-json", d.task());
        assertEquals("json-fence", d.reason());
    }

    @Test
    void jsonNeedsAFormatHint() {
        TaskDecision d = classifier.decide(request(null, "把简历抽成字段，返回 json"));

        assertEquals("extract-json", d.task());
        assertEquals("json-output", d.reason());
    }

    @Test
    void bareJsonMentionIsChat() {
        // 光出现 json 不算 —— 这是个知识问答。
        TaskDecision d = classifier.decide(request(null, "json 是什么"));

        assertEquals("chat", d.task());
        assertEquals(TaskDecision.FALLBACK, d.source());
    }

    // --- 摘要判定 ---

    @Test
    void longInputIsSummarize() {
        // 中文按 0.512 token/字估（TokenEstimator 的实测系数），4500 字约 2300 token，
        // 超过 2000 的阈值。写 3000 字只有约 1600，会得到"没到阈值"的假失败。
        TaskDecision d = classifier.decide(request(null, "字".repeat(4500)));

        assertEquals("summarize", d.task());
        assertEquals("long-input", d.reason());
    }

    @Test
    void summaryKeywordIsSummarize() {
        TaskDecision d = classifier.decide(request(null, "帮我总结一下这段话"));

        assertEquals("summarize", d.task());
        assertEquals("summary-keyword", d.reason());
    }

    // --- 兜底与健壮性 ---

    @Test
    void onlySystemMessageFallsBack() {
        // system 是被测应用注入的模板，不代表用户意图 —— 不能拿它判任务。
        ChatRequest req = new ChatRequest("auto", false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("system", "你是一个严谨的技术助手")), null, null);

        TaskDecision d = classifier.decide(req);

        assertEquals("chat", d.task());
        assertEquals("empty-input", d.reason());
    }

    @Test
    void nullMessagesFallBackWithoutError() {
        ChatRequest req = new ChatRequest("auto", false, 0.0, 64, null, null, null, null, null, null, null);

        TaskDecision d = classifier.decide(req);

        assertEquals("chat", d.task());
        assertEquals(TaskDecision.FALLBACK, d.source());
    }

    @Test
    void lastUserMessageDecides() {
        // 多轮对话里已经换了话题：按最后一条判，不能被历史淹没。
        ChatRequest req = new ChatRequest("auto", false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", "```java\nint a = 1;\n```\n这段代码有错吗"),
                        new ChatMessage("assistant", "有一个问题……"),
                        new ChatMessage("user", "今天天气不错")), null, null);

        assertEquals("chat", classifier.decide(req).task());
    }

    @Test
    void classifierNeverThrows() {
        // 让长度估算炸掉：分类器必须自己咽下去并回落，而不是把异常抛给请求链路。
        TokenEstimator broken = mock(TokenEstimator.class);
        when(broken.estimatePrompt(any())).thenThrow(new IllegalStateException("boom"));
        TaskClassifier unsafe = new TaskClassifier(broken, props);

        TaskDecision d = unsafe.decide(request(null, "随便聊聊今天天气"));

        assertNotNull(d);
        assertEquals("chat", d.task());
        assertEquals("classifier-error", d.reason());
    }

    @Test
    void sameInputAlwaysYieldsSameDecision() {
        // 纪律 1：确定性。分类一旦随机，离线基线就不可复现。
        ChatRequest req = request(null, "if (a) { return b; }");

        assertEquals(classifier.decide(req), classifier.decide(req));
    }

    /**
     * classifier=off 关掉的是**推断**，不是任务路由本身。
     *
     * <p>这两个开关是独立的：enabled 管"要不要按任务选模型"，classifier 管
     * "要不要让网关猜"。混成一个，就会出现"想禁用推断结果把整个能力关了"。
     */
    @Test
    void classifierOffDisablesInferenceButKeepsExplicitTask() {
        props.getRouter().getTaskRouting().setClassifier("off");

        TaskDecision inferred = classifier.decide(request(null, "```java\nint a = 1;\n```"));
        assertEquals("chat", inferred.task());
        assertEquals("classifier-off", inferred.reason());

        TaskDecision explicit = classifier.decide(request("code", "随便写点什么"));
        assertEquals("code", explicit.task());
        assertEquals(TaskDecision.EXPLICIT, explicit.source());
    }

    // --- helpers ---

    private static ChatRequest request(String task, String userText) {
        return new ChatRequest("auto", false, 0.0, 64, null, null, null, null,
                List.of(new ChatMessage("user", userText)), null, task);
    }
}
