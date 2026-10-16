package com.llmgw.core.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F0：上游流式分片的解析。
 *
 * <h3>为什么这一条必须有单测</h3>
 * 这里曾经埋着一个 502：解析逻辑写在 {@code handle((json, sink) -> ...)} 里，
 * 而 {@code SynchronousSink} **每次调用只允许发一个元素**。
 * "用量分支"和"收尾分支"各自调一次 {@code sink.next()} —— 只要有一帧同时命中两者，
 * 整条流就抛 {@code Cannot emit more than one data}，客户端收到 502。
 *
 * <p>它之前一直是好的，纯属**数据形状的巧合**：普通增量帧没有 usage、
 * 用量帧没有 choices，两个分支从未在同一次调用里同时命中。
 * 直到 F0 加入收尾帧，而 DeepSeek 的末帧恰好**同时带 usage 与 finish_reason**
 * （include_usage=true 时）—— 巧合被打破，流式整条挂掉。
 *
 * <p>这类"靠巧合运行"的代码，靠真机跑一次是抓不住的（要恰好碰上那种帧）。
 * 所以把解析抽成 {@link OpenAiAdapter#parseChunk}，在这里用构造出来的
 * 原始 JSON 把每一种帧形状钉死。
 */
class OpenAiAdapterChunkTest {

    private static final String MODEL = "deepseek-flash";

    // --- 关键回归：一帧同时带 usage 与 finish_reason ---

    /**
     * 这是把流打成 502 的那个形状。它必须能同时产出**两个** StreamChunk，
     * 而不是抛异常。
     */
    @Test
    void lastFrameCarryingBothUsageAndFinishReasonYieldsTwoChunks() {
        String frame = "{"
                + "\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}],"
                + "\"usage\":{\"prompt_tokens\":33,\"completion_tokens\":64,\"total_tokens\":97}"
                + "}";

        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(frame, MODEL);

        assertEquals(2, chunks.size(),
                "末帧同时带 usage 与 finish_reason 时必须产出两个分片，不能只发一个");
        // 顺序：用量在前、收尾在后。客户端先拿到用量（若它要），再看到结束原因。
        assertTrue(chunks.get(0).usage() != null && !chunks.get(0).isFinish(),
                "第 1 个应是用量帧");
        assertTrue(chunks.get(1).isFinish(), "第 2 个应是收尾帧");
        assertEquals("length", chunks.get(1).finishReason());
    }

    @Test
    void parseChunkNeverThrowsOnThatShape() {
        // 上一条断言的是结果，这条断言的是"不炸"—— 因为原来的失败方式就是抛异常，
        // 而异常被上游的 onErrorResume 变成 502，从响应体上完全看不出是解析问题。
        String frame = "{\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}],"
                + "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}";

        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(frame, MODEL);
        assertFalse(chunks.isEmpty());
    }

    // --- 各种帧形状 ---

    @Test
    void plainDeltaFrameYieldsOneDeltaChunk() {
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[{\"index\":0,\"delta\":{\"content\":\"语义\"},\"finish_reason\":null}]}",
                MODEL);

        assertEquals(1, chunks.size());
        assertTrue(chunks.get(0).hasDelta());
        assertEquals("语义", chunks.get(0).delta());
        assertFalse(chunks.get(0).isFinish());
    }

    /** role-only 帧（流的第一个分片通常只有 delta.role）不该产出任何东西。 */
    @Test
    void roleOnlyFrameYieldsNothing() {
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}",
                MODEL);

        assertTrue(chunks.isEmpty());
    }

    /**
     * reasoning-only 帧：推理型模型的主战场。它**不该**被转发给客户端 ——
     * 网关对外的契约是 OpenAI 兼容，reasoning_content 不在该契约里；
     * 而且把它当正文转出去会让客户端把"思考过程"当成答案显示。
     * （F0 只透传 finish_reason，不改这条取舍。）
     */
    @Test
    void reasoningOnlyFrameYieldsNothing() {
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"我们需要回答用户\"},"
                        + "\"finish_reason\":null}]}",
                MODEL);

        assertTrue(chunks.isEmpty(), "reasoning 内容不转发给客户端");
    }

    @Test
    void pureUsageFrameYieldsOneUsageChunk() {
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":9}}", MODEL);

        assertEquals(1, chunks.size());
        assertTrue(chunks.get(0).usage() != null);
        assertEquals(7, chunks.get(0).usage().promptTokens());
        assertEquals(9, chunks.get(0).usage().completionTokens());
    }

    /** 末帧的 finish_reason 为空串等同于"没给"，不该产出收尾帧。 */
    @Test
    void blankFinishReasonYieldsNoFinishChunk() {
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":\"\"}]}",
                MODEL);

        assertEquals(1, chunks.size());
        assertTrue(chunks.get(0).hasDelta());
        assertFalse(chunks.get(0).isFinish());
    }

    @Test
    void malformedJsonYieldsNothingInsteadOfThrowing() {
        // 上游偶发会推来不完整/非 JSON 的注释帧。吞掉它继续读，
        // 比让整条流因为一行坏数据而 502 要好。
        assertTrue(OpenAiAdapter.parseChunk("not-json{{", MODEL).isEmpty());
    }

    @Test
    void emptyObjectYieldsNothing() {
        assertTrue(OpenAiAdapter.parseChunk("{}", MODEL).isEmpty());
    }

    @Test
    void deltaAndFinishInSameFrameYieldTwoChunks() {
        // 有些实现会在一帧里同时给正文和 finish_reason。
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[{\"index\":0,\"delta\":{\"content\":\"最后一段\"},\"finish_reason\":\"stop\"}]}",
                MODEL);

        assertEquals(2, chunks.size());
        assertEquals("最后一段", chunks.get(0).delta());
        assertEquals("stop", chunks.get(1).finishReason());
    }

    @Test
    void truncatedReasonIsCarriedVerbatim() {
        // 非流式那边曾有"硬编码 stop"的问题；流式这边同样要原样透传，
        // 尤其不能把 length 美化成 stop —— 那正是 F0 要修的静默失效。
        List<StreamChunk> chunks = OpenAiAdapter.parseChunk(
                "{\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}", MODEL);

        assertEquals("length", chunks.get(0).finishReason());
    }
}
