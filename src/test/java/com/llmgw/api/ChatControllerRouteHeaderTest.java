package com.llmgw.api;

import com.llmgw.api.dto.ChatMessage;
import com.llmgw.api.dto.ChatRequest;
import com.llmgw.api.dto.ChatResponse;
import com.llmgw.api.dto.Usage;
import com.llmgw.core.pipeline.ChatPipeline;
import com.llmgw.core.protocol.StreamChunk;
import com.llmgw.core.router.ModelTier;
import com.llmgw.core.router.Route;
import com.llmgw.core.router.RouteTrace;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 响应头 {@code X-Route-Task} / {@code X-Route-Source} 的接线。
 *
 * <h3>为什么用 {@code MockServerWebExchange} 而不是 WebTestClient</h3>
 * 这里要验的不是路由逻辑（那是 {@code RouterServiceTest} 的事），而是
 * "判定结果有没有原样到达 HTTP 层"。WebTestClient 会顺带启动一整套
 * 编解码与异常处理，失败时很难判断是头没设还是被别的层覆盖了 ——
 * 直接调控制器方法，断言的就是那两个 {@code getHeaders().set(...)}。
 *
 * <h3>被钉住的三个容易退化的点</h3>
 * <ol>
 *   <li><b>流式也要有</b>：流式的头必须在 {@code writeAndFlushWith} 之前设好，
 *       之后就没有第二次机会了。</li>
 *   <li><b>路由失败时不能设假值</b>：宁可没有这个头，也不要写一个 n/a 让人
 *       以为"任务路由没开"。</li>
 *   <li><b>非 ASCII 不能进头</b>：诊断信息没有让请求变 500 的权力。</li>
 * </ol>
 */
class ChatControllerRouteHeaderTest {

    private final ChatPipeline pipeline = mock(ChatPipeline.class);
    private final ChatController controller = new ChatController(pipeline);

    @Test
    void nonStreamCarriesRouteHeaders() {
        when(pipeline.execute(any(), any(), any(), any())).thenAnswer(inv -> {
            RouteTrace trace = inv.getArgument(3);
            trace.record(new Route(new ModelTier("deepseek-flash", "CHEAP", 0.0015, 0.72),
                    "code", "inferred", "code-marker"));
            return Mono.just(answer());
        });

        MockServerWebExchange exchange = exchange();
        controller.chat("tenant-a", request(false), exchange, exchange.getResponse()).block();

        assertEquals("code", exchange.getResponse().getHeaders()
                .getFirst(OpenAiProtocol.ROUTE_TASK_HEADER));
        assertEquals("inferred", exchange.getResponse().getHeaders()
                .getFirst(OpenAiProtocol.ROUTE_SOURCE_HEADER));
    }

    @Test
    void streamCarriesRouteHeaders() {
        when(pipeline.executeStream(any(), any(), any(), any())).thenAnswer(inv -> {
            RouteTrace trace = inv.getArgument(3);
            trace.record(new Route(new ModelTier("deepseek-pro", "STRONG", 0.008, 0.9),
                    "summarize", "explicit", "client-declared-task"));
            return Flux.just(StreamChunk.delta("deepseek-pro", "摘要如下"),
                    StreamChunk.finish("deepseek-pro", "stop"));
        });

        MockServerWebExchange exchange = exchange();
        controller.chat("tenant-a", request(true), exchange, exchange.getResponse()).block();

        assertEquals("summarize", exchange.getResponse().getHeaders()
                .getFirst(OpenAiProtocol.ROUTE_TASK_HEADER));
        assertEquals("explicit", exchange.getResponse().getHeaders()
                .getFirst(OpenAiProtocol.ROUTE_SOURCE_HEADER));
        // 顺带确认流本身没被这次改动弄坏：收尾帧仍在。
        assertTrue(exchange.getResponse().getBodyAsString().block().contains("data: [DONE]"));
    }

    /**
     * 路由没解析成功（参数错 / 未知任务）时，头必须缺席。
     *
     * <p>这是最能说明"头是诊断信息、不是协议字段"的一条：请求本来就失败了，
     * 网关不应该再编一个任务名出来。空着 = "这次没有路由结论"，客户端一看就懂。
     */
    @Test
    void failedRoutingLeavesHeadersAbsent() {
        when(pipeline.execute(any(), any(), any(), any()))
                .thenReturn(Mono.error(new IllegalArgumentException("未知任务 [nope]")));

        MockServerWebExchange exchange = exchange();
        controller.chat("tenant-a", request(false), exchange, exchange.getResponse()).block();

        assertEquals(HttpStatus.BAD_REQUEST, exchange.getResponse().getStatusCode());
        assertNull(exchange.getResponse().getHeaders().getFirst(OpenAiProtocol.ROUTE_TASK_HEADER));
        assertNull(exchange.getResponse().getHeaders().getFirst(OpenAiProtocol.ROUTE_SOURCE_HEADER));
    }

    /**
     * 非 ASCII 任务名被降级成 {@code _}，而不是让 Netty 抛异常。
     *
     * <p>约定上任务名是 ASCII 标识符，但配置是用户可写的 ——
     * 有人写 {@code tasks: {代码: ...}} 完全可能。那种情况下请求必须照常成功，
     * 代价只是这个头看不出原名（原名在日志与 {@code /admin/route} 里都是原文）。
     */
    @Test
    void nonAsciiTaskNameIsSanitisedInsteadOfBreakingTheResponse() {
        when(pipeline.execute(any(), any(), any(), any())).thenAnswer(inv -> {
            RouteTrace trace = inv.getArgument(3);
            trace.record(new Route(new ModelTier("deepseek-flash", "CHEAP", 0.0015, 0.72),
                    "代码", "inferred", "code-marker"));
            return Mono.just(answer());
        });

        MockServerWebExchange exchange = exchange();
        controller.chat("tenant-a", request(false), exchange, exchange.getResponse()).block();

        assertEquals(HttpStatus.OK, exchange.getResponse().getStatusCode());
        assertEquals("__", exchange.getResponse().getHeaders()
                .getFirst(OpenAiProtocol.ROUTE_TASK_HEADER));
    }

    private static MockServerWebExchange exchange() {
        return MockServerWebExchange.from(
                MockServerHttpRequest.post("/v1/chat/completions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("{}"));
    }

    private static ChatRequest request(boolean stream) {
        return new ChatRequest("auto", stream, 0.0, 256, null, null, null, null,
                List.of(new ChatMessage("user", "把这段代码重构成更清晰的写法")), null, null);
    }

    private static ChatResponse answer() {
        return new ChatResponse("id-1", "deepseek-flash", "好的", new Usage(10, 5), false, "stop");
    }
}
