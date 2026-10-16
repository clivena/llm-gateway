package com.llmgw.observability;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * traceId 生成/透传：与旧项目的 Zipkin 思路一致，用于跨服务与账本对账。
 *
 * <p>traceId 要同时送到三个地方，少一个就有盲区（P7 补全）：
 * <ol>
 *   <li><b>响应头</b> —— 调用方拿它去账本对账；</li>
 *   <li><b>exchange attributes</b> —— 能拿到 exchange 的代码（Controller/Filter）可读；</li>
 *   <li><b>Reactor Context</b> —— 这一条以前缺了。内部组件（ChatPipeline /
 *       ProviderAdapter / 缓存层）手里<b>没有 exchange</b>，它们要打日志就只能靠
 *       {@link MdcContextLifter} 从 Context 同步到 MDC。少了这一步，
 *       "一次请求的多行日志串起来"就只是句口号。</li>
 * </ol>
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdFilter implements WebFilter {

    public static final String HEADER = "X-Trace-Id";

    /** Reactor Context 里的 key，同时也是 MDC 里的 key。 */
    public static final String CONTEXT_KEY = "traceId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String traceId = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }
        // 无论 traceId 是上游透传还是本地生成，都要回写响应头 ——
        // 否则调用方拿不到它，就无法用同一个 id 去账本里对账。
        exchange.getResponse().getHeaders().set(HEADER, traceId);
        exchange.getAttributes().put(HEADER, traceId);

        final String tid = traceId;
        return chain.filter(exchange)
                .contextWrite(ctx -> ctx.put(CONTEXT_KEY, tid));
    }
}
