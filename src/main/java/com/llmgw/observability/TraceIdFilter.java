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
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdFilter implements WebFilter {

    public static final String HEADER = "X-Trace-Id";

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
        return chain.filter(exchange);
    }
}
