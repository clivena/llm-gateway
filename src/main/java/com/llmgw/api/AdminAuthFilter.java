package com.llmgw.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.config.GatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code /admin/**} 的访问控制。
 *
 * <h3>为什么不用 Spring Security</h3>
 * 这里要解决的问题只有一句："这个请求能不能读运维数据"。为此引入一整套
 * filter chain / AuthenticationManager / SecurityContext，代价是<b>多一套要解释的
 * 失败语义</b>——配错时表现可能是"整个应用 401"或"静默放行"，而不是一条明确的日志。
 * 一个 WebFilter 六十行就能把这件事说清楚。
 *
 * <h3>默认策略：没配 token 时只允许本机</h3>
 * 常见做法是"配了 token 才校验"，但它的失败形态最糟：忘配 = <b>全部租户的消费明细
 * 直接公开在公网上，且没有任何信号</b>。这里的默认是：
 * <ul>
 *   <li>配了 {@code gateway.admin.token} → 必须带 {@code X-Admin-Token} 且值相等；</li>
 *   <li>没配 → 只允许环回地址（127.0.0.1 / ::1），其余一律 403 并打一条 warn。</li>
 * </ul>
 * 本地开发、dashboard、真机脚本都走环回，零配置可用；一旦从外部访问就明确拒绝。
 *
 * <h3>只管 /admin，不管 /v1</h3>
 * 业务接口有自己的鉴权维度（apiKey + 配额），两件事不能混在一个 filter 里——
 * 混在一起之后，"这个 401 是因为没配额还是因为没运维权限"就说不清了。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 100)
public class AdminAuthFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(AdminAuthFilter.class);

    public static final String HEADER = "X-Admin-Token";

    private final GatewayProperties properties;
    private final ObjectMapper mapper;

    public AdminAuthFilter(GatewayProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!isAdminPath(path)) {
            return chain.filter(exchange);
        }

        String expected = properties.getAdmin().getToken();
        if (expected == null || expected.isBlank()) {
            if (isLoopback(exchange)) {
                return chain.filter(exchange);
            }
            log.warn("拒绝运维请求 {}：来源 {}，未配置 gateway.admin.token（只允许本机访问）",
                    path, remoteOf(exchange));
            return reject(exchange, HttpStatus.FORBIDDEN, "admin_token_not_configured",
                    "运维接口仅允许本机访问：请配置 gateway.admin.token，"
                            + "并在请求头 " + HEADER + " 中携带");
        }

        String given = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (given != null && constantTimeEquals(expected, given)) {
            return chain.filter(exchange);
        }
        // 不打印 token 的值，只说"缺了"还是"不对" —— 日志本身也可能被人看到。
        log.warn("拒绝运维请求 {}：来源 {}，{}", path, remoteOf(exchange),
                given == null ? "缺少 " + HEADER : HEADER + " 不匹配");
        return reject(exchange, HttpStatus.UNAUTHORIZED, "unauthorized",
                "缺少或错误的 " + HEADER);
    }

    /**
     * 只匹配 {@code /admin} 与 {@code /admin/...}。
     *
     * <p>不能用 {@code startsWith("/admin")}：那会把 {@code /administrator}、
     * {@code /admin-backup} 之类的路径一起拦下。它们是两个不同的路径，
     * 将来谁加了一个同前缀的公开端点，这个 bug 会以"莫名其妙的 401"出现。
     */
    private static boolean isAdminPath(String path) {
        return path.equals("/admin") || path.startsWith("/admin/");
    }

    /**
     * 拿不到来源地址时按"非本机"处理（fail-closed）。
     *
     * <p>反过来的写法（null 当本机）在测试里几乎不会暴露，在生产上却是一条
     * 直通口：任何让 remoteAddress 为空的链路（某些代理、Unix socket 转发）
     * 都会静默变成免鉴权。
     */
    private static boolean isLoopback(ServerWebExchange exchange) {
        InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
        if (remote == null || remote.getAddress() == null) {
            return false;
        }
        return remote.getAddress().isLoopbackAddress();
    }

    private static String remoteOf(ServerWebExchange exchange) {
        InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
        return remote == null ? "unknown" : String.valueOf(remote);
    }

    /**
     * 常量时间比较。
     *
     * <p>普通的 {@code equals} 在第一个不同字节处就返回，比较耗时与"猜中了多少位"
     * 成正比。对一个可以被无限次尝试的入口，这就够把 token 逐位试出来。
     * 这条在本地跑不出差别，属于"写对了没人知道、写错了也没人知道"的那类代码。
     */
    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private Mono<Void> reject(ServerWebExchange exchange, HttpStatus status,
                              String type, String message) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            return Mono.empty();
        }
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> error = new LinkedHashMap<>();
        error.put("message", message);
        error.put("type", type);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", error);

        byte[] bytes;
        try {
            bytes = mapper.writeValueAsBytes(body);
        } catch (Exception e) {
            // 兜底：错误响应绝不能因为序列化失败变成 500 —— 那会让人以为
            // "服务挂了"，而不是"你没权限"。
            bytes = ("{\"error\":{\"type\":\"" + type + "\"}}").getBytes(StandardCharsets.UTF_8);
        }
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}
