package com.llmgw.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgw.config.GatewayProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code /admin/**} 的访问控制。
 *
 * <h3>为什么要钉死"拿不到来源地址"这一条</h3>
 * 这是整组断言里唯一一条<b>不可能在真机上复现</b>的：反代的某些配置、
 * Unix socket 转发、以及未来换成别的运行时，都可能让 remoteAddress 为空。
 * 如果实现写成"null 当作本机"，它在本地测试里看起来完美，在生产上就是一条
 * 免鉴权直通口。所以这里用 mock 精确构造 null，把失败形态钉住。
 *
 * <h3>被钉住的四个边界</h3>
 * <ol>
 *   <li>没配 token 时，环回放行、外部拒绝（不是"没配就全放"）；</li>
 *   <li>配了 token 时，环回也必须带 token（否则"配了等于没配"）；</li>
 *   <li>{@code /administrator} 不是 {@code /admin} 的子路径，不能被误拦；</li>
 *   <li>业务接口 {@code /v1/**} 不受影响 —— 否则一次运维加固会把所有用户挡在门外。</li>
 * </ol>
 */
class AdminAuthFilterTest {

    private GatewayProperties props;
    private AdminAuthFilter filter;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        filter = new AdminAuthFilter(props, new ObjectMapper());
    }

    /** 记录 chain 是否被调用：这是"放行还是拦截"唯一可靠的判据。 */
    private static final class Chain implements WebFilterChain {
        final AtomicBoolean called = new AtomicBoolean(false);

        @Override
        public Mono<Void> filter(org.springframework.web.server.ServerWebExchange exchange) {
            called.set(true);
            return Mono.empty();
        }
    }

    private MockServerWebExchange exchange(String path, String remoteIp, String token) {
        MockServerHttpRequest.BaseBuilder<?> builder = MockServerHttpRequest.get(path);
        if (remoteIp != null) {
            builder = builder.remoteAddress(new InetSocketAddress(remoteIp, 51234));
        }
        if (token != null) {
            builder = builder.header(AdminAuthFilter.HEADER, token);
        }
        return MockServerWebExchange.from(builder.build());
    }

    private String body(MockServerWebExchange ex) {
        return ex.getResponse().getBodyAsString().block();
    }

    @Test
    void withoutTokenLoopbackIsAllowed() {
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/admin/status", "127.0.0.1", null);

        filter.filter(ex, chain).block();

        assertTrue(chain.called.get(), "本机访问不该被拦");
        assertNull(ex.getResponse().getStatusCode(), "不该写任何状态码");
    }

    @Test
    void withoutTokenExternalAddressIsForbidden() {
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/admin/ledger/recent", "10.1.2.3", null);

        filter.filter(ex, chain).block();

        assertFalse(chain.called.get(), "未配 token 时外部地址必须被拒");
        assertEquals(HttpStatus.FORBIDDEN, ex.getResponse().getStatusCode());
        assertTrue(body(ex).contains("admin_token_not_configured"),
                "错误体要能让人看出是'没配 token'而不是'token 错了'，实际：" + body(ex));
    }

    /**
     * 拿不到来源地址时按"非本机"处理。
     *
     * <p>这条断言的价值不在当前行为，而在它挡住的那个改写：把
     * {@code remote == null} 判成本机，本地测试全绿、生产上直通。
     */
    @Test
    void withoutTokenAndUnknownRemoteIsForbidden() {
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/admin/status", null, null);

        filter.filter(ex, chain).block();

        assertFalse(chain.called.get(), "来源未知必须 fail-closed");
        assertEquals(HttpStatus.FORBIDDEN, ex.getResponse().getStatusCode());
    }

    @Test
    void configuredTokenRequiresTheHeaderEvenFromLoopback() {
        props.getAdmin().setToken("s3cret");
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/admin/route", "127.0.0.1", null);

        filter.filter(ex, chain).block();

        assertFalse(chain.called.get(), "配了 token 就不能再靠'我是本机'通过");
        assertEquals(HttpStatus.UNAUTHORIZED, ex.getResponse().getStatusCode());
        assertTrue(body(ex).contains("\"type\":\"unauthorized\""), body(ex));
    }

    @Test
    void configuredTokenRejectsWrongValue() {
        props.getAdmin().setToken("s3cret");
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/admin/route", "127.0.0.1", "s3cres");

        filter.filter(ex, chain).block();

        assertFalse(chain.called.get());
        assertEquals(HttpStatus.UNAUTHORIZED, ex.getResponse().getStatusCode());
    }

    @Test
    void configuredTokenAcceptsCorrectValueFromAnyAddress() {
        props.getAdmin().setToken("s3cret");
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/admin/route", "10.1.2.3", "s3cret");

        filter.filter(ex, chain).block();

        assertTrue(chain.called.get(), "带对了 token，从哪来都该放行");
        assertNull(ex.getResponse().getStatusCode());
    }

    /** 业务接口不吃这套鉴权：它的维度是 apiKey + 配额，混在一起就说不清 401 的原因。 */
    @Test
    void businessPathIsNotProtected() {
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/v1/chat/completions", "10.1.2.3", null);

        filter.filter(ex, chain).block();

        assertTrue(chain.called.get());
        assertNull(ex.getResponse().getStatusCode());
    }

    /**
     * {@code /administrator} 不匹配 {@code /admin} 前缀。
     *
     * <p>写成 {@code startsWith("/admin")} 时这条会失败，而失败信息是
     * "这个端点为什么突然要鉴权了"—— 极难联想到前缀匹配。
     */
    @Test
    void pathWithSamePrefixButDifferentSegmentIsNotProtected() {
        Chain chain = new Chain();
        MockServerWebExchange ex = exchange("/administrator", "10.1.2.3", null);

        filter.filter(ex, chain).block();

        assertTrue(chain.called.get(), "/administrator 不是 /admin 的子路径");
    }
}
