package com.llmgw.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.net.URI;

/**
 * 让 /dashboard 直接打开控制台首页。
 *
 * 静态资源本身由 Spring Boot 从 classpath:/static/ 提供（构建产物已拷进 static/dashboard），
 * 但欢迎页只在根路径生效，/dashboard/ 这类子目录不会自动去找 index.html，
 * 所以这里显式加一条重定向——否则访问 /dashboard/ 会 404，看起来像"页面没打包进去"。
 *
 * 用 RouterFunction 而不是 WebFluxConfigurer.addViewControllers：
 * 后者是 WebMvc 的写法，WebFlux 里没有对应的 ViewControllerRegistry（编译期就找不到符号）。
 */
@Configuration
public class DashboardWebConfig {

    @Bean
    public RouterFunction<ServerResponse> dashboardEntry() {
        return RouterFunctions.route()
                .GET("/dashboard", request -> redirectToIndex())
                .GET("/dashboard/", request -> redirectToIndex())
                .build();
    }

    private static Mono<ServerResponse> redirectToIndex() {
        return ServerResponse.temporaryRedirect(URI.create("/dashboard/index.html")).build();
    }
}
