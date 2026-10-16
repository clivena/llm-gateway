package com.llmgw.observability;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;

/**
 * 安装 Reactor 的全局 MDC 钩子 —— 把 {@link MdcContextLifter} 挂到每一个算子之上。
 *
 * <p>用 {@code Hooks.onEachOperator} 而不是在每个 Publisher 上手工挂：
 * 后者要改遍所有返回 Mono/Flux 的地方，漏一个就有盲区，且新写的代码默认没有 ——
 * 属于"必须记得做"的约束，迟早会忘。
 *
 * <p>key 用固定字符串（而不是类名）是因为它出现在 Reactor 的调试栈里，
 * 一个可读的名字能让人一眼看出这层装饰是谁加的。
 *
 * <p>副作用需要说清楚：这个钩子对<b>整个 JVM 的所有响应式流</b>生效，
 * 包括测试代码。开销是每个算子多一层对象包装与四次 MDC 写入 ——
 * 在（默认关闭的）调试场景之外，这是为了可观测性支付的固定成本。
 * 若某天确认它成为瓶颈，正确的替代方案是结构化日志而不是摘掉钩子。
 */
@Component
public class MdcHookInstaller {

    private static final String KEY = "llmgw-mdc-context";

    @PostConstruct
    void install() {
        // 先摘除再注册，让装配幂等：Hooks.onEachOperator 对同一个 key 重复注册会抛
        // IllegalArgumentException，而测试里 Spring 上下文可能被重建
        // （GatewayContextTest 会分别用默认配置与 local profile 各装载一次）。
        // 生产只装载一次，这行不产生额外行为；测试里它决定了能不能跑两遍。
        Hooks.resetOnEachOperator(KEY);
        Hooks.onEachOperator(KEY,
                Operators.lift((scannable, subscriber) -> new MdcContextLifter<>(subscriber)));
    }

    /**
     * 摘除钩子。生产不会调用它 —— 它存在的意义是让集成测试能在干净的环境变量下重复运行，
     * 避免同一个 key 被重复注册（{@code onEachOperator} 用同一个 key 注册两次会抛异常）。
     */
    static void uninstall() {
        Hooks.resetOnEachOperator(KEY);
    }
}
