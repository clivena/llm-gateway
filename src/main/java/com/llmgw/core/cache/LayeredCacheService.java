package com.llmgw.core.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 缓存门面：按 {@code @Order} 顺序串联各层，<b>前一层命中即短路</b>，不再付出后续层的代价。
 *
 * <p>这是 {@link SemanticCacheService} 唯一的实现 —— 调用方（ChatPipeline）
 * 注入到的永远是它，因此"加一层缓存"不需要改动主链路一行代码。
 * 各层通过 {@link CacheLayer} 注册，顺序由 {@code @Order} 决定。
 *
 * <h3>为什么用 concatMap 而不是 flatMap</h3>
 * {@code flatMap} 会同时订阅所有层 —— L1 已经命中了，L2 还会去调一次 embedding。
 * embedding 是<b>要花钱的上游调用</b>，为了一个已经命中的请求去调它，
 * 等于把缓存省下的钱又花回去了。而且并发订阅会让"命中了哪层"的日志顺序错乱。
 *
 * <h3>为什么逐层吞掉异常</h3>
 * L2 需要 embedding，而 embedding 可能因为"这家供应商没有该端点"或网络问题失败。
 * 这类失败只应让该层降级为 MISS，不该让整条缓存链失败 ——
 * 更不该冒泡到主链路把一次本来能成功的对话变成 502。
 */
@Service
public class LayeredCacheService implements SemanticCacheService {

    private static final Logger log = LoggerFactory.getLogger(LayeredCacheService.class);

    /** Spring 按 @Order 排序注入，顺序即层的优先级。 */
    private final List<CacheLayer> layers;

    public LayeredCacheService(List<CacheLayer> layers) {
        this.layers = layers;
        log.info("缓存层装配：{}", layers.stream().map(CacheLayer::name).toList());
    }

    /**
     * 已装配的层名（按优先级顺序）。
     *
     * 暴露出来的理由与"把启动日志打出来"是同一个，但更强：**空装配是最难查的静默失效**。
     * Spring 对集合注入的规则是"没有候选 bean 就给空集合，不报错"，
     * 一旦哪天某个层因为漏了注解、包扫描范围变了而不在集合里，
     * 表现就是"缓存配置全开、运行时一次都不命中"，而日志里一片正常。
     * 有了这个方法，装配结果可以被测试与运维面板直接断言。
     */
    public List<String> layerNames() {
        return layers.stream().map(CacheLayer::name).toList();
    }

    @Override
    public Mono<CacheEntry> lookup(String namespace, String question) {
        if (layers.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(layers)
                .concatMap(layer -> layer.lookup(namespace, question)
                        // 命中层名进日志：排查"为什么这次没命中"时，
                        // 第一个要回答的问题就是"L1 到底有没有工作"。
                        .doOnNext(entry -> log.debug("cache hit at {} [ns={}]", layer.name(), namespace))
                        .onErrorResume(e -> {
                            log.debug("cache layer {} lookup failed: {}", layer.name(), e.getMessage());
                            return Mono.empty();
                        }))
                .next();
    }

    @Override
    public Mono<Void> write(String namespace, String question, String answer) {
        if (layers.isEmpty()) {
            return Mono.empty();
        }
        // 写入不做短路：L1 与 L2 各自决定要不要写（没有 embedding 能力时 L2 自己跳过）。
        // 用 concatMap 而非 merge 是为了让写入顺序确定 —— 排查问题时日志是可读的。
        return Flux.fromIterable(layers)
                .concatMap(layer -> layer.write(namespace, question, answer)
                        .onErrorResume(e -> {
                            log.debug("cache layer {} write failed: {}", layer.name(), e.getMessage());
                            return Mono.empty();
                        }))
                .then();
    }
}
