package com.llmgw.core.protocol;

import com.llmgw.config.GatewayProperties;
import com.llmgw.core.token.TokenEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 适配器注册表：按配置为每个 provider 创建一个适配器，并建立 model → adapter 索引。
 *
 * 为什么需要这一层（P2 多 provider）：
 *   上层（ChatPipeline / RedisSemanticCacheService）其实早就按 List&lt;ProviderAdapter&gt;
 *   写了，缺的只是"谁来生产多个"。旧实现把 OpenAiAdapter 做成单例 @Component，
 *   构造时 findFirst() 只认第一个名为 openai 的 provider —— 配置里加第二家
 *   在运行时完全不生效，而且不报错，属于最难发现的一类配置失效。
 *
 * model 归属规则：同一个 model 名出现在多家 provider 时，**先声明者胜**
 * （putIfAbsent）。顺序即配置文件里的顺序，所以"谁优先"是显式可读的，
 * 不依赖 bean 加载顺序 —— 后者不可预测，不适合用来决定路由。
 */
@Component
public class ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistry.class);

    private final List<ProviderAdapter> adapters;
    private final Map<String, ProviderAdapter> byModel;

    public ProviderRegistry(GatewayProperties properties, WebClient.Builder builder,
                            TokenEstimator tokenEstimator) {
        List<ProviderAdapter> built = new ArrayList<>();
        for (GatewayProperties.Provider provider : properties.getProviders()) {
            validate(provider);
            built.add(new OpenAiAdapter(provider, builder, tokenEstimator));
        }
        this.adapters = List.copyOf(built);

        Map<String, ProviderAdapter> index = new LinkedHashMap<>();
        for (ProviderAdapter adapter : this.adapters) {
            for (String model : adapter.models()) {
                ProviderAdapter owner = index.putIfAbsent(model, adapter);
                if (owner != null) {
                    log.warn("模型 {} 在多家 provider 下重复声明：采用先声明的 {}，忽略 {}",
                            model, owner.name(), adapter.name());
                }
            }
        }
        this.byModel = Collections.unmodifiableMap(index);

        // 启动时把每家 provider 的落位逐条打出来。配置写错（列表为空、模型名拼错、
        // 多家 base-url 因共享 builder 而互相覆盖）应该在这一行日志里就能看见，
        // 而不是等第一次请求 502 才回头查配置。
        for (GatewayProperties.Provider p : properties.getProviders()) {
            log.info("provider 注册：[{}] {} → {}", p.getName(), p.getBaseUrl(), p.getModels());
        }
        log.info("注册汇总：{} 家 provider / {} 个模型", this.adapters.size(), this.byModel.size());
    }

    public List<ProviderAdapter> all() {
        return adapters;
    }

    /**
     * 按模型名找适配器。
     *
     * 找不到时抛 IllegalArgumentException（→ HTTP 400）而不是 IllegalStateException
     * （→ 502）：从网关的视角看"这个模型名我不认识"是**调用方把参数写错了**，
     * 不是网关下游故障。报成 502 会诱导客户端退避重试，而重试不会让模型名变对。
     * 消息里带上已注册清单，让人一眼看出是拼错了还是根本没配。
     *
     * 另一条路径 model=auto 选到未配置的模型，属于部署错误，已在启动期由
     * RouterService 的候选一致性校验拦掉，正常情况下走不到这里。
     */
    public ProviderAdapter forModel(String model) {
        ProviderAdapter adapter = byModel.get(model);
        if (adapter == null) {
            throw new IllegalArgumentException("model 不在本网关服务范围内：" + model
                    + "（已注册模型：" + byModel.keySet() + "）");
        }
        return adapter;
    }

    public boolean supports(String model) {
        return byModel.containsKey(model);
    }

    /**
     * 启动期校验：结构性配置错误要在这里失败，而不是变成运行时的 502。
     *
     * 唯一不阻塞启动的是 api-key：默认配置里它就是空的（便于别人 clone 后直接起服务、
     * 先看日志与面板，填 key 再真调），所以这里只 warn。
     * 生产环境是否要改成 fail-fast 属于部署策略，不该由这个类替使用者决定。
     */
    private void validate(GatewayProperties.Provider p) {
        if (p.getName() == null || p.getName().isBlank()) {
            throw new IllegalStateException("provider 缺少 name（用于日志与归因，不是协议标识）");
        }
        if (p.getBaseUrl() == null || p.getBaseUrl().isBlank()) {
            throw new IllegalStateException("provider " + p.getName() + " 缺少 base-url");
        }
        if (p.getModels() == null || p.getModels().isEmpty()) {
            throw new IllegalStateException("provider " + p.getName() + " 未声明任何 models");
        }
        if (p.getApiKey() == null || p.getApiKey().isBlank()) {
            log.warn("provider [{}] 未配置 api-key：调用它会返回 401。"
                    + "（仅提示，不阻塞启动）", p.getName());
        }
    }
}
