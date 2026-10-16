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
 * model 归属规则（F2 起改为候选链）：一个 model 名可以被多家 provider 声明，
 * 它们按配置声明顺序组成**故障转移候选链**，链首即默认执行者。
 * 顺序即配置文件里的顺序，所以"谁优先"是显式可读的，
 * 不依赖 bean 加载顺序 —— 后者不可预测，不适合用来决定路由。
 *
 * <h3>为什么 F2 必须把 Map 改成 Map&lt;String, List&gt;</h3>
 * 旧实现用 {@code putIfAbsent} 做单值索引：同一个 model 名在第二家 provider 下重复声明时
 * 只 warn 一句就把它**丢掉**了。这在"只想挑一家"的语义下没错，但它让
 * "同模型多部署"这个配置根本表达不出来 —— 写完配置、启动日志也打了、请求也没报错，
 * 只是第二家永远不会被用到。F2 要做故障转移，第一个要改的就是这个数据结构，
 * 否则转移逻辑写完了也没有第二个候选可转移（这是"先改数据结构、再写算法"的典型）。
 *
 * <p>唯一真正该报警的重复声明是 **base-url 相同**：同一份出口挂了两次名，
 * 转移过去只会得到同一个失败，白烧一次调用。这种情况仍然 warn。
 */
@Component
public class ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistry.class);

    private final List<ProviderAdapter> adapters;
    /** model → 候选链（按配置声明顺序）。链首是默认执行者，其余是故障转移目标。 */
    private final Map<String, List<ProviderAdapter>> byModel;

    public ProviderRegistry(GatewayProperties properties, WebClient.Builder builder,
                            TokenEstimator tokenEstimator) {
        List<ProviderAdapter> built = new ArrayList<>();
        // provider 名 → base-url。用于"同一个出口重复声明"这条配置错误的识别。
        // 刻意从 properties 里取而不是给 ProviderAdapter 接口加一个 baseUrl()：
        // 那个接口是**协议适配**的抽象，出口地址属于部署配置，不该成为它的契约；
        // 而且加接口方法会波及所有 mock，收益只有一行日志。
        Map<String, String> urlByName = new LinkedHashMap<>();
        for (GatewayProperties.Provider provider : properties.getProviders()) {
            validate(provider);
            built.add(new OpenAiAdapter(provider, builder, tokenEstimator));
            urlByName.put(provider.getName(), provider.getBaseUrl());
        }
        this.adapters = List.copyOf(built);

        Map<String, List<ProviderAdapter>> index = new LinkedHashMap<>();
        for (ProviderAdapter adapter : this.adapters) {
            for (String model : adapter.models()) {
                List<ProviderAdapter> chain = index.computeIfAbsent(model, k -> new ArrayList<>());
                if (chain.contains(adapter)) {
                    // 同一家 provider 在自己的 models 里把同一个模型名写了两遍 → 去重，
                    // 否则候选链里会出现两个一模一样的候选，转移时白试一次。
                    log.warn("provider [{}] 重复声明模型 {}，已去重", adapter.name(), model);
                    continue;
                }
                if (!chain.isEmpty()) {
                    String same = sameEndpointAs(chain, urlByName, adapter.name());
                    if (same != null) {
                        log.warn("模型 {} 在 provider [{}] 与 [{}] 下重复声明，且 base-url 相同（{}）"
                                        + "：故障转移对同一个出口没有意义，只会让同一份失败多记一次。",
                                model, same, adapter.name(), urlByName.get(adapter.name()));
                    } else {
                        log.info("模型 {} 的第 {} 个部署登记为故障转移候选：[{}] {}",
                                model, chain.size() + 1, adapter.name(), urlByName.get(adapter.name()));
                    }
                }
                chain.add(adapter);
            }
        }
        Map<String, List<ProviderAdapter>> frozen = new LinkedHashMap<>();
        index.forEach((model, chain) -> frozen.put(model, List.copyOf(chain)));
        this.byModel = Collections.unmodifiableMap(frozen);

        // 启动时把每家 provider 的落位逐条打出来。配置写错（列表为空、模型名拼错、
        // 多家 base-url 因共享 builder 而互相覆盖）应该在这一行日志里就能看见，
        // 而不是等第一次请求 502 才回头查配置。
        for (GatewayProperties.Provider p : properties.getProviders()) {
            log.info("provider 注册：[{}] {} → {}", p.getName(), p.getBaseUrl(), p.getModels());
        }
        // 候选链单独打一遍：F2 之后"这个模型有哪几家可以兜底"是一个需要一眼看清的部署事实，
        // 而上面那行只知道每家有什么模型，推不出链。
        frozen.forEach((model, chain) ->
                log.info("模型候选链：{} → {}", model, chain.stream().map(ProviderAdapter::name).toList()));
        log.info("注册汇总：{} 家 provider / {} 个模型", this.adapters.size(), this.byModel.size());
    }

    /** 链里是否已有 provider 用了和 candidate 相同的出口地址；返回那个 provider 名，没有则 null。 */
    private static String sameEndpointAs(List<ProviderAdapter> chain, Map<String, String> urlByName,
                                         String candidate) {
        String url = urlByName.get(candidate);
        if (url == null || url.isBlank()) {
            return null;
        }
        for (ProviderAdapter a : chain) {
            if (url.equalsIgnoreCase(urlByName.get(a.name()))) {
                return a.name();
            }
        }
        return null;
    }

    public List<ProviderAdapter> all() {
        return adapters;
    }

    /**
     * 按模型名找<b>唯一</b>适配器：候选链的第一个。
     *
     * <p>保留这个方法而不是让所有调用方改用 {@link #candidatesFor}：单一答案的语义
     * 在"只想挑一家"的地方更清楚（缓存层的向量模型、启动期校验），
     * 而且它是既有行为，改成"返回列表"会让每处调用方各自去想"取第几个"。
     * 需要故障转移的地方显式调 {@code candidatesFor}。
     */
    public ProviderAdapter forModel(String model) {
        return candidatesFor(model).get(0);
    }

    /**
     * 某个模型的<b>故障转移候选链</b>（F2）：按配置声明顺序，链首是默认执行者。
     *
     * <p>返回的列表是不可变的 —— 调用方（ChatPipeline）会按它顺序尝试，
     * 顺手往里加一家就会改变故障期行为，那种改动必须是显式配置而不是运行时副作用。
     *
     * <p>找不到时抛 IllegalArgumentException（→ HTTP 400）而不是 IllegalStateException
     * （→ 502）：从网关的视角看"这个模型名我不认识"是**调用方把参数写错了**，
     * 不是网关下游故障。报成 502 会诱导客户端退避重试，而重试不会让模型名变对。
     * 消息里带上已注册清单，让人一眼看出是拼错了还是根本没配。
     *
     * <p>另一条路径 model=auto 选到未配置的模型，属于部署错误，已在启动期由
     * RouterService 的候选一致性校验拦掉，正常情况下走不到这里。
     */
    public List<ProviderAdapter> candidatesFor(String model) {
        List<ProviderAdapter> chain = byModel.get(model);
        if (chain == null || chain.isEmpty()) {
            throw new IllegalArgumentException("model 不在本网关服务范围内：" + model
                    + "（已注册模型：" + byModel.keySet() + "）");
        }
        return chain;
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
