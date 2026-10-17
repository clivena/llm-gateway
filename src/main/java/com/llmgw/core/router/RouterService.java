package com.llmgw.core.router;

import com.llmgw.api.dto.ChatRequest;
import com.llmgw.config.GatewayProperties;
import com.llmgw.core.protocol.ProviderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 路由与级联决策。纯逻辑，便于单测。
 *
 * <p>P3 改动：候选模型表由硬编码常量改为配置项（gateway.router.candidates）。
 *
 * <pre>
 *   旧实现把候选写死在字段初始化里（gpt-4o-mini / gpt-4o），而实际 provider 只配了
 *   deepseek —— 于是 model=auto 挑到的模型名没有任何适配器支持，100% 返回 502，
 *   而且只有真打一次请求才会发现。这是"配置与代码各说各话"的典型。
 *
 *   现在一致性由两道闸门守住：
 *     ① 候选表来自配置，模型名随部署环境一起走；
 *     ② 启动时校验每个候选都被某个 provider 支持，不一致直接启动失败 ——
 *        把运行时的 502 前移成启动期的报错。
 * </pre>
 *
 * <p>任务路由改动：{@code model=auto} 不再只按全局 policy 挑一个模型，
 * 而是先判任务、再查映射表。解析顺序见 {@link #resolve(ChatRequest)}。
 *
 * <p><b>任务路由是"选链首模型"的前置决策，不是第二条执行路径。</b>
 * 它只改变这里返回的 {@link Route#tier()}，下游（候选链 / 重试熔断 / 级联升级 /
 * 配额结算）全部复用原有代码。把它写进 dispatch 会和缓存段、级联判据、
 * 候选链、三个配额不变量各打一次架。
 */
@Service
public class RouterService {

    private static final Logger log = LoggerFactory.getLogger(RouterService.class);

    private final List<ModelTier> candidates;
    private final RoutingPolicy policy;
    private final GatewayProperties.TaskRouting taskRouting;
    /** task（已归一化）→ 档位。任务路由关闭时为空表。 */
    private final Map<String, ModelTier> tasks;
    private final TaskClassifier classifier;

    public RouterService(GatewayProperties properties, ProviderRegistry providers,
                         TaskClassifier classifier) {
        this.classifier = classifier;
        this.candidates = properties.getRouter().getCandidates().stream()
                .map(c -> new ModelTier(c.getModel(), c.getTier(), c.getCostPer1k(), c.getQualityScore()))
                .toList();

        if (candidates.isEmpty()) {
            throw new IllegalStateException(
                    "gateway.router.candidates 为空：model=auto 无档位可选。"
                            + "请在配置里声明候选模型（模型名须与 providers[].models 一致）");
        }
        for (ModelTier tier : candidates) {
            if (!providers.supports(tier.model())) {
                throw new IllegalStateException("gateway.router.candidates 声明了模型 "
                        + tier.model() + "，但没有任何 provider 提供它 —— model=auto 会返回 502。"
                        + "请核对 gateway.providers[].models");
            }
        }

        this.policy = parsePolicy(properties.getRouter().getPolicy());
        this.taskRouting = properties.getRouter().getTaskRouting();
        this.tasks = buildTaskTable(taskRouting, candidates);

        log.info("路由候选表：{}", candidates.stream().map(ModelTier::model).toList());
        if (taskRouting.isEnabled()) {
            log.info("任务路由已启用：default={} 映射={}", taskRouting.getDefaultTask(), tasks.keySet());
        } else {
            log.info("任务路由未启用，model=auto 按 policy={} 选模型", policy);
        }
    }

    /**
     * 解析一条请求该用哪个模型。三层，顺序不可换：
     *
     * <pre>
     *   ① 客户端点名了模型        → EXPLICIT，质量分 1.0，永不升级（现状，不动）
     *   ② 任务路由关闭            → 全局 policy（现状，不动）
     *   ③ 任务路由开启            → 判任务 → 查表 → 落 default-task
     * </pre>
     *
     * <p>为什么"显式模型"必须排在最前面：用户点名了模型，网关不能因为
     * "这个任务更适合另一个模型"就换掉它 —— 那是未经同意的替换，也是
     * 未经同意的计费。任务信息在这种情况下只用于观测（打一条 debug）。
     */
    public Route resolve(ChatRequest request) {
        String model = request.resolvedModel();
        if (!"auto".equals(model)) {
            if (request.resolvedTask() != null) {
                log.debug("请求同时指定了 model={} 与 task={}，以 model 为准（显式优先）",
                        model, request.resolvedTask());
            }
            return new Route(new ModelTier(model, "EXPLICIT", 0.0, 1.0),
                    "explicit", Route.EXPLICIT_MODEL, "client-declared-model");
        }

        if (!taskRouting.isEnabled()) {
            return new Route(pick(policy), "n/a", "policy", "task-routing-disabled");
        }

        return pickFor(classifier.decide(request));
    }

    /**
     * 把一次任务判定落实成档位。
     *
     * <p>两类"找不到"的处理刻意不同：
     * <ul>
     *   <li><b>显式声明的未知任务 → 400</b>（IllegalArgumentException 在
     *       OpenAiProtocol 里映射成 invalid_request_error）。静默兜底是最坏的选择：
     *       调用方以为 code 任务生效了，实际走了 chat 模型，只有翻账本才能发现。</li>
     *   <li><b>推断出的未知任务 → 落 default-task</b>。推断是网关自己的判断，
     *       判错不该让用户的请求失败。</li>
     * </ul>
     *
     * <p>注意这里用 IllegalArgumentException 而不是 IllegalStateException：
     * 后者在 statusOf 里映射成 <b>502</b>（它代表服务端状态异常），
     * 用它表达"你传的任务名不对"会让用户收到一个上游错误的假象。
     */
    public Route pickFor(TaskDecision decision) {
        String wanted = normalize(decision.task());
        ModelTier tier = tasks.get(wanted);
        if (tier != null) {
            return new Route(tier, wanted, decision.source(), decision.reason());
        }

        String defaultTask = normalize(taskRouting.getDefaultTask());
        if (decision.explicit()) {
            throw new IllegalArgumentException("未知任务 [" + decision.task()
                    + "]。当前可用：" + tasks.keySet()
                    + "；也可以在请求里省略 task，由网关推断");
        }

        ModelTier fallback = tasks.get(defaultTask);
        if (fallback == null) {
            // 启动校验保证走到这里时 default 一定存在；这是防御性分支，
            // 只在"配置在运行期被改"（当前不支持热更）时才会命中。
            log.warn("default-task [{}] 不在映射表里，回落 policy={}", defaultTask, policy);
            return new Route(pick(policy), "n/a", TaskDecision.FALLBACK, "default-task-missing");
        }
        return new Route(fallback, defaultTask, TaskDecision.FALLBACK,
                "task-not-configured:" + decision.task());
    }

    public ModelTier pick(RoutingPolicy policy) {
        return switch (policy) {
            case COST_FIRST -> candidates.stream()
                    .min(Comparator.comparingDouble(ModelTier::costPer1k))
                    .orElseThrow();
            case QUALITY_FIRST -> candidates.stream()
                    .max(Comparator.comparingDouble(ModelTier::qualityScore))
                    .orElseThrow();
        };
    }

    /**
     * 级联判据：小模型答得不可用、且**还存在更强的档位**时升级。
     *
     * 这里刻意不按 tier 字符串（"CHEAP"）判断，而是问"还有没有 qualityScore 更高的候选"——
     * 这样三档（CHEAP / MEDIUM / STRONG）也能自然工作，不必回来改这段逻辑。
     * TODO: 接入真实判据（格式校验之外的 judge 抽样）。
     */
    public boolean shouldEscalate(ModelTier used, String answer, boolean formatValid) {
        if (formatValid) {
            return false;
        }
        return candidates.stream().anyMatch(c -> c.qualityScore() > used.qualityScore());
    }

    public ModelTier escalateFrom(ModelTier used) {
        return candidates.stream()
                .filter(c -> c.qualityScore() > used.qualityScore())
                .max(Comparator.comparingDouble(ModelTier::qualityScore))
                .orElse(used);
    }

    /** 当前生效的任务映射（只读，供运维端点与测试断言）。 */
    public Map<String, ModelTier> taskTable() {
        return tasks;
    }

    // --- 启动期校验 ---

    private static RoutingPolicy parsePolicy(String raw) {
        try {
            return RoutingPolicy.valueOf(raw);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "gateway.router.policy 非法：[" + raw + "]，可选：COST_FIRST / QUALITY_FIRST。"
                            + "（TASK_BASED 已删除：任务路由是前置判定，不是一种全局策略）");
        }
    }

    /**
     * 构建 task → 档位表。三条启动期校验，都是为了把"运行期才会静默出问题"
     * 前移成启动失败：
     *
     * <ol>
     *   <li>enabled 但表为空 → 等于开了个空壳，所有请求都落 default，
     *       而 default 又必然不在表里，最后每条请求都报错。直接启动失败。</li>
     *   <li>任务指定的模型不在候选表 → 该模型没有 qualityScore，
     *       级联会找不到"更强档位"，表现为<b>永不升级</b>而不是报错。</li>
     *   <li>default-task 未声明 → 兜底路径断裂。</li>
     * </ol>
     */
    private static Map<String, ModelTier> buildTaskTable(GatewayProperties.TaskRouting routing,
                                                         List<ModelTier> candidates) {
        if (!routing.isEnabled()) {
            return Map.of();
        }
        if (routing.getTasks().isEmpty()) {
            throw new IllegalStateException(
                    "gateway.router.task-routing.enabled=true 但 tasks 为空。"
                            + "要么填映射表，要么把 enabled 改成 false");
        }

        // classifier 只实现了 rule / off。写成别的值必须启动失败 ——
        // 尤其是看到配置里写 model 的人会以为"接了模型分类"，实际只是静默走了规则。
        // 配置存在但无效，比配置不存在更难排查。
        String mode = routing.getClassifier();
        if (mode == null || (!"rule".equalsIgnoreCase(mode) && !"off".equalsIgnoreCase(mode))) {
            throw new IllegalStateException(
                    "gateway.router.task-routing.classifier=[" + mode + "] 不支持，可选 rule / off。"
                            + "（model 是预留值，当前未实现：在线模型分类会给所有请求加一次固定开销，"
                            + "且判定不可静态解释）");
        }

        Map<String, ModelTier> built = new LinkedHashMap<>();
        for (Map.Entry<String, GatewayProperties.TaskRule> e : routing.getTasks().entrySet()) {
            String task = normalize(e.getKey());
            String model = e.getValue() == null ? null : e.getValue().getModel();
            if (model == null || model.isBlank()) {
                throw new IllegalStateException("任务 [" + task + "] 没有指定 model");
            }
            ModelTier tier = candidates.stream()
                    .filter(c -> c.model().equals(model))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "任务 [" + task + "] 指定的模型 [" + model + "] 不在 gateway.router.candidates 里。"
                                    + "不在候选表里就没有 qualityScore，级联升级会静默失效。"
                                    + "候选：" + candidates.stream().map(ModelTier::model).toList()));
            if (built.put(task, tier) != null) {
                throw new IllegalStateException(
                        "任务名 [" + task + "] 重复（大小写与首尾空格归一化后冲突）");
            }
        }

        String defaultTask = normalize(routing.getDefaultTask());
        if (!built.containsKey(defaultTask)) {
            throw new IllegalStateException(
                    "gateway.router.task-routing.default-task=[" + routing.getDefaultTask()
                            + "] 未在 tasks 里声明。已声明：" + built.keySet());
        }
        return Map.copyOf(built);
    }

    /**
     * 归一化任务名。配置侧与请求侧必须用同一套规则，否则
     * 配置写 {@code Code}、请求传 {@code code} 会匹配不上 —— 而显式任务
     * 匹配不上是要返回 400 的，用户眼里他明明写对了。
     */
    private static String normalize(String task) {
        return task == null ? "" : task.trim().toLowerCase(Locale.ROOT);
    }
}
