package com.llmgw.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private Cache cache = new Cache();
    private Quota quota = new Quota();
    private TokenEstimate tokenEstimate = new TokenEstimate();
    private Resilience resilience = new Resilience();
    private Router router = new Router();
    private Cost cost = new Cost();
    private List<Provider> providers = new ArrayList<>();
    /** 模型 -> 单价（元 / 1K tokens）。缺失时回退到 ModelTier.costPer1k 估算。 */
    private Map<String, Price> pricing = new LinkedHashMap<>();

    public static class Cache {
        private boolean enabled = true;
        /**
         * L1 精确缓存（规范化问题 → 哈希 → 单值）。不依赖 embedding 能力，
         * 因此任何部署下都能工作，默认开。
         */
        private boolean exactEnabled = true;
        /**
         * L2 语义缓存（向量检索）。依赖上游的 /embeddings 能力 ——
         * 供应商没有该端点时它会退化为恒 MISS，且每次请求还要多走一遍
         * "试遍所有 adapter 取向量"的失败路径。没这个能力就显式关掉。
         */
        private boolean semanticEnabled = true;
        private double similarityThreshold = 0.92;
        private long ttlSeconds = 3600;
        private int maxEntriesPerNamespace = 500;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public boolean isExactEnabled() { return exactEnabled; }
        public void setExactEnabled(boolean v) { this.exactEnabled = v; }
        public boolean isSemanticEnabled() { return semanticEnabled; }
        public void setSemanticEnabled(boolean v) { this.semanticEnabled = v; }
        public double getSimilarityThreshold() { return similarityThreshold; }
        public void setSimilarityThreshold(double v) { this.similarityThreshold = v; }
        public long getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }
        public int getMaxEntriesPerNamespace() { return maxEntriesPerNamespace; }
        public void setMaxEntriesPerNamespace(int v) { this.maxEntriesPerNamespace = v; }
    }

    public static class Quota {
        private long defaultLimitTokens = 200_000;
        private long periodTtlSeconds = 3_024_000;
        public long getDefaultLimitTokens() { return defaultLimitTokens; }
        public void setDefaultLimitTokens(long v) { this.defaultLimitTokens = v; }
        public long getPeriodTtlSeconds() { return periodTtlSeconds; }
        public void setPeriodTtlSeconds(long v) { this.periodTtlSeconds = v; }
    }

    /**
     * 重试与熔断（F1）。
     *
     * <p><b>为什么参数是全局一份，而不是按 provider 各配一份</b>：
     * 当前只部署了 1 家 provider，而且"哪家更爱抖"这件事在跑够真机数据之前说不清。
     * 先给一套全局默认值；等有数据了再加 per-provider 覆盖。
     * 提前加一层 per-provider 配置，只会让"为什么这家没重试"多一个排查维度，
     * 而它现在一次都用不上。
     *
     * <p>熔断器与重试实例**按 provider 名**分别持有（见 UpstreamResilience）——
     * 那是必要的：熔断的语义是"这条下游链路不安全"，链路就是 provider，
     * 两家 provider 的失败绝不能共用一个滑动窗口。
     */
    public static class Resilience {

        private Retry retry = new Retry();
        private CircuitBreaker circuitBreaker = new CircuitBreaker();
        private Failover failover = new Failover();

        /**
         * 非流式请求的整体超时。
         *
         * <p>WebClient 底座已配了连接超时 3s / 响应超时 60s（WebClientConfig），
         * 这里再设一层 Reactor timeout 不是重复劳动：netty 的 responseTimeout 只管到
         * "响应头到达"，而这里是"整条 Mono 完成"。两者覆盖的时间区间不同 ——
         * 一个管连不上，一个管连上了但不肯说话。
         */
        private long timeoutMillis = 60_000;

        /**
         * 流式请求的<b>首片</b>超时。刻意不是"整条流的超时"。
         *
         * <p>一次长回答天然要几十秒，给整条流设超时会把它拦腰砍断。
         * 真正该被约束的是"多久还没吐出第一个字" —— 用户对首字延迟的体感
         * 和不耐烦阈值，都发生在那个窗口里。
         *
         * <p>实现上直接映射到 {@code Flux.timeout(Duration)}：该方法的作用域
         * <b>就是首元素</b>，后续元素不再受它约束。参数语义与算子语义天然对齐，
         * 不需要自己写计时器。
         *
         * <p>它同时也是"流式可重试窗口"的时间上界 —— 超过它，就把这次尝试
         * 当成失败来处理（可能触发重试）。
         */
        private long firstChunkTimeoutMillis = 30_000;

        public Retry getRetry() { return retry; }
        public void setRetry(Retry retry) { this.retry = retry; }
        public CircuitBreaker getCircuitBreaker() { return circuitBreaker; }
        public void setCircuitBreaker(CircuitBreaker circuitBreaker) { this.circuitBreaker = circuitBreaker; }
        public Failover getFailover() { return failover; }
        public void setFailover(Failover failover) { this.failover = failover; }
        public long getTimeoutMillis() { return timeoutMillis; }
        public void setTimeoutMillis(long v) { this.timeoutMillis = v; }
        public long getFirstChunkTimeoutMillis() { return firstChunkTimeoutMillis; }
        public void setFirstChunkTimeoutMillis(long v) { this.firstChunkTimeoutMillis = v; }
    }

    /**
     * 重试策略。
     *
     * <p>重试谓词本身不在这里 —— 它是代码（见 {@code UpstreamResilience.retryable}），
     * 因为"哪些错误值得重试"是一条业务判断，不是可调参数：
     * 重试一个 400 只会把同一个 400 拿回来第二次，还多花一次钱。
     */
    public static class Retry {
        /** 总尝试次数<b>含首次</b>。3 表示"首次 + 最多重试 2 次"。1 = 不重试。 */
        private int maxAttempts = 3;
        /** 首次重试前的等待。加等待而不是立刻重试：上游抖动往往需要几百毫秒才缓过来。 */
        private long waitDurationMillis = 300;
        /** 指数退避倍数。第 n 次重试等待 = wait × multiplier^(n-1)。 */
        private double backoffMultiplier = 2.0;
        /** 退避上限。没有上限的话，maxAttempts 一大就会等到天荒地老（而请求还占着配额）。 */
        private long maxWaitDurationMillis = 2_000;

        /**
         * 独立开关，而不是靠"把 max-attempts 设成 1"。
         *
         * <p>理由：故障期运维想临时关掉重试时，改 max-attempts 会破坏
         * "最多 3 次"这个配置的语义；事后回滚时没人知道该改回 3 还是 2。
         * 一个显式的开关让"故意关的"和"配错了"在管理端点上可以直接区分。
         */
        private boolean enabled = true;

        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int v) { this.maxAttempts = v; }
        public long getWaitDurationMillis() { return waitDurationMillis; }
        public void setWaitDurationMillis(long v) { this.waitDurationMillis = v; }
        public double getBackoffMultiplier() { return backoffMultiplier; }
        public void setBackoffMultiplier(double v) { this.backoffMultiplier = v; }
        public long getMaxWaitDurationMillis() { return maxWaitDurationMillis; }
        public void setMaxWaitDurationMillis(long v) { this.maxWaitDurationMillis = v; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
    }

    /** 熔断策略。状态机：CLOSED →（失败率超阈值）→ OPEN →（等待期结束）→ HALF_OPEN → CLOSED。 */
    public static class CircuitBreaker {
        private boolean enabled = true;
        /** 滑动窗口大小（按调用次数计）。窗口太小会被单次抖动误判，太大则反应迟钝。 */
        private int slidingWindowSize = 20;
        /**
         * 窗口内至少要攒够这么多次调用才开始计算失败率。
         *
         * <p>没有它，启动后前 2 个请求都失败 = 失败率 100% → 立刻熔断。
         * 而 2 个样本什么也说明不了。这是熔断器最常见的误配置。
         */
        private int minimumNumberOfCalls = 8;
        /** 失败率阈值（百分比）。 */
        private float failureRateThreshold = 50f;
        /** 打开后等待多久进入半开。太短起不到"给下游喘息"的作用，太长则恢复慢。 */
        private long waitDurationInOpenStateMillis = 10_000;
        /** 半开状态放行几个试探请求。全成功才关闭，任一失败立刻重新打开。 */
        private int permittedNumberOfCallsInHalfOpenState = 3;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public int getSlidingWindowSize() { return slidingWindowSize; }
        public void setSlidingWindowSize(int v) { this.slidingWindowSize = v; }
        public int getMinimumNumberOfCalls() { return minimumNumberOfCalls; }
        public void setMinimumNumberOfCalls(int v) { this.minimumNumberOfCalls = v; }
        public float getFailureRateThreshold() { return failureRateThreshold; }
        public void setFailureRateThreshold(float v) { this.failureRateThreshold = v; }
        public long getWaitDurationInOpenStateMillis() { return waitDurationInOpenStateMillis; }
        public void setWaitDurationInOpenStateMillis(long v) { this.waitDurationInOpenStateMillis = v; }
        public int getPermittedNumberOfCallsInHalfOpenState() { return permittedNumberOfCallsInHalfOpenState; }
        public void setPermittedNumberOfCallsInHalfOpenState(int v) { this.permittedNumberOfCallsInHalfOpenState = v; }
    }

    /**
     * 跨 provider 故障转移（F2）。
     *
     * <h3>它解决的是"熔断之后怎么办"</h3>
     * F1 让熔断打开时返回 503 快速失败 —— 诚实地告诉客户端"我现在不替你问了"，
     * 但这仍然是<b>一次失败</b>。F2 补上后半个答案："不问这家，去问下一家。"
     * 只有熔断没有转移，一次链路故障就等于整站不可用；只有转移没有熔断，
     * 转移会在故障期对着一堆坏链路反复冲。
     *
     * <h3>为什么必须有两个上限，而不是一个</h3>
     * 重试次数与候选家数是<b>相乘</b>关系：{@code maxAttempts=3} × 3 家 = 9 次
     * 真实计费的上游调用。故障期里这个乘法会把账单放大一个量级，
     * 而"故障期账单爆炸"是最难在事后解释清楚的一类事故。所以：
     * <ul>
     *   <li>{@code maxProviders} 限制<b>换几家</b>（宽度）；</li>
     *   <li>{@code maxTotalAttempts} 限制<b>总共发几次</b>（总量）。</li>
     * </ul>
     * 只配其中一个都会留下一个可以无限放大的方向（家数越多、或者每家的
     * maxAttempts 越大）。两个一起配，最坏情况才有一个能算出来的数。
     *
     * <p>总量是<b>请求级</b>而不是 provider 级：预算按候选顺序递减分配，
     * 第一家拿满，后面的候选只分到剩余额度。这与"先试最信任的那家、
     * 不行再快速探一下其余的"这个直觉一致 —— 也让上限严格成立。
     */
    public static class Failover {
        /**
         * 总开关。
         *
         * <p>关掉时行为与 F1 完全一致（只用链首、失败即返回），
         * 是故障期排查"是不是转移把问题放大了"的一键回退开关。
         */
        private boolean enabled = true;
        /**
         * 最多尝试几家候选（含链首）。默认 2 = "主力 + 1 个备选"。
         *
         * <p>默认只给 2 而不是"配置了几家就试几家"：候选链长度是<b>部署拓扑</b>，
         * 它不该顺便决定故障期的调用次数。想多试几家就显式调大，
         * 让这个决定是有人做过的。
         */
        private int maxProviders = 2;
        /**
         * 一次请求内允许发起的<b>上游调用总次数</b>（跨 provider 累计，含首次）。
         *
         * <p>默认 4 与 {@code retry.max-attempts: 3} 的关系是刻意算过的：
         * 链首拿 3 次（首次 + 2 次重试），剩下的 1 次留给备选 ——
         * 备选只探一次就够回答"是不是整条链路都坏了"，再多探就只是在花钱。
         */
        private int maxTotalAttempts = 4;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public int getMaxProviders() { return maxProviders; }
        public void setMaxProviders(int v) { this.maxProviders = v; }
        public int getMaxTotalAttempts() { return maxTotalAttempts; }
        public void setMaxTotalAttempts(int v) { this.maxTotalAttempts = v; }
    }

    /**
     * Token 估算系数。
     *
     * 这几个数字不是"经验常数"，而是**实测校准值**：token 数由上有的分词器决定，
     * 随供应商与模型版本变化。当前默认值来自 2026-10-16 对 DeepSeek 的真机实测
     * （见 scripts/calibrate-token-estimator.py 的输出），换供应商必须重新校准。
     */
    public static class TokenEstimate {
        /**
         * <b>一次性</b>固定开销 —— 整个请求只付一次，与消息条数无关。
         *
         * 这一项是实测挖出来的（deepseek-flash 测得 ≈28）：只按需计费
         * "18 个 ASCII 字符的请求"解释不了上游回传的 32 prompt_tokens，
         * 差的那部分就是上游把请求套进自家对话模板（BOS/EOS 标记、角色分隔等）
         * 的固定成本。
         *
         * 它是短请求的主导项 —— 28 token 的固定开销对一条 5 字提问来说占 85%，
         * 忽略它会让估算在短请求上偏小数倍。反过来，对 2000 字的长请求它
         * 只占 2.5%，可以忽略。
         */
        private double baseOverhead = 26.52;
        /** 每条 CJK / 全角字符折算的 token 数。DeepSeek 实测 ≈0.512（约 2 汉字/token）。 */
        private double cjkPerChar = 0.512;
        /** 每个非 CJK 字符折算的 token 数。DeepSeek 实测 ≈0.188（约 5.3 字符/token）。 */
        private double asciiPerChar = 0.188;
        /** 每条消息的额外结构开销（在 baseOverhead 之外，每条再付一次）。实测 ≈1.16。 */
        private double perMessageOverhead = 1.16;
        /**
         * 全局安全系数。刻意 > 1：配额场景下低估 = 允许超支（钱漏了），
         * 高估 = 提前拦截。两者不对称，所以选保守方向。
         */
        private double safetyFactor = 1.05;

        public double getBaseOverhead() { return baseOverhead; }
        public void setBaseOverhead(double v) { this.baseOverhead = v; }
        public double getCjkPerChar() { return cjkPerChar; }
        public void setCjkPerChar(double v) { this.cjkPerChar = v; }
        public double getAsciiPerChar() { return asciiPerChar; }
        public void setAsciiPerChar(double v) { this.asciiPerChar = v; }
        public double getPerMessageOverhead() { return perMessageOverhead; }
        public void setPerMessageOverhead(double v) { this.perMessageOverhead = v; }
        public double getSafetyFactor() { return safetyFactor; }
        public void setSafetyFactor(double v) { this.safetyFactor = v; }
    }

    /**
     * 路由策略与候选模型表。
     *
     * candidates 必须是**配置项**而不是硬编码常量：模型名随部署环境变化
     * （本机用 deepseek、服务器上可能换 glm），硬编码会让 model=auto 挑到一个
     * 没有适配器的模型名直接 502 —— 而且只有真打请求才会暴露。
     * 现在由 RouterService 在启动时校验"每个候选都被某个 provider 支持"。
     */
    public static class Router {
        private String policy = "COST_FIRST";
        private List<Candidate> candidates = new ArrayList<>();
        private TaskRouting taskRouting = new TaskRouting();

        public String getPolicy() { return policy; }
        public void setPolicy(String policy) { this.policy = policy; }
        public List<Candidate> getCandidates() { return candidates; }
        public void setCandidates(List<Candidate> candidates) { this.candidates = candidates; }
        public TaskRouting getTaskRouting() { return taskRouting; }
        public void setTaskRouting(TaskRouting taskRouting) { this.taskRouting = taskRouting; }
    }

    /**
     * 按任务类型选模型。
     *
     * <p>与 {@code policy} 不是并列关系，而是<b>覆盖与兜底</b>：
     * 任务命中就用任务表里的模型，没命中才落回 policy。所以这里
     * <b>不能</b>再保留一个 {@code policy=TASK_BASED} —— 同一件事有两个开关，
     * 四种组合里必有两种语义不明（policy 说按任务、开关说关闭，听谁的？）。
     * 开关只能有一个。
     *
     * <p>为什么 {@code model} 必须来自 {@code candidates}：不在候选表里的模型
     * 没有 qualityScore，级联升级会找不到"更强档位"，表现为<b>静默永不升级</b>
     * 而不是报错。这类结构性错误只有启动期才能拦，故由 RouterService 校验。
     *
     * <p>默认 {@code enabled=false}：没配映射表就开启，等于所有请求都落
     * default-task，而 default-task 又必须在表里声明 —— 结果是启动失败。
     * 能力默认关闭，用的人显式打开，这样"clone 下来直接能起"不被破坏。
     */
    public static class TaskRouting {
        private boolean enabled = false;
        private String defaultTask = "chat";
        /** rule | off。model 是预留值，当前未实现。 */
        private String classifier = "rule";
        private Map<String, TaskRule> tasks = new LinkedHashMap<>();

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getDefaultTask() { return defaultTask; }
        public void setDefaultTask(String defaultTask) { this.defaultTask = defaultTask; }
        public String getClassifier() { return classifier; }
        public void setClassifier(String classifier) { this.classifier = classifier; }
        public Map<String, TaskRule> getTasks() { return tasks; }
        public void setTasks(Map<String, TaskRule> tasks) { this.tasks = tasks; }
    }

    /** 一个任务对应的模型。升级路径不在这里定义 —— 它由 candidates 的 qualityScore 全局决定。 */
    public static class TaskRule {
        private String model;

        public String getModel() { return model; }
        public void setModel(String model) { this.model = model; }
    }

    /**
     * 一个候选档位。
     * costPer1k 与 pricing 同口径（元 / 1K tokens），仅在模型没有配 pricing 时作为兜底估算。
     */
    public static class Candidate {
        private String model;
        private String tier = "CHEAP";
        private double costPer1k;
        private double qualityScore;

        public String getModel() { return model; }
        public void setModel(String model) { this.model = model; }
        public String getTier() { return tier; }
        public void setTier(String tier) { this.tier = tier; }
        public double getCostPer1k() { return costPer1k; }
        public void setCostPer1k(double v) { this.costPer1k = v; }
        public double getQualityScore() { return qualityScore; }
        public void setQualityScore(double v) { this.qualityScore = v; }
    }

    /** 账本落库方式：postgres | kafka | both。 */
    public static class Cost {
        private String sink = "kafka";
        public String getSink() { return sink; }
        public void setSink(String sink) { this.sink = sink; }

        public boolean toPostgres() {
            return "postgres".equalsIgnoreCase(sink) || "both".equalsIgnoreCase(sink);
        }

        public boolean toKafka() {
            return "kafka".equalsIgnoreCase(sink) || "both".equalsIgnoreCase(sink);
        }
    }

    /** 单价：元 / 1K tokens，输入输出分开计（两者价差通常数倍，合并算会失真）。 */
    public static class Price {
        private double inputPer1k;
        private double outputPer1k;
        public double getInputPer1k() { return inputPer1k; }
        public void setInputPer1k(double v) { this.inputPer1k = v; }
        public double getOutputPer1k() { return outputPer1k; }
        public void setOutputPer1k(double v) { this.outputPer1k = v; }

        public double costOf(int promptTokens, int completionTokens) {
            return promptTokens / 1000.0 * inputPer1k + completionTokens / 1000.0 * outputPer1k;
        }
    }

    public static class Provider {
        private String name;
        private String baseUrl;
        private String apiKey;
        private String embeddingModel;
        private List<String> models = new ArrayList<>();
        /**
         * 是否向上游索取流式末帧的真实 usage（OpenAI 的 stream_options.include_usage）。
         * 默认开 —— 成本归因依赖它，少了它流式账本只能按字符数估算。
         * 个别 OpenAI 兼容实现不认这个参数并直接返回 400，那时在这里关掉。
         */
        private boolean streamIncludeUsage = true;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
        public String getEmbeddingModel() { return embeddingModel; }
        public void setEmbeddingModel(String m) { this.embeddingModel = m; }
        public List<String> getModels() { return models; }
        public void setModels(List<String> models) { this.models = models; }
        public boolean isStreamIncludeUsage() { return streamIncludeUsage; }
        public void setStreamIncludeUsage(boolean v) { this.streamIncludeUsage = v; }
    }

    public Cache getCache() { return cache; }
    public void setCache(Cache cache) { this.cache = cache; }
    public Quota getQuota() { return quota; }
    public void setQuota(Quota quota) { this.quota = quota; }
    public TokenEstimate getTokenEstimate() { return tokenEstimate; }
    public void setTokenEstimate(TokenEstimate v) { this.tokenEstimate = v; }
    public Resilience getResilience() { return resilience; }
    public void setResilience(Resilience v) { this.resilience = v; }
    public Router getRouter() { return router; }
    public void setRouter(Router router) { this.router = router; }
    public Cost getCost() { return cost; }
    public void setCost(Cost cost) { this.cost = cost; }
    public Map<String, Price> getPricing() { return pricing; }
    public void setPricing(Map<String, Price> pricing) { this.pricing = pricing; }
    public List<Provider> getProviders() { return providers; }
    public void setProviders(List<Provider> providers) { this.providers = providers; }
}
