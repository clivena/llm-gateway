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

        public String getPolicy() { return policy; }
        public void setPolicy(String policy) { this.policy = policy; }
        public List<Candidate> getCandidates() { return candidates; }
        public void setCandidates(List<Candidate> candidates) { this.candidates = candidates; }
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
    public Router getRouter() { return router; }
    public void setRouter(Router router) { this.router = router; }
    public Cost getCost() { return cost; }
    public void setCost(Cost cost) { this.cost = cost; }
    public Map<String, Price> getPricing() { return pricing; }
    public void setPricing(Map<String, Price> pricing) { this.pricing = pricing; }
    public List<Provider> getProviders() { return providers; }
    public void setProviders(List<Provider> providers) { this.providers = providers; }
}
