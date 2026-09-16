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
    private Router router = new Router();
    private Cost cost = new Cost();
    private List<Provider> providers = new ArrayList<>();
    /** 模型 -> 单价（元 / 1K tokens）。缺失时回退到 ModelTier.costPer1k 估算。 */
    private Map<String, Price> pricing = new LinkedHashMap<>();

    public static class Cache {
        private boolean enabled = true;
        private double similarityThreshold = 0.92;
        private long ttlSeconds = 3600;
        private int maxEntriesPerNamespace = 500;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
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

    public static class Router {
        private String policy = "COST_FIRST";
        public String getPolicy() { return policy; }
        public void setPolicy(String policy) { this.policy = policy; }
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
    }

    public Cache getCache() { return cache; }
    public void setCache(Cache cache) { this.cache = cache; }
    public Quota getQuota() { return quota; }
    public void setQuota(Quota quota) { this.quota = quota; }
    public Router getRouter() { return router; }
    public void setRouter(Router router) { this.router = router; }
    public Cost getCost() { return cost; }
    public void setCost(Cost cost) { this.cost = cost; }
    public Map<String, Price> getPricing() { return pricing; }
    public void setPricing(Map<String, Price> pricing) { this.pricing = pricing; }
    public List<Provider> getProviders() { return providers; }
    public void setProviders(List<Provider> providers) { this.providers = providers; }
}
