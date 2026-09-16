package com.llmgw.core.quota;

/**
 * 配额超额。
 *
 * 单独建一个异常类型（而不是复用 IllegalStateException），是为了让入口层能把
 * "限流"和"内部错误"映射成不同的 HTTP 状态码 —— 客户端需要靠 429 决定退避与重试，
 * 而不是从一次 HTTP 200 的响应体里去猜。
 *
 * message 保持 "quota exceeded" 不变：验证脚本 scripts/verify-quota-ledger.py
 * 靠这个字符串识别"被配额拦下"的请求。
 */
public class QuotaExceededException extends RuntimeException {

    private final String apiKey;

    public QuotaExceededException(String apiKey) {
        super("quota exceeded");
        this.apiKey = apiKey;
    }

    public String apiKey() {
        return apiKey;
    }
}
