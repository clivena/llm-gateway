package com.llmgw.api;

import com.llmgw.core.quota.QuotaExceededException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回归：旧实现把所有错误都写成 HTTP 200 + 响应体里的 error 字段，
 * 客户端无法用状态码判断"该退避还是该重试"，也不符合 OpenAI 兼容协议。
 */
class ChatControllerStatusTest {

    @Test
    void quotaExceededMapsTo429() {
        HttpStatus status = ChatController.statusOf(new QuotaExceededException("tenant-a"));
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, status);
    }

    @Test
    void upstreamFailureMapsTo502() {
        assertEquals(HttpStatus.BAD_GATEWAY,
                ChatController.statusOf(new IllegalStateException("upstream error: rate limited")));
    }

    @Test
    void unexpectedErrorMapsTo500() {
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR,
                ChatController.statusOf(new RuntimeException("boom")));
    }

    @Test
    void quotaMessageStaysStableForVerifyScript() {
        // scripts/verify-quota-ledger.py 靠这个字符串识别"被配额拦下"的请求
        assertTrue(new QuotaExceededException("tenant-a").getMessage().contains("quota exceeded"));
    }
}
