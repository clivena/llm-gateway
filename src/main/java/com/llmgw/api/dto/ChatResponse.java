package com.llmgw.api.dto;

public record ChatResponse(
        String id,
        String model,
        String content,
        Usage usage,
        boolean cacheHit
) {
}
