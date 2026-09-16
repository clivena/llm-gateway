package com.llmgw.api.dto;

public record Usage(int promptTokens, int completionTokens) {
    public int total() {
        return promptTokens + completionTokens;
    }
}
