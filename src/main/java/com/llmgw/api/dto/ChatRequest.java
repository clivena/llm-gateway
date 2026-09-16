package com.llmgw.api.dto;

import java.util.List;

public record ChatRequest(
        String model,
        Boolean stream,
        Double temperature,
        List<ChatMessage> messages,
        String feature
) {
    public boolean isStream() {
        return Boolean.TRUE.equals(stream);
    }

    public String resolvedModel() {
        return (model == null || model.isBlank() || "auto".equals(model)) ? "auto" : model;
    }
}
