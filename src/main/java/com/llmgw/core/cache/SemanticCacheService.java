package com.llmgw.core.cache;

import java.util.Optional;

public interface SemanticCacheService {

    Optional<CacheEntry> lookup(String namespace, String question);

    void write(String namespace, String question, String answer);

    record CacheEntry(String answer, double similarity) {
    }
}
