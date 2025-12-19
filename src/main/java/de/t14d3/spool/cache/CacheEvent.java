package de.t14d3.spool.cache;

import java.util.Objects;

public final class CacheEvent {

    public enum Operation {
        UPSERT,
        DELETE
    }

    private final Operation operation;
    private final CacheKey key;

    public CacheEvent(Operation operation, CacheKey key) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.key = Objects.requireNonNull(key, "key");
    }

    public Operation operation() {
        return operation;
    }

    public CacheKey key() {
        return key;
    }
}

