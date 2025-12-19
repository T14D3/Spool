package de.t14d3.spool.cache;

import java.util.Objects;

/**
 * Stable cache key for entity-by-id caching.
 *
 * Uses entity class name + a stringified id to allow cross-process invalidation payloads
 * without depending on the id's concrete Java type.
 */
public final class CacheKey {
    private final String entityClassName;
    private final String id;

    public CacheKey(String entityClassName, String id) {
        this.entityClassName = Objects.requireNonNull(entityClassName, "entityClassName");
        this.id = Objects.requireNonNull(id, "id");
    }

    public static CacheKey of(Class<?> entityClass, Object id) {
        if (entityClass == null) {
            throw new IllegalArgumentException("entityClass must not be null");
        }
        if (id == null) {
            throw new IllegalArgumentException("id must not be null");
        }
        return new CacheKey(entityClass.getName(), String.valueOf(id));
    }

    public String entityClassName() {
        return entityClassName;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CacheKey cacheKey)) return false;
        return entityClassName.equals(cacheKey.entityClassName) && id.equals(cacheKey.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityClassName, id);
    }

    @Override
    public String toString() {
        return entityClassName + "#" + id;
    }
}

