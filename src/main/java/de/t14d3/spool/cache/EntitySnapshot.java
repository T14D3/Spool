package de.t14d3.spool.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot of an entity's persisted fields.
 *
 * Snapshot values should be simple JDBC-compatible types (String, Number, UUID, Date, etc.).
 */
public final class EntitySnapshot {
    private final String entityClassName;
    private final String id;
    private final Map<String, Object> fieldValues;

    public EntitySnapshot(String entityClassName, String id, Map<String, Object> fieldValues) {
        this.entityClassName = Objects.requireNonNull(entityClassName, "entityClassName");
        this.id = Objects.requireNonNull(id, "id");

        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : Objects.requireNonNull(fieldValues, "fieldValues").entrySet()) {
            String key = Objects.requireNonNull(entry.getKey(), "fieldValues key");
            values.put(key, entry.getValue());
        }
        this.fieldValues = Collections.unmodifiableMap(values);
    }

    public String entityClassName() {
        return entityClassName;
    }

    public String id() {
        return id;
    }

    public Map<String, Object> fieldValues() {
        return fieldValues;
    }
}
