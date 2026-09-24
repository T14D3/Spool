package de.t14d3.spool.mapping;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Value object holding the id values of an entity, in the order of its
 * {@code @Id} fields.
 * <p>
 * For single-column ids this wraps exactly one value, so identity-map and
 * cache keys remain identical to passing the raw id. For composite ids the
 * values are compared as a list.
 */
public final class EntityId {
    private final List<Object> values;

    private EntityId(List<Object> values) {
        // Null components are allowed: they mark a transient (not yet
        // inserted) entity, so List.copyOf/List.of cannot be used here.
        this.values = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(values));
    }

    /**
     * Read the id values from an entity instance.
     */
    public static EntityId of(EntityMetadata metadata, Object entity) {
        Objects.requireNonNull(entity, "entity must not be null");
        List<Field> idFields = metadata.getIdFields();
        List<Object> values = new java.util.ArrayList<>(idFields.size());
        for (Field idField : idFields) {
            values.add(metadata.getFieldValue(entity, idField));
        }
        return new EntityId(values);
    }

    /**
     * Build an id from a user-supplied key. Accepted forms:
     * <ul>
     *   <li>an {@link EntityId} instance (returned as-is)</li>
     *   <li>an entity instance of the target class (id read from its fields)</li>
     *   <li>a {@code Map} keyed by field name or column name (composite ids)</li>
     *   <li>a raw value for single-column ids</li>
     * </ul>
     */
    public static EntityId from(EntityMetadata metadata, Object key) {
        if (key == null) {
            throw new IllegalArgumentException("id must not be null");
        }
        if (key instanceof EntityId entityId) {
            if (entityId.values.size() != metadata.getIdFields().size()) {
                throw new IllegalArgumentException(
                        "Composite id has " + entityId.values.size() + " values but "
                                + metadata.getEntityClass().getName() + " declares "
                                + metadata.getIdFields().size() + " @Id fields");
            }
            return entityId;
        }
        if (metadata.isCompositeId()) {
            if (key instanceof Map<?, ?> map) {
                List<Field> idFields = metadata.getIdFields();
                List<Object> values = new java.util.ArrayList<>(idFields.size());
                for (Field field : idFields) {
                    Object value = map.get(field.getName());
                    if (value == null) {
                        value = map.get(metadata.getColumnName(field));
                    }
                    values.add(value);
                }
                return new EntityId(values);
            }
            if (metadata.getEntityClass().isInstance(key)) {
                return of(metadata, key);
            }
            throw new IllegalArgumentException(
                    "Composite id for " + metadata.getEntityClass().getName()
                            + " must be provided as a Map<String,Object> keyed by @Id field name, or as an entity instance");
        }
        if (metadata.getEntityClass().isInstance(key)) {
            return of(metadata, key);
        }
        List<Object> values = new java.util.ArrayList<>(1);
        values.add(key);
        return new EntityId(values);
    }

    /**
     * Id values in {@code @Id} field order.
     */
    public List<Object> values() {
        return values;
    }

    /**
     * True when every id component is set. Incomplete ids identify transient
     * entities that have not been inserted yet.
     */
    public boolean isComplete() {
        for (Object value : values) {
            if (value == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Convenience accessor for single-column ids.
     */
    public Object singleValue() {
        if (values.size() != 1) {
            throw new IllegalStateException("Entity has a composite id with " + values.size() + " columns");
        }
        return values.get(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntityId entityId)) return false;
        return values.equals(entityId.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        if (values.size() == 1) {
            return String.valueOf(values.get(0));
        }
        return values.toString();
    }
}
