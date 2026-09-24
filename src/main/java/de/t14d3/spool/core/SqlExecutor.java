package de.t14d3.spool.core;

import de.t14d3.spool.mapping.EntityId;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.query.Query;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Executes SQL queries and maps results to entities.
 *
 * Responsible for:
 *  - executing parameterized SQL statements
 *  - mapping result sets to entity instances
 *  - handling generated keys for inserts
 *  - using Query builders for statement construction
 *
 * This class aims to be defensive with JDBC resources, perform basic
 * type conversions and use prepared statements consistently.
 */
public class SqlExecutor {
    private final Connection connection;
    private final Dialect dialect;

    public SqlExecutor(Connection connection) {
        Dialect tempDialect;
        this.connection = connection;

        // Detect dialect
        try {
            String url = connection.getMetaData().getURL();
            tempDialect = Dialect.detectFromUrl(url);
        } catch (SQLException e) {
            tempDialect = Dialect.detectFromUrl(null);
        }
        this.dialect = tempDialect;
    }

    public Connection getConnection() {
        return connection;
    }

    // ---------------------------------------------------------------------
    // Generic statement execution helpers
    // ---------------------------------------------------------------------

    public void execute(String sql, List<Object> params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql + " params=" + params, e);
        }
    }

    public Object executeInsert(String sql, List<Object> params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            setParameters(stmt, params);
            stmt.executeUpdate();

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute INSERT: " + sql + " params=" + params, e);
        }
    }

    public int executeUpdate(String sql, List<Object> params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute UPDATE: " + sql + " params=" + params, e);
        }
    }

    public long count(String tableName) {
        Query q = Query.select(dialect, "COUNT(*)")
                .from(tableName)
                .build();
        try (PreparedStatement stmt = connection.prepareStatement(q.getSql());
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows in " + tableName, e);
        }
    }

    // ---------------------------------------------------------------------
    // High-level ORM operations using Query builders
    // ---------------------------------------------------------------------

    /**
     * Builds an {@code AND}-joined primary-key predicate and appends the id
     * values to {@code params}. Supports single-column and composite ids.
     */
    public static String idWhere(Dialect dialect, EntityMetadata metadata, EntityId id, List<Object> params) {
        List<Field> idFields = metadata.getIdFields();
        List<Object> values = id.values();
        StringBuilder condition = new StringBuilder();
        for (int i = 0; i < idFields.size(); i++) {
            if (i > 0) {
                condition.append(" AND ");
            }
            condition.append(dialect.quoteIdentifier(metadata.getColumnName(idFields.get(i)))).append(" = ?");
            params.add(values.get(i));
        }
        return condition.toString();
    }

    public <T> T findById(Class<T> entityClass, Object id) {
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        EntityId entityId = metadata.getEntityIdFrom(id);

        List<Object> params = new ArrayList<>();
        String condition = idWhere(dialect, metadata, entityId, params);
        Query q = Query.select(dialect, "*")
                .from(metadata.getTableName())
                .where(condition, params.toArray())
                .build();

        try (PreparedStatement stmt = connection.prepareStatement(q.getSql())) {
            setParameters(stmt, q.getParameters());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToEntity(rs, metadata);
                }
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find entity by ID: " + id, e);
        }
    }

    /**
     * Load entities by a list of single-column ids in one {@code IN} query.
     * Used for batch prefetching of eager relations.
     */
    public <T> List<T> findByIds(Class<T> entityClass, List<Object> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        String column = dialect.quoteIdentifier(metadata.getIdColumnName());
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                placeholders.append(", ");
            }
            placeholders.append('?');
        }
        Query q = Query.select(dialect, "*")
                .from(metadata.getTableName())
                .where(column + " IN (" + placeholders + ")", ids.toArray())
                .build();

        List<T> results = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(q.getSql())) {
            setParameters(stmt, q.getParameters());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapResultSetToEntity(rs, metadata));
                }
            }
            return results;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find entities by ids for " + entityClass.getName(), e);
        }
    }

    public <T> List<T> findAll(Class<T> entityClass) {
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        Query q = Query.select(dialect, "*")
                .from(metadata.getTableName())
                .build();

        List<T> results = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(q.getSql())) {
            setParameters(stmt, q.getParameters());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapResultSetToEntity(rs, metadata));
                }
            }
            return results;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find all entities of " + entityClass.getName(), e);
        }
    }

    public boolean existsById(Class<?> entityClass, Object id) {
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        EntityId entityId = metadata.getEntityIdFrom(id);

        List<Object> params = new ArrayList<>();
        String condition = idWhere(dialect, metadata, entityId, params);
        Query q = Query.select(dialect, "1")
                .from(metadata.getTableName())
                .where(condition, params.toArray())
                .limit("1")
                .build();

        try (PreparedStatement stmt = connection.prepareStatement(q.getSql())) {
            setParameters(stmt, q.getParameters());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check entity existence by ID: " + id, e);
        }
    }

    public void delete(Object entity, EntityMetadata metadata) {
        EntityId id = metadata.getEntityId(entity);
        if (!id.isComplete()) {
            return;
        }

        List<Object> params = new ArrayList<>();
        String condition = idWhere(dialect, metadata, id, params);
        Query q = Query.deleteFrom(dialect, metadata.getTableName())
                .where(condition, params.toArray())
                .build();

        executeUpdate(q.getSql(), q.getParameters());
    }

    private static boolean isUnsetAutoIncrementId(EntityMetadata metadata, Object idValue) {
        if (!metadata.isAutoIncrement()) {
            return false;
        }
        if (idValue == null) {
            return true;
        }
        return idValue instanceof Number n && n.longValue() == 0L;
    }

    public void insert(Object entity, EntityMetadata metadata) {
        boolean shouldGenerateId = false;
        if (metadata.isAutoIncrement()) {
            shouldGenerateId = isUnsetAutoIncrementId(metadata, metadata.getIdValue(entity));
        }

        if (metadata.hasVersion()) {
            Field versionField = metadata.getVersionField();
            if (metadata.getFieldValue(entity, versionField) == null) {
                metadata.setFieldValue(entity, versionField, zeroVersion(versionField.getType()));
            }
        }

        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();

        for (Field field : metadata.getFields()) {
            // Skip auto-generated id when unset (null or 0).
            if (metadata.isIdField(field) && shouldGenerateId) {
                continue;
            }

            cols.add(metadata.getColumnName(field));

            if (EntityMetadata.isOwningToOne(field)) {
                Object related = metadata.getFieldValue(entity, field);
                if (related == null) {
                    vals.add(null);
                } else {
                    EntityMetadata relatedMeta = EntityMetadata.of(related.getClass());
                    vals.add(relatedMeta.getIdValue(related));
                }
            } else {
                vals.add(metadata.getFieldValue(entity, field));
            }
        }

        if (cols.isEmpty()) {
            throw new IllegalStateException("No columns to insert for " + metadata.getEntityClass().getName());
        }

        // Build and execute insert via Query builder
        Query q = Query.insertInto(dialect, metadata.getTableName())
                .columns(cols.toArray(new String[0]))
                .values(vals.toArray())
                .build();

        if (shouldGenerateId) {
            Object generated = executeInsert(q.getSql(), q.getParameters());
            if (generated != null) {
                Object converted = de.t14d3.spool.mapping.TypeMapper.convertToJavaType(generated, metadata.getIdField().getType());
                metadata.setIdValue(entity, converted);
            }
        } else {
            execute(q.getSql(), q.getParameters());
        }
    }

    /**
     * The value used for dirty comparison. {@code ManyToOne} fields are
     * represented by the referenced entity's id in the snapshot, so the
     * entity object must be normalized before comparing.
     */
    static Object comparableFieldValue(EntityMetadata metadata, Field field, Object entity) {
        Object value = metadata.getFieldValue(entity, field);
        if (value == null) {
            return null;
        }
        if (EntityMetadata.isOwningToOne(field)) {
            EntityMetadata relatedMeta = EntityMetadata.of(value.getClass());
            if (relatedMeta.isCompositeId()) {
                return value;
            }
            return relatedMeta.getIdValue(value);
        }
        return value;
    }

    /**
     * Returns true when at least one mapped non-id column of {@code entity}
     * differs from the value captured in {@code snapshot}. A null snapshot
     * means "no baseline available" and always counts as dirty.
     */
    public static boolean hasChanges(Object entity, EntityMetadata metadata, Map<Field, Object> snapshot) {
        if (snapshot == null) {
            return true;
        }
        for (Field field : metadata.getFields()) {
            if (metadata.isIdField(field)) continue;

            Object current = comparableFieldValue(metadata, field, entity);
            Object original = snapshot.get(field);
            if (!Objects.deepEquals(current, original)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update an entity's dirty columns.
     *
     * @return true when an UPDATE statement was executed, false when the
     *         entity had no changes and was skipped
     */
    public boolean update(Object entity, EntityMetadata metadata, Map<Field, Object> snapshot) {
        Query.UpdateBuilder ub = Query.update(dialect, metadata.getTableName());
        boolean hasSnapshot = snapshot != null;
        boolean isDirty = false;
        for (Field field : metadata.getFields()) {
            if (metadata.isIdField(field)) continue;

            Object comparable = comparableFieldValue(metadata, field, entity);
            Object original = hasSnapshot ? snapshot.get(field) : null;

            if (!hasSnapshot || !Objects.deepEquals(comparable, original)) {
                ub.set(metadata.getColumnName(field), comparable);
                isDirty = true;
            }
        }

        // If no fields to update (no changes), skip the update
        if (!isDirty) {
            return false;
        }

        EntityId id = metadata.getEntityId(entity);
        if (!id.isComplete()) {
            throw new IllegalStateException("Cannot update entity without id: " + metadata.getEntityClass().getName());
        }

        Field versionField = metadata.getVersionField();
        Object previousVersion = null;
        Object nextVersion = null;
        if (versionField != null) {
            previousVersion = metadata.getFieldValue(entity, versionField);
            if (previousVersion == null) {
                throw new IllegalStateException(
                        "Cannot update entity with a null @Version value: " + metadata.getEntityClass().getName());
            }
            nextVersion = incrementVersion(previousVersion, versionField.getType());
            ub.set(metadata.getColumnName(versionField), nextVersion);
        }

        List<Object> params = new ArrayList<>();
        String condition = idWhere(dialect, metadata, id, params);
        ub.where(condition, params.toArray());
        if (versionField != null) {
            ub.where(dialect.quoteIdentifier(metadata.getColumnName(versionField)) + " = ?", previousVersion);
        }

        Query q = ub.build();
        int affected = executeUpdate(q.getSql(), q.getParameters());
        if (affected == 0) {
            throw new de.t14d3.spool.OptimisticLockException(
                    metadata.getEntityClass(), id.values(),
                    "Update matched no row for " + metadata.getEntityClass().getName() + "#" + id
                            + (versionField != null
                            ? "; the row was modified by another transaction (version mismatch)"
                            : "; the row was deleted by another transaction"));
        }
        if (versionField != null) {
            metadata.setFieldValue(entity, versionField, nextVersion);
        }
        return true;
    }

    private static Object zeroVersion(Class<?> versionType) {
        if (versionType == long.class || versionType == Long.class) return 0L;
        if (versionType == int.class || versionType == Integer.class) return 0;
        if (versionType == short.class || versionType == Short.class) return (short) 0;
        throw new IllegalArgumentException("Unsupported @Version type: " + versionType.getName());
    }

    private static Object incrementVersion(Object current, Class<?> versionType) {
        if (!(current instanceof Number number)) {
            throw new IllegalArgumentException(
                    "@Version value must be numeric, got " + current.getClass().getName());
        }
        if (versionType == long.class || versionType == Long.class) return number.longValue() + 1L;
        if (versionType == int.class || versionType == Integer.class) return number.intValue() + 1;
        if (versionType == short.class || versionType == Short.class) return (short) (number.shortValue() + 1);
        throw new IllegalArgumentException("Unsupported @Version type: " + versionType.getName());
    }

    // ---------------------------------------------------------------------
    // Mapping helpers
    // ---------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public static <T> T mapResultSetToEntity(ResultSet rs, EntityMetadata metadata) throws SQLException {
        Object entity = metadata.newInstance();

        for (Field field : metadata.getFields()) {
            String columnName = metadata.getColumnName(field);
            Object raw;
            try {
                raw = rs.getObject(columnName);
            } catch (SQLException e) {
                // if column not present use null (defensive)
                raw = null;
            }

            if (raw != null) {
                raw = de.t14d3.spool.mapping.TypeMapper.convertToJavaType(raw, field.getType());
            }

            // For owning single references we do not eagerly load the related
            // entity; instead store an id-only stub entity. EntityManager may
            // later eagerly hydrate it.
            if (EntityMetadata.isOwningToOne(field)) {
                if (raw == null) {
                    metadata.setFieldValue(entity, field, null);
                } else {
                    EntityMetadata relatedMeta = EntityMetadata.of(field.getType());
                    if (relatedMeta.isCompositeId()) {
                        throw new UnsupportedOperationException(
                                "Single-reference target with a composite id is not supported: "
                                        + field.getType().getName());
                    }
                    Object related = relatedMeta.newInstance();
                    Object convertedId = de.t14d3.spool.mapping.TypeMapper.convertToJavaType(raw, relatedMeta.getIdField().getType());
                    relatedMeta.setIdValue(related, convertedId);
                    metadata.setFieldValue(entity, field, related);
                }
                continue;
            }

            metadata.setFieldValue(entity, field, raw);
        }

        return (T) entity;
    }

    // ---------------------------------------------------------------------
    // PreparedStatement parameter binding with simple type handling
    // ---------------------------------------------------------------------

    public static PreparedStatement setParameters(PreparedStatement stmt, List<Object> params) throws SQLException {
        if (params == null || params.isEmpty()) return stmt;

        for (int i = 0; i < params.size(); i++) {
            Object p = params.get(i);
            int idx = i + 1;

            switch (p) {
                case null -> stmt.setNull(idx, Types.NULL);
                case String s -> stmt.setString(idx, s);
                case Integer integer -> stmt.setInt(idx, integer);
                case Long l -> stmt.setLong(idx, l);
                case Boolean b -> stmt.setBoolean(idx, b);
                case Double v -> stmt.setDouble(idx, v);
                case Float v -> stmt.setFloat(idx, v);
                case Short aShort -> stmt.setShort(idx, aShort);
                case Byte b -> stmt.setByte(idx, b);
                case java.sql.Date date -> stmt.setDate(idx, date);
                case Time time -> stmt.setTime(idx, time);
                case Timestamp timestamp -> stmt.setTimestamp(idx, timestamp);
                case Date date -> stmt.setTimestamp(idx, new Timestamp(date.getTime()));
                case LocalDate localDate -> stmt.setDate(idx, java.sql.Date.valueOf(localDate));
                case LocalDateTime localDateTime -> stmt.setTimestamp(idx, Timestamp.valueOf(localDateTime));
                case Instant instant -> stmt.setTimestamp(idx, Timestamp.from(instant));
                case OffsetDateTime offsetDateTime -> stmt.setTimestamp(idx, Timestamp.from(offsetDateTime.toInstant()));
                case UUID uuid -> stmt.setString(idx, uuid.toString());
                case Enum<?> anEnum -> stmt.setString(idx, anEnum.name());
                default ->
                    // fallback - let JDBC try to handle it
                        stmt.setObject(idx, p);
            }
        }
        return stmt;
    }
}
