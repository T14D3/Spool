package de.t14d3.spool.core;

import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.query.Query;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
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

    // ---------------------------------------------------------------------
    // High-level ORM operations using Query builders
    // ---------------------------------------------------------------------

    public <T> T findById(Class<T> entityClass, Object id) {
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        Query q = Query.select(dialect, "*")
                .from(metadata.getTableName())
                .where(metadata.getIdColumnName() + " = ?", id)
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

    public void delete(Object entity, EntityMetadata metadata) {
        Object id = metadata.getIdValue(entity);
        if (id == null) {
            return;
        }

        Query q = Query.deleteFrom(dialect, metadata.getTableName())
                .where(metadata.getIdColumnName() + " = ?", id)
                .build();

        executeUpdate(q.getSql(), q.getParameters());
    }

    public void insert(Object entity, EntityMetadata metadata) {
        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();

        for (Field field : metadata.getFields()) {
            // skip null auto-generated id
            if (field.equals(metadata.getIdField()) && metadata.isAutoIncrement()) {
                Object idVal = metadata.getIdValue(entity);
                if (idVal == null) {
                    continue;
                }
            }

            cols.add(metadata.getColumnName(field));

            if (field.isAnnotationPresent(ManyToOne.class)) {
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

        if (metadata.isAutoIncrement() && metadata.getIdValue(entity) == null) {
            Object generated = executeInsert(q.getSql(), q.getParameters());
            if (generated != null) {
                Object converted = de.t14d3.spool.mapping.TypeMapper.convertToJavaType(generated, metadata.getIdField().getType());
                metadata.setIdValue(entity, converted);
            }
        } else {
            execute(q.getSql(), q.getParameters());
        }
    }

    public void update(Object entity, EntityMetadata metadata, Map<Field, Object> snapshot) {
        Query.UpdateBuilder ub = Query.update(dialect, metadata.getTableName());
        boolean hasSnapshot = snapshot != null;
        boolean isDirty = false;
        for (Field field : metadata.getFields()) {
            if (field.equals(metadata.getIdField())) continue;

            Object current = metadata.getFieldValue(entity, field);
            Object original = hasSnapshot ? snapshot.get(field) : null;

            if (!hasSnapshot || !Objects.equals(current, original)) {
                if (field.isAnnotationPresent(ManyToOne.class)) {
                    Object related = metadata.getFieldValue(entity, field);
                    if (related == null) {
                        ub.set(metadata.getColumnName(field), null);
                    } else {
                        EntityMetadata relatedMeta = EntityMetadata.of(related.getClass());
                        ub.set(metadata.getColumnName(field), relatedMeta.getIdValue(related));
                    }
                } else {
                    ub.set(metadata.getColumnName(field), current);
                }
                isDirty = true;
            }
        }

        // If no fields to update (no changes), skip the update
        if (!isDirty) {
            return;
        }

        Object idValue = metadata.getIdValue(entity);
        if (idValue == null) {
            throw new IllegalStateException("Cannot update entity without id: " + metadata.getEntityClass().getName());
        }

        ub.where(metadata.getIdColumnName() + " = ?", idValue);
        Query q = ub.build();
        executeUpdate(q.getSql(), q.getParameters());
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

            // For ManyToOne we currently do not eagerly load related entity;
            // instead store an id-only stub entity. EntityManager may later eagerly hydrate it.
            if (field.isAnnotationPresent(ManyToOne.class)) {
                if (raw == null) {
                    metadata.setFieldValue(entity, field, null);
                } else {
                    EntityMetadata relatedMeta = EntityMetadata.of(field.getType());
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
