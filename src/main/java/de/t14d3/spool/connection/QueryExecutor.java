package de.t14d3.spool.connection;

import de.t14d3.spool.exceptions.OrmException;
import de.t14d3.spool.mapping.EntityMetadata;

import java.lang.reflect.Field;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

public class QueryExecutor {
    private final Connection conn;

    public QueryExecutor(Connection conn) {
        this.conn = conn;
    }

    public void execute(String sql, List<Object> params) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new OrmException(e);
        }
    }

    public <T> T find(Class<T> cls, Object id) {
        EntityMetadata md = EntityMetadata.of(cls);
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", md.getTableName(), md.getIdColumn());
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return mapRowToEntity(rs, cls, md);
            }
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }

    public <T> List<T> findAll(Class<T> cls) {
        EntityMetadata md = EntityMetadata.of(cls);
        String sql = String.format("SELECT * FROM %s", md.getTableName());
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<T> results = new ArrayList<>();
            while (rs.next()) {
                results.add(mapRowToEntity(rs, cls, md));
            }
            return results;
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }

    public <T> T findOneBy(Class<T> cls, Map<String, Object> criteria) {
        return findOneBy(cls, criteria, null);
    }

    public <T> T findOneBy(Class<T> cls, Map<String, Object> criteria, String orderBy) {
        List<T> results = findBy(cls, criteria, orderBy, 1);
        return results.isEmpty() ? null : results.get(0);
    }

    public <T> List<T> findBy(Class<T> cls, Map<String, Object> criteria) {
        return findBy(cls, criteria, null, -1, -1);
    }

    public <T> List<T> findBy(Class<T> cls, Map<String, Object> criteria, String orderBy) {
        return findBy(cls, criteria, orderBy, -1, -1);
    }

    public <T> List<T> findBy(Class<T> cls, Map<String, Object> criteria, int limit) {
        return findBy(cls, criteria, null, limit, -1);
    }

    public <T> List<T> findBy(Class<T> cls, Map<String, Object> criteria, String orderBy, int limit) {
        return findBy(cls, criteria, orderBy, limit, -1);
    }

    public <T> List<T> findBy(Class<T> cls, Map<String, Object> criteria, int limit, int offset) {
        return findBy(cls, criteria, null, limit, offset);
    }

    public <T> List<T> findBy(Class<T> cls, Map<String, Object> criteria, String orderBy, int limit, int offset) {
        EntityMetadata md = EntityMetadata.of(cls);
        Map<String, String> fieldToColumnMap = createFieldToColumnMap(md);
        List<String> whereClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        // Handle criteria (optional)
        if (criteria != null && !criteria.isEmpty()) {
            for (Map.Entry<String, Object> entry : criteria.entrySet()) {
                String fieldName = entry.getKey();
                String column = fieldToColumnMap.get(fieldName);

                if (column == null) {
                    throw new OrmException("Unknown field: " + fieldName);
                }

                whereClauses.add(column + " = ?");
                params.add(entry.getValue());
            }
        }

        // Build SQL query
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(md.getTableName());

        if (!whereClauses.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }

        if (orderBy != null && !orderBy.trim().isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }

        if (limit > 0) {
            sql.append(" LIMIT ").append(limit);
        }

        if (offset > 0) {
            sql.append(" OFFSET ").append(offset);
        }

        // Execute query
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = ps.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapRowToEntity(rs, cls, md));
                }
                return results;
            }
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }

    private Map<String, String> createFieldToColumnMap(EntityMetadata md) {
        Map<String, String> map = new HashMap<>();

        // Add ID field
        map.put(md.getIdField().getName(), md.getIdColumn());

        // Add regular fields
        List<Field> fields = md.getFields();
        List<String> columns = md.getColumns();
        for (int i = 0; i < fields.size(); i++) {
            map.put(fields.get(i).getName(), columns.get(i));
        }

        return map;
    }

    private <T> T mapRowToEntity(ResultSet rs, Class<T> cls, EntityMetadata md)
            throws Exception {
        T entity = cls.getDeclaredConstructor().newInstance();

        // Map ID field
        Field idField = md.getIdField();
        idField.setAccessible(true);
        Object rawId = rs.getObject(md.getIdColumn());
        idField.set(entity, convertValue(rawId, idField.getType()));

        // Map other fields
        List<Field> fields = md.getFields();
        List<String> columns = md.getColumns();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            field.setAccessible(true);
            Object raw = rs.getObject(columns.get(i));
            Object converted = convertValue(raw, field.getType());
            field.set(entity, converted);
        }

        return entity;
    }

    /**
     * Convert raw JDBC value to target Java type.
     */
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) {
            if (targetType.isPrimitive()) {
                if (targetType == boolean.class) return false;
                if (targetType == byte.class) return (byte) 0;
                if (targetType == short.class) return (short) 0;
                if (targetType == int.class) return 0;
                if (targetType == long.class) return 0L;
                if (targetType == float.class) return 0f;
                if (targetType == double.class) return 0d;
                if (targetType == char.class) return '\u0000';
            }
            return null;
        }

        // Boolean handling
        if (targetType == boolean.class || targetType == Boolean.class) {
            if (value instanceof Number) {
                return ((Number) value).intValue() != 0;
            }
            if (value instanceof Boolean) {
                return value;
            }
            throw new OrmException("Cannot convert " + value.getClass() + " to boolean");
        }

        // Numeric types
        if (value instanceof Number num) {
            if (targetType == byte.class || targetType == Byte.class) return num.byteValue();
            if (targetType == short.class || targetType == Short.class) return num.shortValue();
            if (targetType == int.class || targetType == Integer.class) return num.intValue();
            if (targetType == long.class || targetType == Long.class) return num.longValue();
            if (targetType == float.class || targetType == Float.class) return num.floatValue();
            if (targetType == double.class || targetType == Double.class) return num.doubleValue();
        }

        // Date/time types
        if (value instanceof Timestamp && targetType == java.util.Date.class) {
            return new java.util.Date(((Timestamp) value).getTime());
        }
        if (value instanceof java.sql.Date && targetType == java.util.Date.class) {
            return new java.util.Date(((java.sql.Date) value).getTime());
        }
        if (value instanceof Time && targetType == java.util.Date.class) {
            return new java.util.Date(((Time) value).getTime());
        }

        // String
        if (targetType == String.class && !(value instanceof String)) {
            return value.toString();
        }

        // Enum
        if (targetType.isEnum()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            Class<? extends Enum> enumType = (Class<? extends Enum>) targetType;
            //noinspection unchecked
            return Enum.valueOf(enumType, value.toString());
        }

        // Blob to byte[]
        if (value instanceof Blob blob && targetType == byte[].class) {
            try {
                byte[] bytes = blob.getBytes(1, (int) blob.length());
                blob.free();
                return bytes;
            } catch (SQLException e) {
                throw new OrmException(e);
            }
        }

        // Clob to String
        if (value instanceof Clob && targetType == String.class) {
            Clob clob = (Clob) value;
            try {
                String text = clob.getSubString(1, (int) clob.length());
                clob.free();
                return text;
            } catch (SQLException e) {
                throw new OrmException(e);
            }
        }

        // Fallback
        if (targetType.isAssignableFrom(value.getClass())) {
            return value;
        }

        throw new OrmException("Unsupported mapping from " + value.getClass() + " to " + targetType);
    }
}