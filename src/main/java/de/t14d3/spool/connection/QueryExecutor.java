package de.t14d3.spool.connection;

import de.t14d3.spool.exceptions.OrmException;
import de.t14d3.spool.mapping.EntityMetadata;

import java.lang.reflect.Field;
import java.sql.*;
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

        // Set ID field
        Field idField = md.getIdField();
        idField.setAccessible(true);
        idField.set(entity, rs.getObject(md.getIdColumn()));

        // Set other fields
        List<Field> fields = md.getFields();
        List<String> columns = md.getColumns();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            field.setAccessible(true);
            field.set(entity, rs.getObject(columns.get(i)));
        }

        return entity;
    }
}