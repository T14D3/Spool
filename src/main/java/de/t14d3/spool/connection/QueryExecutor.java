package de.t14d3.spool.connection;

import de.t14d3.spool.exceptions.OrmException;
import de.t14d3.spool.mapping.EntityMetadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
                T entity = cls.getDeclaredConstructor().newInstance();
                // set ID field
                md.getIdField().set(entity, rs.getObject(md.getIdColumn()));
                // set other columns
                for (int i = 0; i < md.getColumns().size(); i++) {
                    String col = md.getColumns().get(i);
                    Object val = rs.getObject(col);
                    md.getFields().get(i).set(entity, val);
                }
                return entity;
            }
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }

    public <T> List<T> findAll(Class<T> cls) {
        EntityMetadata md = EntityMetadata.of(cls);
        String sql = String.format("SELECT * FROM %s", md.getTableName());
        List<T> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                T entity = cls.getDeclaredConstructor().newInstance();
                md.getIdField().set(entity, rs.getObject(md.getIdColumn()));
                for (int i = 0; i < md.getColumns().size(); i++) {
                    String col = md.getColumns().get(i);
                    Object val = rs.getObject(col);
                    md.getFields().get(i).set(entity, val);
                }
                list.add(entity);
            }
            return list;
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }
}
