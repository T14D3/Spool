package de.t14d3.spool.core;

import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.connection.QueryExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Persister {
    private final QueryExecutor executor;

    public Persister(QueryExecutor executor) {
        this.executor = executor;
    }

    public <T> void insert(T entity, EntityMetadata md) {
        if (md.isAutoIncrement() && md.idValue(entity) == null) {
            Long nextId = getNextId(md);
            try {
                md.getIdField().set(entity, nextId);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to set auto-incremented ID", e);
            }
        }
        // include ID column as first
        List<String> cols = new ArrayList<>();
        cols.add(md.getIdColumn());
        cols.addAll(md.getColumns());

        String columns = String.join(", ", cols);
        String params = cols.stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                md.getTableName(),
                columns,
                params
        );

        List<Object> values = new ArrayList<>();
        values.add(md.idValue(entity));
        values.addAll(md.values(entity));

        executor.execute(sql, values);
    }

    public <T> void delete(T entity, EntityMetadata md) {
        String sql = String.format(
                "DELETE FROM %s WHERE %s = ?",
                md.getTableName(),
                md.getIdColumn()
        );
        executor.execute(sql, List.of(md.idValue(entity)));
    }

    public <T> void update(T entity, EntityMetadata md) {
        // build "col = ?" assignments
        List<String> assignments = md.getColumns().stream()
                .map(c -> c + " = ?")
                .collect(Collectors.toList());

        String sql = String.format(
                "UPDATE %s SET %s WHERE %s = ?",
                md.getTableName(),
                String.join(", ", assignments),
                md.getIdColumn()
        );

        // collect values in the same order
        List<Object> params = new ArrayList<>(md.values(entity));
        // finally, the ID for WHERE clause
        params.add(md.idValue(entity));

        executor.execute(sql, params);
    }

    private <T> Long getNextId(EntityMetadata md) {
        String sql = String.format("SELECT MAX(%s) FROM %s", md.getIdColumn(), md.getTableName());
        Long maxId = executor.queryForLong(sql);
        return maxId != null ? maxId + 1 : 1L;
    }
}
