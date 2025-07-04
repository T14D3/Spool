package de.t14d3.spool.core;

import de.t14d3.spool.exceptions.OrmException;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.connection.QueryExecutor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Persister {
    private final QueryExecutor executor;

    public Persister(QueryExecutor executor) {
        this.executor = executor;
    }

    public <T> void insert(T entity, EntityMetadata md) {
        boolean auto = md.isAutoIncrement();
        String table = md.getTableName();
        String idCol = md.getIdColumn();

        // Prepare column list and values
        List<String> cols = new ArrayList<>(md.getColumns());
        List<Object> vals = new ArrayList<>(md.values(entity));

        if (auto && md.idValue(entity) == null) {
            // omit ID so DB auto-generates
            String columnList = String.join(", ", cols);
            String paramList  = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", table, columnList, paramList);

            executor.execute(sql, vals);

            try {
                // fetch generated ID
                String keySql = String.format("SELECT MAX(%s) FROM %s", idCol, table);
                Long generated = executor.queryForLong(keySql);
                if (generated != null) {
                    Field idField = md.getIdField();
                    idField.setAccessible(true);
                    idField.set(entity, generated);
                }
            } catch (Exception e) {
                throw new OrmException("Failed to set auto-generated ID", e);
            }
        } else {
            // include ID explicitly
            List<String> allCols = new ArrayList<>();
            allCols.add(idCol);
            allCols.addAll(cols);

            List<Object> allVals = new ArrayList<>();
            allVals.add(md.idValue(entity));
            allVals.addAll(vals);

            String columnList = String.join(", ", allCols);
            String paramList  = allCols.stream().map(c -> "?").collect(Collectors.joining(", "));
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", table, columnList, paramList);

            executor.execute(sql, allVals);
        }
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
