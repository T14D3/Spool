package de.t14d3.spool.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SQL Query builder using the Builder pattern for safe and fluent query construction.
 * Supports multiple SQL dialects with proper identifier quoting and escaping.
 */
public class Query {
    private final Dialect dialect;
    private final StringBuilder sql;
    private final List<Object> parameters;

    private Query(Dialect dialect, StringBuilder sql, List<Object> parameters) {
        this.dialect = dialect;
        this.sql = sql;
        this.parameters = new ArrayList<>(parameters);
    }

    /**
     * Get the final SQL string.
     */
    public String getSql() {
        return sql.toString();
    }

    /**
     * Get the query parameters for prepared statement binding.
     */
    public List<Object> getParameters() {
        return new ArrayList<>(parameters);
    }

    /**
     * Get the dialect used for this query.
     */
    public Dialect getDialect() {
        return dialect;
    }

    /**
     * Create a new SELECT query builder.
     */
    public static SelectBuilder builder(Dialect dialect) {
        return new SelectBuilder(dialect);
    }

    /**
     * Create a new SELECT query builder for a table.
     */
    public static SelectBuilder select(Dialect dialect, String... columns) {
        return new SelectBuilder(dialect).select(columns);
    }

    /**
     * Create a new INSERT query builder.
     */
    public static InsertBuilder insertInto(Dialect dialect, String table) {
        return new InsertBuilder(dialect).into(table);
    }

    /**
     * Create a new UPDATE query builder.
     */
    public static UpdateBuilder update(Dialect dialect, String table) {
        return new UpdateBuilder(dialect).table(table);
    }

    /**
     * Create a new DELETE query builder.
     */
    public static DeleteBuilder deleteFrom(Dialect dialect, String table) {
        return new DeleteBuilder(dialect).from(table);
    }

    // ========================================================================
    // SELECT QUERY BUILDER
    // ========================================================================

    public static class SelectBuilder {
        private final Dialect dialect;
        private final List<String> columns = new ArrayList<>();
        private String fromTable;
        private final StringBuilder whereClause = new StringBuilder();
        private final List<Object> parameters = new ArrayList<>();
        private String orderBy;
        private String limit;

        public SelectBuilder(Dialect dialect) {
            this.dialect = dialect;
        }

        public SelectBuilder select(String... columns) {
            this.columns.addAll(Arrays.asList(columns));
            return this;
        }

        public SelectBuilder from(String table) {
            this.fromTable = table;
            return this;
        }

        public SelectBuilder where(String condition, Object... params) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            whereClause.append(condition);
            if (params != null && params.length > 0) {
                parameters.addAll(Arrays.asList(params));
            }
            return this;
        }

        public SelectBuilder orderBy(String orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        public SelectBuilder limit(String limit) {
            this.limit = limit;
            return this;
        }

        public Query build() {
            if (columns.isEmpty()) {
                throw new IllegalStateException("SELECT query must specify columns");
            }
            if (fromTable == null) {
                throw new IllegalStateException("SELECT query must specify a table");
            }

            StringBuilder sql = new StringBuilder("SELECT ");

            String columnList = columns.stream()
                    .map(this::quoteColumn)
                    .collect(Collectors.joining(", "));
            sql.append(columnList);

            sql.append(" FROM ").append(dialect.quoteIdentifier(fromTable));

            if (!whereClause.isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            if (orderBy != null && !orderBy.trim().isEmpty()) {
                sql.append(" ORDER BY ").append(processOrderBy(orderBy));
            }

            if (limit != null && !limit.trim().isEmpty()) {
                sql.append(" LIMIT ").append(limit);
            }

            return new Query(dialect, sql, parameters);
        }

        private String quoteColumn(String col) {
            if (col == null) return "";
            String trimmed = col.trim();
            // if it's a wildcard or contains a function call or an expression/alias, leave as-is
            if ("*".equals(trimmed) || trimmed.contains("(") || containsAsClause(trimmed)) {
                return trimmed;
            }
            // handle qualified names like schema.table or table.column
            if (trimmed.contains(".")) {
                return Arrays.stream(trimmed.split("\\."))
                        .map(String::trim)
                        .map(dialect::quoteIdentifier)
                        .collect(Collectors.joining("."));
            }
            return dialect.quoteIdentifier(trimmed);
        }

        private boolean containsAsClause(String s) {
            String upper = s.toUpperCase();
            return upper.contains(" AS ") || upper.matches(".*\\s+AS\\s+.*");
        }

        private String processOrderBy(String orderBy) {
            return Arrays.stream(orderBy.split(","))
                    .map(String::trim)
                    .map(part -> {
                        if (part.isEmpty()) return part;
                        String[] tokens = part.split("\\s+");
                        String col = tokens[0];
                        String rest = part.substring(col.length()).trim();
                        String quoted = quoteColumn(col);
                        if (rest.isEmpty()) return quoted;
                        return quoted + " " + rest;
                    })
                    .collect(Collectors.joining(", "));
        }
    }

    // ========================================================================
    // INSERT QUERY BUILDER
    // ========================================================================

    public static class InsertBuilder {
        private final Dialect dialect;
        private String table;
        private final List<String> columns = new ArrayList<>();
        private final List<String> valuePlaceholders = new ArrayList<>();
        private final List<Object> parameters = new ArrayList<>();

        public InsertBuilder(Dialect dialect) {
            this.dialect = dialect;
        }

        public InsertBuilder into(String table) {
            this.table = table;
            return this;
        }

        public InsertBuilder columns(String... columns) {
            if (columns == null || columns.length == 0) {
                throw new IllegalArgumentException("columns must not be null or empty");
            }
            this.columns.clear();
            this.columns.addAll(Arrays.asList(columns));
            this.valuePlaceholders.clear();
            for (int i = 0; i < columns.length; i++) {
                this.valuePlaceholders.add("?");
            }
            return this;
        }

        public InsertBuilder values(Object... values) {
            if (values == null) {
                throw new IllegalArgumentException("values must not be null");
            }
            if (values.length != columns.size()) {
                throw new IllegalArgumentException("Number of values must match number of columns");
            }
            parameters.addAll(Arrays.asList(values));
            return this;
        }

        public Query build() {
            if (table == null) {
                throw new IllegalStateException("INSERT query must specify a table");
            }
            if (columns.isEmpty()) {
                throw new IllegalStateException("INSERT query must specify columns");
            }
            if (parameters.isEmpty()) {
                throw new IllegalStateException("INSERT query must specify values");
            }

            StringBuilder sql = new StringBuilder("INSERT INTO ");
            sql.append(dialect.quoteIdentifier(table));
            sql.append(" (")
                    .append(columns.stream().map(dialect::quoteIdentifier).collect(Collectors.joining(", ")))
                    .append(") VALUES (")
                    .append(String.join(", ", valuePlaceholders))
                    .append(")");

            return new Query(dialect, sql, parameters);
        }
    }

    // ========================================================================
    // UPDATE QUERY BUILDER
    // ========================================================================

    public static class UpdateBuilder {
        private final Dialect dialect;
        private String table;
        private final StringBuilder setClause = new StringBuilder();
        private final StringBuilder whereClause = new StringBuilder();
        private final List<Object> parameters = new ArrayList<>();
        private int setParameterCount = 0;

        public UpdateBuilder(Dialect dialect) {
            this.dialect = dialect;
        }

        public UpdateBuilder table(String table) {
            this.table = table;
            return this;
        }

        public UpdateBuilder set(String column, Object value) {
            if (!setClause.isEmpty()) {
                setClause.append(", ");
            }
            setClause.append(dialect.quoteIdentifier(column)).append(" = ?");
            parameters.add(value);
            setParameterCount++;
            return this;
        }

        public UpdateBuilder where(String condition, Object... params) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            whereClause.append(condition);
            if (params != null && params.length > 0) {
                parameters.addAll(Arrays.asList(params));
            }
            return this;
        }

        public Query build() {
            if (table == null) {
                throw new IllegalStateException("UPDATE query must specify a table");
            }
            if (setParameterCount == 0) {
                throw new IllegalStateException("UPDATE query must specify at least one SET clause");
            }

            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append(dialect.quoteIdentifier(table));
            sql.append(" SET ").append(setClause);

            if (!whereClause.isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            return new Query(dialect, sql, parameters);
        }
    }

    // ========================================================================
    // DELETE QUERY BUILDER
    // ========================================================================

    public static class DeleteBuilder {
        private final Dialect dialect;
        private String table;
        private final StringBuilder whereClause = new StringBuilder();
        private final List<Object> parameters = new ArrayList<>();

        public DeleteBuilder(Dialect dialect) {
            this.dialect = dialect;
        }

        public DeleteBuilder from(String table) {
            this.table = table;
            return this;
        }

        public DeleteBuilder where(String condition, Object... params) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            whereClause.append(condition);
            if (params != null && params.length > 0) {
                parameters.addAll(Arrays.asList(params));
            }
            return this;
        }

        public Query build() {
            if (table == null) {
                throw new IllegalStateException("DELETE query must specify a table");
            }

            StringBuilder sql = new StringBuilder("DELETE FROM ");
            sql.append(dialect.quoteIdentifier(table));

            if (!whereClause.isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            return new Query(dialect, sql, parameters);
        }
    }
}
