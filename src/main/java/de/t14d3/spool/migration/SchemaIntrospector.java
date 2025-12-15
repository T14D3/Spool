package de.t14d3.spool.migration;



import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.mapping.TypeMapper;
import de.t14d3.spool.query.Dialect;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Introspects database schema and entity metadata to build schema definitions.
 * This implementation is defensive and database-agnostic: it prefers DatabaseMetaData,
 * but falls back to vendor-specific queries when necessary (notably for SQLite).
 */
public class SchemaIntrospector {
    private final Connection connection;
    private final Dialect dialect;

    public SchemaIntrospector(Connection connection) {
        this.connection = connection;
        String url = null;
        try {
            DatabaseMetaData md = connection.getMetaData();
            url = md.getURL();
        } catch (SQLException ignored) {
            // If metadata fails, keep url null and default to GENERIC
        }
        this.dialect = Dialect.detectFromUrl(url);
    }

    /**
     * Get all table names from the database (user tables only).
     * Returns names in lowercase for easier, case-insensitive comparisons.
     */
    public Set<String> getTableNames() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        Set<String> tables = new HashSet<>();

        // Try the portable metadata approach first
        try {
            String schema = resolveSchemaForMetadata();
            // Request only "TABLE" and include "VIEW" as well in some DBs user tables may be views
            try (ResultSet rs = metaData.getTables(null, schema, "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String tableSchema = rs.getString("TABLE_SCHEM"); // may be null for sqlite
                    if (tableName == null) continue;

                    // Filter out system schemas/tables
                    if (isSystemSchema(tableSchema)) continue;
                    if (isSystemTableName(tableName)) continue;

                    tables.add(tableName.toLowerCase());
                }
            }
            return tables;
        } catch (SQLException e) {
            // Some drivers (older sqlite drivers) can throw on metadata calls with schema param.
            // Fallback to vendor-specific methods (SQLite) or rethrow for unknown drivers.
            if (dialect == Dialect.SQLITE) {
                return getSqliteTableNamesFallback();
            }
            throw e;
        }
    }

    /**
     * Check if a table exists (case-insensitive).
     */
    public boolean tableExists(String tableName) throws SQLException {
        if (tableName == null || tableName.isEmpty()) return false;

        // Try quick metadata lookup first
        DatabaseMetaData metaData = connection.getMetaData();
        String schema = resolveSchemaForMetadata();

        try {
            // Use getTables with explicit name pattern to be efficient
            try (ResultSet rs = metaData.getTables(null, schema, tableName, new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tname = rs.getString("TABLE_NAME");
                    String tschema = rs.getString("TABLE_SCHEM");
                    if (tname != null && tname.equalsIgnoreCase(tableName) && !isSystemSchema(tschema)) {
                        return true;
                    }
                }
            }
            // Try case-insensitive check via getTableNames if the above returned nothing
            Set<String> tables = getTableNames();
            return tables.contains(tableName.toLowerCase());
        } catch (SQLException e) {
            if (dialect == Dialect.SQLITE) {
                // SQLite fallback
                return sqliteTableExistsFallback(tableName);
            }
            throw e;
        }
    }

    /**
     * Get the schema definition of a table. Returns null if the table doesn't exist or has no columns.
     */
    public TableDefinition getTableDefinition(String tableName) throws SQLException {
        if (tableName == null || tableName.isEmpty()) return null;

        DatabaseMetaData metaData = connection.getMetaData();
        String schema = resolveSchemaForMetadata();

        // Try metadata-based approach
        try {
            // First locate the actual table name and schema via getTables
            String actualTable = null;
            String foundSchema = null;
            try (ResultSet tablesRs = metaData.getTables(null, schema, tableName, new String[]{"TABLE", "VIEW"})) {
                while (tablesRs.next()) {
                    String tname = tablesRs.getString("TABLE_NAME");
                    String tschema = tablesRs.getString("TABLE_SCHEM");
                    if (tname != null && tname.equalsIgnoreCase(tableName) && !isSystemSchema(tschema)) {
                        actualTable = tname;
                        foundSchema = tschema;
                        break;
                    }
                }
            }

            if (actualTable == null) {
                // maybe uppercase/lowercase variant
                try (ResultSet tablesRs = metaData.getTables(null, schema, tableName.toUpperCase(), new String[]{"TABLE", "VIEW"})) {
                    while (tablesRs.next()) {
                        String tname = tablesRs.getString("TABLE_NAME");
                        String tschema = tablesRs.getString("TABLE_SCHEM");
                        if (tname != null && tname.equalsIgnoreCase(tableName) && !isSystemSchema(tschema)) {
                            actualTable = tname;
                            foundSchema = tschema;
                            break;
                        }
                    }
                }
            }

            if (actualTable == null) {
                // table not found via metadata
                if (dialect == Dialect.SQLITE) {
                    return getTableDefinitionSqliteFallback(tableName);
                }
                return null;
            }

            // gather primary keys
            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet pkRs = metaData.getPrimaryKeys(null, foundSchema, actualTable)) {
                while (pkRs.next()) {
                    String col = pkRs.getString("COLUMN_NAME");
                    if (col != null) primaryKeys.add(col.toLowerCase());
                }
            } catch (SQLException ignored) {
                // Some drivers may not return PK info via metadata
            }

            // gather columns
            List<ColumnDefinition> columns = new ArrayList<>();
            try (ResultSet colRs = metaData.getColumns(null, foundSchema, actualTable, "%")) {
                while (colRs.next()) {
                    // Filter out system columns if any
                    String tname = colRs.getString("TABLE_NAME");
                    String tschema = colRs.getString("TABLE_SCHEM");
                    if (tname == null || isSystemSchema(tschema)) continue;
                    columns.add(buildColumnFromResultSet(colRs, primaryKeys));
                }
            }

            if (columns.isEmpty()) {
                // If metadata returned no columns, attempt sqlite fallback or return null
                if (dialect == Dialect.SQLITE) {
                    return getTableDefinitionSqliteFallback(actualTable);
                }
                return null;
            }

            TableDefinition table = new TableDefinition(actualTable);
            for (ColumnDefinition c : columns) table.addColumn(c);
            return table;

        } catch (SQLException e) {
            if (dialect == Dialect.SQLITE) {
                return getTableDefinitionSqliteFallback(tableName);
            }
            throw e;
        }
    }

    // -----------------------
    // Helper methods / fallbacks
    // -----------------------

    private String resolveSchemaForMetadata() {
        // Choose schema argument for DatabaseMetaData calls depending on dialect and driver behavior.
        // H2: use connection.getSchema() or "PUBLIC" when null.
        // PostgreSQL / MySQL: leave schema null (driver resolves default).
        // SQLite: should pass null to avoid driver errors.
        try {
            if (dialect == Dialect.H2) {
                String s = connection.getSchema();
                if (s == null) return "PUBLIC";
                return s;
            }
        } catch (Throwable ignored) {
            // ignore and fall through
        }
        // For SQLite and generic, use null
        return null;
    }

    private boolean isSystemSchema(String schema) {
        if (schema == null) return false;
        String s = schema.toUpperCase(Locale.ROOT);
        return s.startsWith("INFORMATION_SCHEMA") || s.startsWith("PG_") || s.equals("SYS") || s.equals("SYSTEM");
    }

    private boolean isSystemTableName(String tableName) {
        if (tableName == null) return true;
        String tn = tableName.toLowerCase(Locale.ROOT);
        // SQLite internal tables start with sqlite_
        if (tn.startsWith("sqlite_")) return true;
        // H2 information schema tables
        if (tn.startsWith("information_schema")) return true;
        return false;
    }

    private Set<String> getSqliteTableNamesFallback() throws SQLException {
        Set<String> result = new HashSet<>();
        String sql = "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name = rs.getString(1);
                if (name != null) result.add(name.toLowerCase());
            }
        }
        return result;
    }

    private boolean sqliteTableExistsFallback(String tableName) throws SQLException {
        String sql = "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private TableDefinition getTableDefinitionSqliteFallback(String tableName) throws SQLException {
        // Check existence
        if (!sqliteTableExistsFallback(tableName)) return null;

        List<ColumnDefinition> columns = new ArrayList<>();

        // Use PRAGMA table_info to get column names, types, notnull, pk, default
        String pragma = "PRAGMA table_info(\"" + tableName.replace("\"", "\"\"") + "\")";
        try (PreparedStatement ps = connection.prepareStatement(pragma);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String colName = rs.getString("name");
                String type = rs.getString("type"); // e.g. "VARCHAR(255)" or "INTEGER"
                int notnull = rs.getInt("notnull"); // 0 or 1
                String dfltValue = rs.getString("dflt_value");
                int pk = rs.getInt("pk"); // 0 or 1 (or >1 for composite)

                ColumnDefinition.Builder b = new ColumnDefinition.Builder()
                        .name(colName)
                        .sqlType(type == null ? "TEXT" : type)
                        .nullable(notnull == 0)
                        .primaryKey(pk != 0)
                        .autoIncrement(false) // SQLite doesn't expose auto-increment in pragma; detect from type if possible
                        .defaultValue(dfltValue);

                int length = parseLengthFromType(type);
                if (length > 0) b.length(length);

                // Rough auto-increment detection: in SQLite an INTEGER PRIMARY KEY is alias for rowid and can be autoinc.
                if (pk != 0 && type != null && type.equalsIgnoreCase("INTEGER")) {
                    b.autoIncrement(true);
                }

                columns.add(b.build());
            }
        }

        if (columns.isEmpty()) return null;
        TableDefinition table = new TableDefinition(tableName);
        for (ColumnDefinition c : columns) table.addColumn(c);
        return table;
    }

    // -----------------------
    // Column/Entity helpers
    // -----------------------

    /**
     * Build a ColumnDefinition from a ResultSet row returned by DatabaseMetaData.getColumns(...)
     */
    private ColumnDefinition buildColumnFromResultSet(ResultSet rs, Set<String> primaryKeys) throws SQLException {
        String columnName = rs.getString("COLUMN_NAME");
        String typeName = rs.getString("TYPE_NAME");
        int columnSize = -1;
        try {
            columnSize = rs.getInt("COLUMN_SIZE");
            if (rs.wasNull()) columnSize = -1;
        } catch (SQLException ignored) {
            columnSize = -1;
        }
        boolean nullable = true;
        try {
            int nullableVal = rs.getInt("NULLABLE");
            nullable = (nullableVal == DatabaseMetaData.columnNullable);
        } catch (SQLException ignored) {
            // leave nullable = true
        }
        String defaultValue = null;
        try {
            defaultValue = rs.getString("COLUMN_DEF");
        } catch (SQLException ignored) {}

        boolean isPrimaryKey = columnName != null && primaryKeys.contains(columnName.toLowerCase());
        boolean isAutoIncrement = false;
        try {
            String ai = rs.getString("IS_AUTOINCREMENT");
            isAutoIncrement = "YES".equalsIgnoreCase(ai);
        } catch (SQLException ignored) {
            // ignore
        }

        ColumnDefinition.Builder builder = new ColumnDefinition.Builder()
                .name(columnName)
                .sqlType(typeName == null ? inferSqlTypeFromSize(columnSize) : typeName)
                .nullable(nullable)
                .primaryKey(isPrimaryKey)
                .autoIncrement(isAutoIncrement)
                .defaultValue(defaultValue);

        if (columnSize > 0) builder.length(columnSize);

        return builder.build();
    }

    /**
     * Build a TableDefinition from entity metadata (what the schema should be).
     */
    public TableDefinition buildTableDefinitionFromEntity(Class<?> entityClass) {
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        TableDefinition table = new TableDefinition(metadata.getTableName());

        for (Field field : metadata.getFields()) {
            String columnName = metadata.getColumnName(field);
            boolean isPrimaryKey = field.equals(metadata.getIdField());
            boolean isAutoIncrement = isPrimaryKey && metadata.isAutoIncrement();

            // Determine SQL type from Java type
            String sqlType = TypeMapper.javaTypeToSqlType(field.getType());
            Integer length = null;
            boolean nullable = true;
            String referencedTable = null;
            String referencedColumn = null;

            // Check for @Column annotation
            if (field.isAnnotationPresent(Column.class)) {
                Column columnAnnotation = field.getAnnotation(Column.class);
                nullable = columnAnnotation.nullable();

                // Use explicit type if specified, otherwise infer from Java type
                String explicitType = columnAnnotation.type().trim();
                if (!explicitType.isEmpty()) {
                    sqlType = explicitType;
                }

                if (field.getType() == String.class) {
                    length = columnAnnotation.length();
                }
            }

            // Primary key never nullable
            if (field.isAnnotationPresent(Id.class)) {
                nullable = false;
            }

            // Handle @ManyToOne - foreign key columns
            if (field.isAnnotationPresent(ManyToOne.class)) {
                ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
                // Foreign key is typically the ID type of the related entity; fall back to BIGINT
                sqlType = "BIGINT";
                nullable = true;
                
                // Determine referenced table and column
                // Determine referenced table and column from field type
                Class<?> targetEntityClass = field.getType();
                if (targetEntityClass != null) {
                    EntityMetadata targetMetadata = EntityMetadata.of(targetEntityClass);
                    referencedTable = targetMetadata.getTableName();
                    referencedColumn = targetMetadata.getIdColumnName();
                } else {
                    // Fallback to field name convention
                    referencedTable = field.getType().getSimpleName().toLowerCase();
                    referencedColumn = "id";
                }

            }

            ColumnDefinition.Builder builder = new ColumnDefinition.Builder()
                    .name(columnName)
                    .sqlType(sqlType)
                    .nullable(nullable)
                    .primaryKey(isPrimaryKey)
                    .autoIncrement(isAutoIncrement)
                    .length(length);
            
            if (referencedTable != null && referencedColumn != null) {
                builder.referencedTable(referencedTable)
                       .referencedColumn(referencedColumn);
            }
            
            ColumnDefinition column = builder.build();
            table.addColumn(column);
        }

        return table;
    }


    // -----------------------
    // Type mapping helpers
    // -----------------------


    private int parseLengthFromType(String type) {
        if (type == null) return -1;
        int start = type.indexOf('(');
        int end = type.indexOf(')');
        if (start >= 0 && end > start) {
            String inside = type.substring(start + 1, end).trim();
            try {
                return Integer.parseInt(inside.split(",")[0].trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return -1;
    }

    private String inferSqlTypeFromSize(int columnSize) {
        if (columnSize <= 0) return "VARCHAR";
        if (columnSize >= Integer.MAX_VALUE / 2) return "TEXT";
        return "VARCHAR";
    }
}
