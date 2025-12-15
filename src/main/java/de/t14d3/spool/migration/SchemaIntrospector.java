package de.t14d3.spool.migration;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.mapping.EntityMetadata;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * Introspects database schema and entity metadata to build schema definitions.
 */
public class SchemaIntrospector {
    private final Connection connection;

    public SchemaIntrospector(Connection connection) {
        this.connection = connection;
    }

    /**
     * Get all table names from the database.
     * Only returns tables from the PUBLIC schema (user tables), not system tables.
     */
    public Set<String> getTableNames() throws SQLException {
        Set<String> tables = new HashSet<>();
        DatabaseMetaData metaData = connection.getMetaData();
        
        // Get the default schema - for H2 it's "PUBLIC"
        String schema = connection.getSchema();
        if (schema == null) {
            schema = "PUBLIC"; // H2 default
        }
        
        // Query tables only from the user schema, not system schemas
        try (ResultSet rs = metaData.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String tableSchema = rs.getString("TABLE_SCHEM");
                // Only include tables from PUBLIC schema (or null schema)
                // Exclude system schemas like INFORMATION_SCHEMA
                if (tableSchema == null || tableSchema.equalsIgnoreCase("PUBLIC") || 
                    tableSchema.equalsIgnoreCase(schema)) {
                    tables.add(rs.getString("TABLE_NAME").toLowerCase());
                }
            }
        }
        return tables;
    }

    /**
     * Check if a table exists in the database.
     */
    public boolean tableExists(String tableName) throws SQLException {
        Set<String> tables = getTableNames();
        // Check both lowercase and uppercase versions
        return tables.contains(tableName.toLowerCase()) || tables.contains(tableName.toUpperCase());
    }

    /**
     * Get the schema definition of a table from the database.
     * Returns null if the table does not exist or has no columns.
     * Only looks in the PUBLIC schema (user tables), not system schemas.
     */
    public TableDefinition getTableDefinition(String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        
        // Get the default schema - for H2 it's "PUBLIC"
        String schema = connection.getSchema();
        if (schema == null) {
            schema = "PUBLIC"; // H2 default
        }
        
        // First get primary keys - filter by schema
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet rs = metaData.getPrimaryKeys(null, schema, tableName.toUpperCase())) {
            while (rs.next()) {
                String foundTable = rs.getString("TABLE_NAME");
                String foundSchema = rs.getString("TABLE_SCHEM");
                if (foundTable != null && foundTable.equalsIgnoreCase(tableName) &&
                    (foundSchema == null || foundSchema.equalsIgnoreCase("PUBLIC") || foundSchema.equalsIgnoreCase(schema))) {
                    primaryKeys.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            }
        }

        // Get columns - filter by schema to exclude system tables
        List<ColumnDefinition> columns = new ArrayList<>();
        
        // Query with schema filter (H2 stores in uppercase)
        try (ResultSet rs = metaData.getColumns(null, schema, tableName.toUpperCase(), null)) {
            while (rs.next()) {
                String foundTable = rs.getString("TABLE_NAME");
                String foundSchema = rs.getString("TABLE_SCHEM");
                // Only include columns from PUBLIC schema (user tables)
                if (foundTable != null && foundTable.equalsIgnoreCase(tableName) &&
                    (foundSchema == null || foundSchema.equalsIgnoreCase("PUBLIC") || foundSchema.equalsIgnoreCase(schema))) {
                    columns.add(buildColumnFromResultSet(rs, primaryKeys));
                }
            }
        }

        // If no columns found, table doesn't exist in user schema
        if (columns.isEmpty()) {
            return null;
        }

        // Build table definition
        TableDefinition table = new TableDefinition(tableName);
        for (ColumnDefinition column : columns) {
            table.addColumn(column);
        }

        return table;
    }

    /**
     * Build a ColumnDefinition from a ResultSet row.
     */
    private ColumnDefinition buildColumnFromResultSet(ResultSet rs, Set<String> primaryKeys) throws SQLException {
        String columnName = rs.getString("COLUMN_NAME");
        String typeName = rs.getString("TYPE_NAME");
        int columnSize = rs.getInt("COLUMN_SIZE");
        boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
        String defaultValue = rs.getString("COLUMN_DEF");
        boolean isPrimaryKey = primaryKeys.contains(columnName.toLowerCase());
        boolean isAutoIncrement = "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"));

        return new ColumnDefinition.Builder()
                .name(columnName)
                .sqlType(typeName)
                .nullable(nullable)
                .primaryKey(isPrimaryKey)
                .autoIncrement(isAutoIncrement)
                .defaultValue(defaultValue)
                .length(columnSize > 0 ? columnSize : null)
                .build();
    }

    /**
     * Get the schema definition of a table from the database (old implementation).
     */
    private TableDefinition getTableDefinitionOld(String tableName) throws SQLException {
        if (!tableExists(tableName)) {
            return null;
        }

        TableDefinition table = new TableDefinition(tableName);
        DatabaseMetaData metaData = connection.getMetaData();

        // Get primary keys (try both cases for table name)
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName.toUpperCase())) {
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
        }
        if (primaryKeys.isEmpty()) {
            try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
                while (rs.next()) {
                    primaryKeys.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            }
        }

        // Get columns (try both cases for table name)
        ResultSet rs = metaData.getColumns(null, null, tableName.toUpperCase(), "%");
        if (!rs.next()) {
            rs.close();
            rs = metaData.getColumns(null, null, tableName, "%");
            if (!rs.next()) {
                rs.close();
                // No columns found - table might not exist or is empty
                // Return null to indicate table doesn't exist properly
                return null;
            }
        }
        
        return table;
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
            String sqlType = javaTypeToSqlType(field.getType());
            Integer length = null;
            boolean nullable = true;

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

            // Check for @Id annotation
            if (field.isAnnotationPresent(Id.class)) {
                nullable = false; // Primary keys are never nullable
            }

            // Handle @ManyToOne - foreign key columns
            if (field.isAnnotationPresent(ManyToOne.class)) {
                // Foreign key is typically the ID type of the related entity
                sqlType = "BIGINT"; // Assume Long IDs for foreign keys
                nullable = true; // Foreign keys can be nullable by default
            }

            ColumnDefinition column = new ColumnDefinition.Builder()
                    .name(columnName)
                    .sqlType(sqlType)
                    .nullable(nullable)
                    .primaryKey(isPrimaryKey)
                    .autoIncrement(isAutoIncrement)
                    .length(length)
                    .build();

            table.addColumn(column);
        }

        return table;
    }

    /**
     * Map Java types to SQL types.
     */
    private String javaTypeToSqlType(Class<?> javaType) {
        if (javaType == String.class) {
            return "VARCHAR";
        } else if (javaType == Long.class || javaType == long.class) {
            return "BIGINT";
        } else if (javaType == Integer.class || javaType == int.class) {
            return "INTEGER";
        } else if (javaType == Short.class || javaType == short.class) {
            return "SMALLINT";
        } else if (javaType == Byte.class || javaType == byte.class) {
            return "TINYINT";
        } else if (javaType == Double.class || javaType == double.class) {
            return "DOUBLE";
        } else if (javaType == Float.class || javaType == float.class) {
            return "FLOAT";
        } else if (javaType == Boolean.class || javaType == boolean.class) {
            return "BOOLEAN";
        } else if (javaType == java.util.Date.class || javaType == java.sql.Date.class) {
            return "DATE";
        } else if (javaType == java.sql.Timestamp.class) {
            return "TIMESTAMP";
        } else if (javaType == java.time.LocalDate.class) {
            return "DATE";
        } else if (javaType == java.time.LocalDateTime.class) {
            return "TIMESTAMP";
        } else if (javaType == byte[].class) {
            return "BLOB";
        } else if (javaType == java.math.BigDecimal.class) {
            return "DECIMAL";
        } else {
            // For entity types (relationships), use BIGINT for foreign keys
            return "BIGINT";
        }
    }
}
