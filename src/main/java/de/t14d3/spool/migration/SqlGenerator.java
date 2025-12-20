package de.t14d3.spool.migration;

import de.t14d3.spool.query.Dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates SQL statements from schema changes.
 * Supports multiple database dialects.
 */
public class SqlGenerator {

    private final Dialect dialect;

    public SqlGenerator(Dialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Generate SQL statements for all changes in a SchemaDiff.
     */
    public List<String> generateSql(SchemaDiff diff) {
        List<String> statements = new ArrayList<>();

        for (SchemaDiff.SchemaChange change : diff.getChanges()) {
            String sql = generateSql(change);
            if (sql != null && !sql.isEmpty()) {
                statements.add(sql);
            }
        }

        return statements;
    }

    /**
     * Generate SQL for a single schema change.
     */
    public String generateSql(SchemaDiff.SchemaChange change) {
        return switch (change.getType()) {
            case CREATE_TABLE -> generateCreateTable(change.getTableDefinition());
            case DROP_TABLE -> generateDropTable(change.getTableName());
            case ADD_COLUMN -> generateAddColumn(change.getTableName(), change.getColumn());
            case DROP_COLUMN -> generateDropColumn(change.getTableName(), change.getColumn());
            case MODIFY_COLUMN ->
                    generateModifyColumn(change.getTableName(), change.getColumn(), change.getOldColumn());
            default -> null;
        };
    }

    /**
     * Generate CREATE TABLE statement.
     */
    private String generateCreateTable(TableDefinition table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(table.getName()).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        List<String> foreignKeyConstraints = new ArrayList<>();

        for (ColumnDefinition column : table.getColumns().values()) {
            StringBuilder colDef = new StringBuilder();
            colDef.append("  ").append(column.getName()).append(" ");
            colDef.append(getColumnTypeDefinition(column));

            if (column.isPrimaryKey()) {
                primaryKeys.add(column.getName());
                if (column.isAutoIncrement()) {
                    colDef.append(" ").append(getAutoIncrementKeyword());
                }
            }

            if (!column.isNullable() && !column.isPrimaryKey()) {
                colDef.append(" NOT NULL");
            }

            if (column.getDefaultValue() != null) {
                colDef.append(" DEFAULT ").append(column.getDefaultValue());
            }

            columnDefs.add(colDef.toString());
        }

        sql.append(String.join(",\n", columnDefs));

        // Add primary key constraint
        if (!primaryKeys.isEmpty()) {
            sql.append(",\n  PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }

        // Add foreign key constraints
        for (ColumnDefinition column : table.getColumns().values()) {
            if (column.isForeignKey()) {
                String constraint = String.format("  FOREIGN KEY (%s) REFERENCES %s(%s)",
                    column.getName(), column.getReferencedTable(), column.getReferencedColumn());
                foreignKeyConstraints.add(constraint);
            }
        }

        if (!foreignKeyConstraints.isEmpty()) {
            sql.append(",\n").append(String.join(",\n", foreignKeyConstraints));
        }

        sql.append("\n)");

        return sql.toString();

    }

    /**
     * Generate DROP TABLE statement.
     */
    private String generateDropTable(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName;
    }

    /**
     * Generate ALTER TABLE ADD COLUMN statement.
     */
    private String generateAddColumn(String tableName, ColumnDefinition column) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName);
        sql.append(" ADD COLUMN ").append(column.getName()).append(" ");
        sql.append(getColumnTypeDefinition(column));

        if (!column.isNullable()) {
            sql.append(" NOT NULL");
        }

        if (column.getDefaultValue() != null) {
            sql.append(" DEFAULT ").append(column.getDefaultValue());
        }

        return sql.toString();
    }

    /**
     * Generate ALTER TABLE DROP COLUMN statement.
     */
    private String generateDropColumn(String tableName, ColumnDefinition column) {
        // SQLite doesn't support DROP COLUMN directly
        if (dialect == Dialect.SQLITE) {
            return "-- SQLite does not support DROP COLUMN. Manual migration required for: " +
                    tableName + "." + column.getName();
        }
        return "ALTER TABLE " + tableName + " DROP COLUMN " + column.getName();
    }

    /**
     * Generate ALTER TABLE MODIFY/ALTER COLUMN statement.
     */
    private String generateModifyColumn(String tableName, ColumnDefinition newColumn, ColumnDefinition oldColumn) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName);

        switch (dialect) {
            case MYSQL:
                sql.append(" MODIFY COLUMN ").append(newColumn.getName()).append(" ");
                break;
            case POSTGRESQL:
                sql.append(" ALTER COLUMN ").append(newColumn.getName()).append(" TYPE ");
                break;
            case SQLITE:
                return "-- SQLite does not support ALTER COLUMN. Manual migration required for: " +
                        tableName + "." + newColumn.getName();
            default:
                sql.append(" ALTER COLUMN ").append(newColumn.getName()).append(" ");
        }

        sql.append(getColumnTypeDefinition(newColumn));

        if (!newColumn.isNullable()) {
            if (dialect == Dialect.POSTGRESQL) {
                // PostgreSQL needs separate statement for NOT NULL
                return sql.toString() + ";\nALTER TABLE " + tableName +
                        " ALTER COLUMN " + newColumn.getName() + " SET NOT NULL";
            }
            sql.append(" NOT NULL");
        }

        return sql.toString();
    }

    /**
     * Get the column type definition including length.
     */
    private String getColumnTypeDefinition(ColumnDefinition column) {
        String type = column.getSqlType();

        // Handle type mapping for different dialects
        if (dialect == Dialect.SQLITE) {
            type = mapToSqliteType(type);
        }

        // MySQL requires a length for VARCHAR. Provide a sensible default when
        // the entity definition didn't specify one.
        if (dialect == Dialect.MYSQL && column.getLength() == null && type != null) {
            if (type.equalsIgnoreCase("VARCHAR")) {
                return "VARCHAR(255)";
            }
            // CHAR defaults to 1 in MySQL, but be explicit for consistency.
            if (type.equalsIgnoreCase("CHAR")) {
                return "CHAR(1)";
            }
        }

        // Add length for VARCHAR types
        if (column.getLength() != null &&
                (type.equalsIgnoreCase("VARCHAR") || type.equalsIgnoreCase("CHAR"))) {
            return type + "(" + column.getLength() + ")";
        }

        return type;
    }

    /**
     * Map SQL types to SQLite types.
     */
    private String mapToSqliteType(String type) {
        type = type.toUpperCase();
        if (type.equals("BIGINT") || type.equals("INTEGER") || type.equals("SMALLINT") ||
                type.equals("TINYINT") || type.equals("BOOLEAN")) {
            return "INTEGER";
        }
        if (type.equals("DOUBLE") || type.equals("FLOAT") || type.equals("DECIMAL")) {
            return "REAL";
        }
        if (type.startsWith("VARCHAR") || type.equals("CHAR") || type.equals("TEXT")) {
            return "TEXT";
        }
        if (type.equals("BLOB")) {
            return "BLOB";
        }
        return type;
    }

    /**
     * Get the auto-increment keyword for the current dialect.
     */
    private String getAutoIncrementKeyword() {
        switch (dialect) {
            case MYSQL:
                return "AUTO_INCREMENT";
            case POSTGRESQL:
                return ""; // PostgreSQL uses SERIAL type instead
            case SQLITE:
            case H2:
                return "AUTO_INCREMENT";
            default:
                return "AUTO_INCREMENT";
        }
    }

}
