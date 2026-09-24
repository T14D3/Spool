package de.t14d3.spool.migration;

import de.t14d3.spool.mapping.UniqueConstraintDef;
import de.t14d3.spool.query.Dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates SQL statements from schema changes.
 * Supports multiple database dialects.
 */
public class SqlGenerator {

    private final Dialect dialect;

    public SqlGenerator(Dialect dialect) {
        this.dialect = dialect;
    }

    private String q(String identifier) {
        return dialect.quoteIdentifier(identifier);
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
            case ADD_UNIQUE_CONSTRAINT -> generateAddUniqueConstraint(change.getTableName(), change.getUniqueConstraint());
            case DROP_UNIQUE_CONSTRAINT -> generateDropUniqueConstraint(change.getTableName(), change.getUniqueConstraint());
        };
    }

    /**
     * Generate CREATE TABLE statement.
     */
    private String generateCreateTable(TableDefinition table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(q(table.getName())).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        List<String> foreignKeyConstraints = new ArrayList<>();
        List<String> uniqueKeyConstraints = new ArrayList<>();

        for (ColumnDefinition column : table.getColumns().values()) {
            StringBuilder colDef = new StringBuilder();
            colDef.append("  ").append(q(column.getName())).append(" ");
            colDef.append(getColumnTypeDefinition(column));

            if (column.isPrimaryKey()) {
                if (dialect == Dialect.SQLITE && column.isAutoIncrement()) {
                    // SQLite has no AUTO_INCREMENT: an auto-generated primary key must
                    // be declared inline as "INTEGER PRIMARY KEY AUTOINCREMENT" (a
                    // rowid alias). A table-level PRIMARY KEY constraint on a plain
                    // INTEGER column does NOT auto-generate values (the column would
                    // stay NULL on insert), and combining both forms raises
                    // "table has more than one primary key". Hence the column is
                    // excluded from the table-level PRIMARY KEY clause below.
                    colDef.append(" PRIMARY KEY AUTOINCREMENT");
                } else {
                    primaryKeys.add(q(column.getName()));
                    if (column.isAutoIncrement()) {
                        colDef.append(" ").append(getAutoIncrementKeyword());
                    }
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

        // Add unique constraints inline. MySQL/MariaDB use the
        // dialect-specific "UNIQUE KEY name (cols)" form; PostgreSQL/H2
        // use the standard "CONSTRAINT name UNIQUE (cols)"; SQLite gets
        // separate CREATE UNIQUE INDEX statements emitted by
        // MigrationManager, since SQLite does not allow table-level
        // UNIQUE constraints in CREATE TABLE on every supported JDBC
        // driver and the additive path is the portable one.
        if (dialect != Dialect.SQLITE) {
            for (UniqueConstraintDef uc : table.getUniqueConstraints()) {
                uniqueKeyConstraints.add(generateInlineUniqueConstraint(uc));
            }
        }

        // Add foreign key constraints
        for (ColumnDefinition column : table.getColumns().values()) {
            if (column.isForeignKey()) {
                String constraint = String.format("  FOREIGN KEY (%s) REFERENCES %s(%s)",
                    q(column.getName()), q(column.getReferencedTable()), q(column.getReferencedColumn()));
                foreignKeyConstraints.add(constraint);
            }
        }

        if (!foreignKeyConstraints.isEmpty()) {
            sql.append(",\n").append(String.join(",\n", foreignKeyConstraints));
        }

        if (!uniqueKeyConstraints.isEmpty()) {
            sql.append(",\n").append(String.join(",\n", uniqueKeyConstraints));
        }

        sql.append("\n)");

        return sql.toString();
    }

    /**
     * Emits the inline {@code UNIQUE} form for the dialect:
     * MySQL/MariaDB: {@code UNIQUE KEY name (cols)}.
     * PostgreSQL/H2: {@code CONSTRAINT name UNIQUE (cols)}.
     */
    private String generateInlineUniqueConstraint(UniqueConstraintDef uc) {
        String cols = uc.getColumns().stream().map(this::q).collect(Collectors.joining(", "));
        if (dialect == Dialect.MYSQL) {
            return "  UNIQUE KEY " + q(uc.getName()) + " (" + cols + ")";
        }
        return "  CONSTRAINT " + q(uc.getName()) + " UNIQUE (" + cols + ")";
    }

    /**
     * Emits {@code CREATE UNIQUE INDEX} for the additive-diff path. Used
     * for both SQLite (where the CREATE TABLE form is omitted) and the
     * cross-dialect case where a constraint was added to an existing
     * table after it was created.
     */
    private String generateAddUniqueConstraint(String tableName, UniqueConstraintDef uc) {
        String cols = uc.getColumns().stream().map(this::q).collect(Collectors.joining(", "));
        // MySQL/MariaDB accept "CREATE UNIQUE INDEX name ON table (cols)".
        // PostgreSQL/H2/SQLite/GENERIC accept the same. Idempotency is
        // handled by the migration manager: when introspecting an existing
        // table, a unique index with the same name is already present, so
        // the diff does not re-emit the ADD.
        return "CREATE UNIQUE INDEX " + q(uc.getName()) + " ON " + q(tableName) + " (" + cols + ")";
    }

    /**
     * Returns the additional {@code CREATE UNIQUE INDEX} statements that
     * must be issued after a CREATE TABLE for dialects (SQLite today)
     * where the table-level CREATE form omits unique constraints. The
     * returned list is empty for dialects that emit unique constraints
     * inline; callers that mix dialects on the same connection can
     * always call this and execute the returned statements unconditionally
     * (CREATE UNIQUE INDEX is a no-op for an index that already exists
     * when wrapped in IF NOT EXISTS, which most drivers do NOT support,
     * so the migration manager's introspection is the source of truth
     * for "already exists" and this helper only emits the additive case).
     */
    public List<String> generatePostCreateTableStatements(TableDefinition table) {
        if (dialect == Dialect.SQLITE) {
            List<String> statements = new ArrayList<>();
            for (UniqueConstraintDef uc : table.getUniqueConstraints()) {
                statements.add(generateAddUniqueConstraint(table.getName(), uc));
            }
            return statements;
        }
        return List.of();
    }

    /**
     * Emits {@code DROP INDEX}. MySQL/MariaDB require the table-qualified
     * form {@code DROP INDEX name ON table}; PostgreSQL/SQLite/H2 accept
     * the unqualified form.
     */
    private String generateDropUniqueConstraint(String tableName, UniqueConstraintDef uc) {
        if (dialect == Dialect.MYSQL) {
            return "DROP INDEX " + q(uc.getName()) + " ON " + q(tableName);
        }
        return "DROP INDEX " + q(uc.getName());
    }

    /**
     * Generate DROP TABLE statement.
     */
    private String generateDropTable(String tableName) {
        return "DROP TABLE IF EXISTS " + q(tableName);
    }

    /**
     * Generate ALTER TABLE ADD COLUMN statement.
     */
    private String generateAddColumn(String tableName, ColumnDefinition column) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(q(tableName));
        sql.append(" ADD COLUMN ").append(q(column.getName())).append(" ");   
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
        return "ALTER TABLE " + q(tableName) + " DROP COLUMN " + q(column.getName());
    }

    /**
     * Generate ALTER TABLE MODIFY/ALTER COLUMN statement.
     */
    private String generateModifyColumn(String tableName, ColumnDefinition newColumn, ColumnDefinition oldColumn) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(q(tableName));

        switch (dialect) {
            case MYSQL:
                sql.append(" MODIFY COLUMN ").append(q(newColumn.getName())).append(" ");
                break;
            case POSTGRESQL:
                sql.append(" ALTER COLUMN ").append(q(newColumn.getName())).append(" TYPE ");
                break;
            case SQLITE:
                return "-- SQLite does not support ALTER COLUMN. Manual migration required for: " +
                        tableName + "." + newColumn.getName();
            default:
                sql.append(" ALTER COLUMN ").append(q(newColumn.getName())).append(" ");
        }

        sql.append(getColumnTypeDefinition(newColumn));

        if (!newColumn.isNullable()) {
            if (dialect == Dialect.POSTGRESQL) {
                // PostgreSQL needs separate statement for NOT NULL
                return sql.toString() + ";\nALTER TABLE " + q(tableName) +
                        " ALTER COLUMN " + q(newColumn.getName()) + " SET NOT NULL";
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
        type = mapTypeForDialect(dialect, type);

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

        // A bare DECIMAL/NUMERIC defaults to scale 0 on H2/MySQL/PostgreSQL
        // and silently truncates fractions. Emit an explicit, generous
        // precision/scale so BigDecimal values round-trip; override with
        // @Column(type = "DECIMAL(p,s)") for a specific scale.
        if (column.getLength() == null && type != null
                && (type.equalsIgnoreCase("DECIMAL") || type.equalsIgnoreCase("NUMERIC"))) {
            return type + "(38, 18)";
        }

        // MySQL/MariaDB TIMESTAMP defaults to second precision. Use
        // microseconds so LocalDateTime/Instant/OffsetDateTime survive a
        // round-trip; override with @Column(type = "TIMESTAMP").
        if (dialect == Dialect.MYSQL && type != null
                && type.equalsIgnoreCase("TIMESTAMP")) {
            return "TIMESTAMP(6)";
        }

        return type;
    }

    /**
     * Map a generic SQL type to the type name the dialect actually stores.
     * Used both when generating DDL and when comparing entity metadata
     * against introspected columns.
     */
    static String mapTypeForDialect(Dialect dialect, String type) {
        if (type == null) {
            return null;
        }
        if (dialect == Dialect.SQLITE) {
            return mapToSqliteType(type);
        }
        return type;
    }

    /**
     * Map SQL types to SQLite types.
     */
    static String mapToSqliteType(String type) {
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
                return "AUTOINCREMENT"; // only used inline: "INTEGER PRIMARY KEY AUTOINCREMENT"
            case H2:
                return "AUTO_INCREMENT";
            default:
                return "AUTO_INCREMENT";
        }
    }

}
