package de.t14d3.spool.migration;

import de.t14d3.spool.mapping.UniqueConstraintDef;
import de.t14d3.spool.query.Dialect;

import java.util.*;

/**
 * Represents the differences between two schemas (expected vs actual).
 * Used to generate migration SQL statements.
 */
public class SchemaDiff {

    /**
     * Types of schema changes.
     */
    public enum ChangeType {
        CREATE_TABLE,
        DROP_TABLE,
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        ADD_UNIQUE_CONSTRAINT,
        DROP_UNIQUE_CONSTRAINT
    }

    /**
     * Represents a single schema change.
     */
    public static class SchemaChange {
        private final ChangeType type;
        private final String tableName;
        private final ColumnDefinition column;
        private final ColumnDefinition oldColumn; // For MODIFY_COLUMN
        private final TableDefinition tableDefinition; // For CREATE_TABLE
        private final UniqueConstraintDef uniqueConstraint; // For ADD/DROP_UNIQUE_CONSTRAINT

        private SchemaChange(ChangeType type, String tableName, ColumnDefinition column,
                             ColumnDefinition oldColumn, TableDefinition tableDefinition,
                             UniqueConstraintDef uniqueConstraint) {
            this.type = type;
            this.tableName = tableName;
            this.column = column;
            this.oldColumn = oldColumn;
            this.tableDefinition = tableDefinition;
            this.uniqueConstraint = uniqueConstraint;
        }

        public static SchemaChange createTable(TableDefinition table) {
            return new SchemaChange(ChangeType.CREATE_TABLE, table.getName(), null, null, table, null);
        }

        public static SchemaChange dropTable(String tableName) {
            return new SchemaChange(ChangeType.DROP_TABLE, tableName, null, null, null, null);
        }

        public static SchemaChange addColumn(String tableName, ColumnDefinition column) {
            return new SchemaChange(ChangeType.ADD_COLUMN, tableName, column, null, null, null);
        }

        public static SchemaChange dropColumn(String tableName, ColumnDefinition column) {
            return new SchemaChange(ChangeType.DROP_COLUMN, tableName, column, null, null, null);
        }

        public static SchemaChange modifyColumn(String tableName, ColumnDefinition newColumn, ColumnDefinition oldColumn) {
            return new SchemaChange(ChangeType.MODIFY_COLUMN, tableName, newColumn, oldColumn, null, null);
        }

        public static SchemaChange addUniqueConstraint(String tableName, UniqueConstraintDef constraint) {
            return new SchemaChange(ChangeType.ADD_UNIQUE_CONSTRAINT, tableName, null, null, null, constraint);
        }

        public static SchemaChange dropUniqueConstraint(String tableName, UniqueConstraintDef constraint) {
            return new SchemaChange(ChangeType.DROP_UNIQUE_CONSTRAINT, tableName, null, null, null, constraint);
        }

        public ChangeType getType() {
            return type;
        }

        public String getTableName() {
            return tableName;
        }

        public ColumnDefinition getColumn() {
            return column;
        }

        public ColumnDefinition getOldColumn() {
            return oldColumn;
        }

        public TableDefinition getTableDefinition() {
            return tableDefinition;
        }

        public UniqueConstraintDef getUniqueConstraint() {
            return uniqueConstraint;
        }

        @Override
        public String toString() {
            return "SchemaChange{" +
                    "type=" + type +
                    ", tableName='" + tableName + '\'' +
                    (column != null ? ", column=" + column.getName() : "") +
                    (uniqueConstraint != null ? ", constraint=" + uniqueConstraint.getName() : "") +
                    '}';
        }
    }

    private final List<SchemaChange> changes;

    public SchemaDiff() {
        this.changes = new ArrayList<>();
    }

    public void addChange(SchemaChange change) {
        changes.add(change);
    }

    public List<SchemaChange> getChanges() {
        return Collections.unmodifiableList(changes);
    }

    public boolean hasChanges() {
        return !changes.isEmpty();
    }

    /**
     * Compare expected schema (from entities) with actual schema (from database).
     *
     * @param expected The expected table definitions from entity metadata
     * @param actual   The actual table definitions from the database
     * @return A SchemaDiff containing all necessary changes
     */
    public static SchemaDiff compare(Map<String, TableDefinition> expected, Map<String, TableDefinition> actual) {
        return compare(expected, actual, Dialect.GENERIC);
    }

    /**
     * Compare expected schema (from entities) with actual schema (from database),
     * normalizing type names for the given dialect (SQLite stores
     * {@code BIGINT} as {@code INTEGER}, {@code VARCHAR} as {@code TEXT}, and
     * so on).
     */
    public static SchemaDiff compare(Map<String, TableDefinition> expected,
                                     Map<String, TableDefinition> actual,
                                     Dialect dialect) {
        SchemaDiff diff = new SchemaDiff();

        // Find tables to create (in expected but not in actual)
        for (Map.Entry<String, TableDefinition> entry : expected.entrySet()) {
            String tableName = entry.getKey().toLowerCase();
            TableDefinition expectedTable = entry.getValue();

            if (!actual.containsKey(tableName)) {
                diff.addChange(SchemaChange.createTable(expectedTable));
            } else {
                // Table exists, compare columns
                TableDefinition actualTable = actual.get(tableName);
                compareColumns(diff, expectedTable, actualTable, dialect);
                compareUniqueConstraints(diff, expectedTable, actualTable);
            }
        }

        // Find tables to drop (in actual but not in expected) - optional, disabled by default
        // Uncomment if you want automatic table drops
        // for (String tableName : actual.keySet()) {
        //     if (!expected.containsKey(tableName.toLowerCase())) {
        //         diff.addChange(SchemaChange.dropTable(tableName));
        //     }
        // }

        return diff;
    }

    /**
     * Compare columns between expected and actual table definitions.
     */
    private static void compareColumns(SchemaDiff diff, TableDefinition expected, TableDefinition actual,
                                       Dialect dialect) {
        String tableName = expected.getName();

        // Find columns to add (case-insensitive comparison)
        for (Map.Entry<String, ColumnDefinition> entry : expected.getColumns().entrySet()) {
            String columnName = entry.getKey().toLowerCase();
            ColumnDefinition expectedColumn = entry.getValue();

            if (!actual.hasColumn(columnName)) {
                diff.addChange(SchemaChange.addColumn(tableName, expectedColumn));
            } else {
                // Column exists, check if it needs modification
                ColumnDefinition actualColumn = actual.getColumn(columnName);
                if (needsModification(expectedColumn, actualColumn, dialect)) {
                    diff.addChange(SchemaChange.modifyColumn(tableName, expectedColumn, actualColumn));
                }
            }
        }

        // Find columns to drop (in actual but not in expected) - disabled by default
        // Uncomment if you want automatic column drops
        // for (String columnName : actual.getColumnNames()) {
        //     if (!expected.hasColumn(columnName)) {
        //         diff.addChange(SchemaChange.dropColumn(tableName, actual.getColumn(columnName)));
        //     }
        // }
    }

    /**
     * Compares unique constraints between expected and actual. Emits
     * {@link ChangeType#ADD_UNIQUE_CONSTRAINT} for every expected constraint
     * missing from the actual table, and
     * {@link ChangeType#DROP_UNIQUE_CONSTRAINT} for every actual constraint
     * that is no longer declared (column drop is disabled by default, so
     * the diff is one-way additive here as well).
     *
     * <p>Databases may rename a declared index on materialization
     * (H2 in particular appends {@code _INDEX_A} to inline
     * {@code CONSTRAINT ... UNIQUE} clauses) and may also uppercase
     * column names in introspected metadata (H2's default for
     * unquoted identifiers). The comparison matches by column set
     * (case-insensitive, order-sensitive) so the diff stays empty
     * after a fresh migrate regardless of the database's naming
     * choices.</p>
     */
    private static void compareUniqueConstraints(SchemaDiff diff, TableDefinition expected, TableDefinition actual) {
        String tableName = expected.getName();

        // Index by column set so vendor-renamed indexes still match.
        Map<String, UniqueConstraintDef> actualByColumns = new LinkedHashMap<>();
        for (UniqueConstraintDef uc : actual.getUniqueConstraints()) {
            actualByColumns.put(columnsKey(uc.getColumns()), uc);
        }

        for (UniqueConstraintDef expectedUc : expected.getUniqueConstraints()) {
            UniqueConstraintDef actualUc = actualByColumns.get(columnsKey(expectedUc.getColumns()));
            if (actualUc != null) continue; // already materialized, regardless of name
            diff.addChange(SchemaChange.addUniqueConstraint(tableName, expectedUc));
        }

        // Optional: emit DROP for unexpected unique constraints. Disabled
        // to match the existing one-way additive column-diff policy; flip
        // to true if you want strict convergence.
        boolean dropUnexpected = false;
        if (dropUnexpected) {
            Set<String> expectedColumnKeys = new HashSet<>();
            for (UniqueConstraintDef uc : expected.getUniqueConstraints()) {
                expectedColumnKeys.add(columnsKey(uc.getColumns()));
            }
            for (UniqueConstraintDef actualUc : actual.getUniqueConstraints()) {
                if (!expectedColumnKeys.contains(columnsKey(actualUc.getColumns()))) {
                    diff.addChange(SchemaChange.dropUniqueConstraint(tableName, actualUc));
                }
            }
        }
    }

    private static String columnsKey(List<String> columns) {
        // Canonical, order-sensitive form: "col1|col2|col3" with all
        // entries lowercased. The columns within a unique index are
        // inherently ordered (composite index semantics), so we do not
        // sort them - but we normalize case so the comparison survives
        // databases that uppercase unquoted identifiers (H2 default).
        return columns.stream()
                .map(c -> c.toLowerCase(java.util.Locale.ROOT))
                .collect(java.util.stream.Collectors.joining("|"));
    }

    /**
     * Check if a column needs modification.
     */
    private static boolean needsModification(ColumnDefinition expected, ColumnDefinition actual, Dialect dialect) {
        // Compare SQL types (normalize for comparison)
        String expectedType = normalizeType(expected.getSqlType(), dialect);
        String actualType = normalizeType(actual.getSqlType(), dialect);

        if (!expectedType.equals(actualType)) {
            return true;
        }

        // Compare nullability
        if (expected.isNullable() != actual.isNullable()) {
            return true;
        }

        // Compare length for VARCHAR types. SQLite has no meaningful column
        // length (all text is TEXT) and ignores the declared length in DDL.
        if (dialect != Dialect.SQLITE
                && expected.getLength() != null && actual.getLength() != null) {
            if (!expected.getLength().equals(actual.getLength())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Normalize SQL type names for comparison.
     */
    private static String normalizeType(String type, Dialect dialect) {
        if (type == null) return "";
        type = SqlGenerator.mapTypeForDialect(dialect, type);
        type = type.toUpperCase();

        // Normalize common type variations
        if (type.equals("INT")) return "INTEGER";
        if (type.equals("INT4")) return "INTEGER";
        if (type.equals("INT8")) return "BIGINT";
        if (type.equals("BOOL")) return "BOOLEAN";
        if (type.startsWith("VARCHAR")) return "VARCHAR";
        if (type.startsWith("CHARACTER VARYING")) return "VARCHAR";
        // JDBC drivers report expanded names for these (H2 uses
        // "DOUBLE PRECISION" / "BINARY LARGE OBJECT").
        if (type.equals("DOUBLE PRECISION") || type.equals("FLOAT8") || type.equals("REAL")) {
            return "DOUBLE";
        }
        if (type.equals("BINARY LARGE OBJECT") || type.equals("BYTEA") || type.endsWith("BLOB")) {
            return "BLOB";
        }
        if (type.equals("NUMERIC")) return "DECIMAL";
        // MySQL/MariaDB store BOOLEAN as TINYINT(1) and report TINYINT.
        if (dialect == Dialect.MYSQL && type.equals("BOOLEAN")) return "TINYINT";

        return type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SchemaDiff{\n");
        for (SchemaChange change : changes) {
            sb.append("  ").append(change).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
