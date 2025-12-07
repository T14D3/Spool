package de.t14d3.spool.migration;

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
        MODIFY_COLUMN
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

        private SchemaChange(ChangeType type, String tableName, ColumnDefinition column,
                             ColumnDefinition oldColumn, TableDefinition tableDefinition) {
            this.type = type;
            this.tableName = tableName;
            this.column = column;
            this.oldColumn = oldColumn;
            this.tableDefinition = tableDefinition;
        }

        public static SchemaChange createTable(TableDefinition table) {
            return new SchemaChange(ChangeType.CREATE_TABLE, table.getName(), null, null, table);
        }

        public static SchemaChange dropTable(String tableName) {
            return new SchemaChange(ChangeType.DROP_TABLE, tableName, null, null, null);
        }

        public static SchemaChange addColumn(String tableName, ColumnDefinition column) {
            return new SchemaChange(ChangeType.ADD_COLUMN, tableName, column, null, null);
        }

        public static SchemaChange dropColumn(String tableName, ColumnDefinition column) {
            return new SchemaChange(ChangeType.DROP_COLUMN, tableName, column, null, null);
        }

        public static SchemaChange modifyColumn(String tableName, ColumnDefinition newColumn, ColumnDefinition oldColumn) {
            return new SchemaChange(ChangeType.MODIFY_COLUMN, tableName, newColumn, oldColumn, null);
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

        @Override
        public String toString() {
            return "SchemaChange{" +
                    "type=" + type +
                    ", tableName='" + tableName + '\'' +
                    ", column=" + (column != null ? column.getName() : "null") +
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
                compareColumns(diff, expectedTable, actualTable);
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
    private static void compareColumns(SchemaDiff diff, TableDefinition expected, TableDefinition actual) {
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
                if (needsModification(expectedColumn, actualColumn)) {
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
     * Check if a column needs modification.
     */
    private static boolean needsModification(ColumnDefinition expected, ColumnDefinition actual) {
        // Compare SQL types (normalize for comparison)
        String expectedType = normalizeType(expected.getSqlType());
        String actualType = normalizeType(actual.getSqlType());

        if (!expectedType.equals(actualType)) {
            return true;
        }

        // Compare nullability
        if (expected.isNullable() != actual.isNullable()) {
            return true;
        }

        // Compare length for VARCHAR types
        if (expected.getLength() != null && actual.getLength() != null) {
            if (!expected.getLength().equals(actual.getLength())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Normalize SQL type names for comparison.
     */
    private static String normalizeType(String type) {
        if (type == null) return "";
        type = type.toUpperCase();

        // Normalize common type variations
        if (type.equals("INT")) return "INTEGER";
        if (type.equals("INT4")) return "INTEGER";
        if (type.equals("INT8")) return "BIGINT";
        if (type.equals("BOOL")) return "BOOLEAN";
        if (type.startsWith("VARCHAR")) return "VARCHAR";
        if (type.startsWith("CHARACTER VARYING")) return "VARCHAR";

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
