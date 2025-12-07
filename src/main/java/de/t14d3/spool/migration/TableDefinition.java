package de.t14d3.spool.migration;

import java.util.*;

/**
 * Represents the definition of a database table.
 */
public class TableDefinition {
    private final String name;
    private final Map<String, ColumnDefinition> columns;
    private final List<String> primaryKeyColumns;

    public TableDefinition(String name) {
        this.name = name;
        this.columns = new LinkedHashMap<>();
        this.primaryKeyColumns = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public Map<String, ColumnDefinition> getColumns() {
        return Collections.unmodifiableMap(columns);
    }

    public ColumnDefinition getColumn(String columnName) {
        return columns.get(columnName.toLowerCase());
    }

    public boolean hasColumn(String columnName) {
        return columns.containsKey(columnName.toLowerCase());
    }

    public void addColumn(ColumnDefinition column) {
        columns.put(column.getName().toLowerCase(), column);
        if (column.isPrimaryKey()) {
            primaryKeyColumns.add(column.getName());
        }
    }

    public List<String> getPrimaryKeyColumns() {
        return Collections.unmodifiableList(primaryKeyColumns);
    }

    public Set<String> getColumnNames() {
        return columns.keySet();
    }

    @Override
    public String toString() {
        return "TableDefinition{" +
                "name='" + name + '\'' +
                ", columns=" + columns.keySet() +
                ", primaryKeyColumns=" + primaryKeyColumns +
                '}';
    }
}
