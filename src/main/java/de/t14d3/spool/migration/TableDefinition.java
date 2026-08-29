package de.t14d3.spool.migration;

import de.t14d3.spool.mapping.UniqueConstraintDef;

import java.util.*;

/**
 * Represents the definition of a database table.
 */
public class TableDefinition {
    private final String name;
    private final Map<String, ColumnDefinition> columns;
    private final List<String> primaryKeyColumns;
    private final List<UniqueConstraintDef> uniqueConstraints;

    public TableDefinition(String name) {
        this.name = name;
        this.columns = new LinkedHashMap<>();
        this.primaryKeyColumns = new ArrayList<>();
        this.uniqueConstraints = new ArrayList<>();
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

    /**
     * Adds a unique constraint to this table definition.
     */
    public void addUniqueConstraint(UniqueConstraintDef constraint) {
        uniqueConstraints.add(constraint);
    }

    /**
     * Returns the unique constraints declared on this table, in insertion
     * order. May be empty.
     */
    public List<UniqueConstraintDef> getUniqueConstraints() {
        return Collections.unmodifiableList(uniqueConstraints);
    }

    @Override
    public String toString() {
        return "TableDefinition{" +
                "name='" + name + '\'' +
                ", columns=" + columns.keySet() +
                ", primaryKeyColumns=" + primaryKeyColumns +
                ", uniqueConstraints=" + uniqueConstraints +
                '}';
    }
}
