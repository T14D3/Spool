package de.t14d3.spool.migration;

import java.util.Objects;

/**
 * Represents the definition of a database column.
 */
public class ColumnDefinition {
    private final String name;
    private final String sqlType;
    private final boolean nullable;
    private final boolean primaryKey;
    private final boolean autoIncrement;
    private final String defaultValue;
    private final Integer length;
    private final String referencedTable;
    private final String referencedColumn;
    private final boolean foreignKey;


    public ColumnDefinition(String name, String sqlType, boolean nullable, boolean primaryKey,
                            boolean autoIncrement, String defaultValue, Integer length,
                            String referencedTable, String referencedColumn) {
        this.name = name;
        this.sqlType = sqlType;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.autoIncrement = autoIncrement;
        this.defaultValue = defaultValue;
        this.length = length;
        this.referencedTable = referencedTable;
        this.referencedColumn = referencedColumn;
        this.foreignKey = referencedTable != null && referencedColumn != null;
    }


    public String getName() {
        return name;
    }

    public String getSqlType() {
        return sqlType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Integer getLength() {
        return length;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public String getReferencedColumn() {
        return referencedColumn;
    }

    public boolean isForeignKey() {
        return foreignKey;
    }

    /**
     * Get the full SQL type including length if applicable.
     */
    public String getFullSqlType() {
        if (length != null && (sqlType.equalsIgnoreCase("VARCHAR") || sqlType.equalsIgnoreCase("CHAR"))) {
            return sqlType + "(" + length + ")";
        }
        return sqlType;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnDefinition that = (ColumnDefinition) o;
        return nullable == that.nullable &&
                primaryKey == that.primaryKey &&
                autoIncrement == that.autoIncrement &&
                Objects.equals(name, that.name) &&
                Objects.equals(sqlType.toUpperCase(), that.sqlType.toUpperCase()) &&
                Objects.equals(length, that.length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, sqlType.toUpperCase(), nullable, primaryKey, autoIncrement, length);
    }

    @Override
    public String toString() {
        return "ColumnDefinition{" +
                "name='" + name + '\'' +
                ", sqlType='" + sqlType + '\'' +
                ", nullable=" + nullable +
                ", primaryKey=" + primaryKey +
                ", autoIncrement=" + autoIncrement +
                ", length=" + length +
                '}';
    }

    /**
     * Builder for creating ColumnDefinition instances.
     */
    public static class Builder {
        private String name;
        private String sqlType;
        private boolean nullable = true;
        private boolean primaryKey = false;
        private boolean autoIncrement = false;
        private String defaultValue = null;
        private Integer length = null;
        private String referencedTable = null;
        private String referencedColumn = null;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder sqlType(String sqlType) {
            this.sqlType = sqlType;
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder primaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder autoIncrement(boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder length(Integer length) {
            this.length = length;
            return this;
        }

        public Builder referencedTable(String referencedTable) {
            this.referencedTable = referencedTable;
            return this;
        }

        public Builder referencedColumn(String referencedColumn) {
            this.referencedColumn = referencedColumn;
            return this;
        }

        public ColumnDefinition build() {
            return new ColumnDefinition(name, sqlType, nullable, primaryKey, autoIncrement, defaultValue, length, referencedTable, referencedColumn);
        }

    }

}
