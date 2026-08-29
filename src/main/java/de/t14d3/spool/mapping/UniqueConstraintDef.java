package de.t14d3.spool.mapping;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Resolved unique constraint on a table.
 *
 * <p>Produced from {@link de.t14d3.spool.annotations.UniqueConstraint} entries
 * on {@link de.t14d3.spool.annotations.Table#uniqueConstraints()}, and from
 * {@code @Column(unique = true)} single-column declarations. Surfaced through
 * {@link EntityMetadata#getUniqueConstraints()} for use by the migration
 * tooling.</p>
 */
public final class UniqueConstraintDef {

    private final String name;
    private final List<String> columns;

    public UniqueConstraintDef(String name, List<String> columns) {
        this.name = Objects.requireNonNull(name, "name");
        this.columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
        if (this.columns.isEmpty()) {
            throw new IllegalArgumentException("Unique constraint must reference at least one column");
        }
        for (String c : this.columns) {
            Objects.requireNonNull(c, "column");
            if (c.isBlank()) {
                throw new IllegalArgumentException("Unique constraint column name must not be blank");
            }
        }
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UniqueConstraintDef that)) return false;
        return name.equals(that.name) && columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns);
    }

    @Override
    public String toString() {
        return "UniqueConstraintDef{name='" + name + "', columns=" + columns + "}";
    }
}
