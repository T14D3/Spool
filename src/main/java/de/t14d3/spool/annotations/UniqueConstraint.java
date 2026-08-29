package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a composite unique constraint on the columns of a table.
 *
 * <p>A unique constraint is materialized as a {@code UNIQUE} index over the
 * listed columns. Inserting a row whose column values duplicate an existing
 * row's values raises a
 * {@link de.t14d3.spool.UniqueConstraintViolationException}.</p>
 *
 * <p>Use the table-level form (on {@link Table#uniqueConstraints()}) for
 * composite constraints or constraints that need a stable name. For a
 * single-column constraint, {@link Column#unique()} is a shorter form with
 * an auto-generated name.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface UniqueConstraint {

    /**
     * The name of the unique index. Must be unique within the table.
     *
     * <p>When blank, the migration manager derives a stable name from the
     * column list (the same columns always produce the same name).</p>
     */
    String name();

    /**
     * The columns that form the unique key. Each entry must reference a
     * column declared on the entity (via {@link Column#name()}, or the
     * field name when no {@code @Column} is present).
     */
    String[] columns();
}
