package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the database table name for an entity.
 * <p>
 * This annotation is used to explicitly define the database table name that
 * corresponds to an entity class. If not specified, the table name defaults
 * to the simple class name of the entity.
 *
 * 
 * @see Entity
 * @see Column
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /**
     * The name of the database table.
     */
    String name();

    /**
     * Composite unique constraints declared at the table level. Each entry is
     * materialized as a {@code UNIQUE} index during schema migration. Inserting
     * a row that duplicates an existing one across the listed columns raises a
     * {@link de.t14d3.spool.UniqueConstraintViolationException}.
     */
    UniqueConstraint[] uniqueConstraints() default {};
}
