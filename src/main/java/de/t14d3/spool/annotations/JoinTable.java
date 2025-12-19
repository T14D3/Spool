package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Customizes the join table used for a {@link ManyToMany} relationship.
 *
 * When omitted, Spool falls back to naming conventions.
 *
 * This annotation is intended for the owning side (i.e. {@code mappedBy == ""}).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinTable {
    /**
     * Join table name.
     */
    String name() default "";

    /**
     * Column name referencing the owning entity's id.
     */
    String joinColumn() default "";

    /**
     * Column name referencing the inverse entity's id.
     */
    String inverseJoinColumn() default "";
}

