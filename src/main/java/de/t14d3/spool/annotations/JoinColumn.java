package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Customizes the foreign key column for ManyToOne / OneToOne relationships.
 * <p>
 * - name: the FK column name on the owning table.
 * - referencedColumnName: the referenced column name on the target table (defaults to the target id column).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinColumn {
    String name();

    String referencedColumnName() default "";
}

