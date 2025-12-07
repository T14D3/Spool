package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a one-to-one relationship.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToOne {
    /**
     * Whether to fetch the related entity lazily.
     */
    boolean fetch() default true;

    /**
     * Cascade operations to the related entity.
     */
    CascadeType[] cascade() default {};

    /**
     * Whether this relationship is optional.
     */
    boolean optional() default true;

    /**
     * The field in the target entity that references this entity (for bidirectional).
     */
    String mappedBy() default "";
}
