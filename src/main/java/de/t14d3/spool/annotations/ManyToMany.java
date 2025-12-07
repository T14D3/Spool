package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a many-to-many relationship.
 * The annotated field should be a collection type.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ManyToMany {
    /**
     * Whether to fetch the related entities lazily.
     */
    boolean fetch() default true;

    /**
     * Cascade operations to related entities.
     */
    CascadeType[] cascade() default {};

    /**
     * The field in the target entity that references this entity (for bidirectional).
     */
    String mappedBy() default "";
}
