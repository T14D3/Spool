package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a one-to-many relationship.
 * <p>
 * Defines a relationship where one entity instance has a collection of related
 * entity instances. The annotated field should be a collection type (such as
 * List, Set, etc.) containing instances of the target entity.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToMany {
    /**
     * The entity class that owns the relationship.
     */
    Class<?> targetEntity();

    /**
     * The field in the target entity that references this entity.
     */
    String mappedBy();

    /**
     * Fetch strategy for the related entities.
     */
    FetchType fetch() default FetchType.EAGER;

    /**
     * Cascade operations to related entities.
     */
    CascadeType[] cascade() default {};
}
