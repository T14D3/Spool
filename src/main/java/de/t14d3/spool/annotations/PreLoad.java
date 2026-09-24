package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a no-argument method that is invoked after an entity instance has been
 * materialized from a database row (or a cached snapshot) and attached to the
 * EntityManager.
 * <p>
 * The callback does not fire when {@code find} returns an already-managed
 * instance from the identity map, because no load took place.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PreLoad {
}
