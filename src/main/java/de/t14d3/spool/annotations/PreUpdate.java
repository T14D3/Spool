package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a no-argument method that is invoked immediately before an
 * {@code UPDATE} statement is executed for a dirty entity. The callback is
 * only fired when the entity has at least one changed column at flush time.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PreUpdate {
}
