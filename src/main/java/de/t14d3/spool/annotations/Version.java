package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a numeric field as the entity's optimistic-lock version.
 * <p>
 * The initial value is 0. Every UPDATE adds
 * {@code AND version = <loaded value>} to the WHERE clause and writes
 * {@code version + 1}; when no row matches, an
 * {@link de.t14d3.spool.OptimisticLockException} is thrown and the
 * surrounding transaction is rolled back.
 * <p>
 * Supported field types: {@code int}, {@code long}, {@code short} and their
 * wrapper types.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Version {
}
