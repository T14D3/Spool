package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a superclass whose persistent fields are mapped into the table of every
 * {@link Entity} subclass.
 * <p>
 * The superclass itself has no table. Fields declared in a
 * {@code @MappedSuperclass} are copied into each subclass's mapping, so base
 * classes can carry the id, audit columns and relationship declarations shared
 * by several entities.
 * <p>
 * A superclass that is neither {@code @Entity} nor {@code @MappedSuperclass}
 * and declares instance fields is rejected when entity metadata is built: its
 * fields would otherwise be silently ignored.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MappedSuperclass {
}
