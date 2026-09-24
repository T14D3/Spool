package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Orders a {@code @OneToMany} or {@code @ManyToMany} collection when it is
 * loaded: {@code @OrderBy("name ASC, id DESC")}.
 * <p>
 * Each comma-separated segment is a target-entity field optionally followed by
 * {@code ASC} (default) or {@code DESC}. Fields must be mapped columns of the
 * target entity (or a {@code @MappedSuperclass} ancestor); an unmapped field is
 * rejected when metadata is built instead of being ignored.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OrderBy {
    String value();
}
