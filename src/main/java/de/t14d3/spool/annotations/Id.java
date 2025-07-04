package de.t14d3.spool.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Id {
    String type() default "BIGINT";
    boolean autoIncrement() default false; // New property for auto-increment
}
