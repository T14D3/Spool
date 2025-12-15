package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the column mapping for a field.
 * <p>
 * This annotation defines how a Java field maps to a database column. It allows
 * customization of column name, nullability, length constraints, and SQL type.
 * If not specified, the field name is used as the column name by default, and
 * the SQL type is inferred from the Java field type.
 *
 * @see Entity
 * @see Id
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * The name of the database column.
     */
    String name();

    /**
     * Whether the column can be null.
     */
    boolean nullable() default true;

    /**
     * The length of the column (for string types).
     */
    int length() default 255;

    /**
     * The SQL type of the column (e.g., "VARCHAR", "TEXT", "BIGINT", etc.).
     * If not specified or empty, the type is inferred from the Java field type.
     */
    String type() default "";
}
