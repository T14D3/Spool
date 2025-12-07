package de.t14d3.spool.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as an entity that can be persisted to the database.
 * <p>
 * This annotation identifies a Java class as a persistent entity that can be
 * stored and retrieved from a database. When a class is annotated with @Entity,
 * the Spool ORM framework will automatically generate the necessary database
 * schema and provide CRUD operations for instances of this class.
 * 
 * @see Table
 * @see Column
 * @see Id
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Entity {
}
