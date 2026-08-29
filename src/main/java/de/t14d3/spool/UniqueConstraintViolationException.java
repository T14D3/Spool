package de.t14d3.spool;

import java.sql.SQLException;

/**
 * Thrown when a database write violates a unique constraint.
 *
 * <p>Wraps the underlying vendor-specific {@link SQLException} (typically
 * an {@link java.sql.SQLIntegrityConstraintViolationException} or its
 * vendor-specific subclass) and carries the resolved constraint name
 * and the entity context when known, so callers can react to a
 * unique-key collision specifically (re-find and update) rather
 * than inspecting vendor error codes or SQLState strings.</p>
 *
 * <p>Surface naturally from {@link de.t14d3.spool.core.EntityManager#flush()};
 * callers that need a checked-exception type can recover the original
 * vendor exception via {@link #getCause()}.</p>
 */
public class UniqueConstraintViolationException extends RuntimeException {

    private final String entityName;
    private final String constraintName;
    private final String sqlState;
    private final int vendorCode;

    public UniqueConstraintViolationException(
            String message,
            String sqlState,
            int vendorCode,
            String entityName,
            String constraintName,
            Throwable cause) {
        super(message, cause);
        this.entityName = entityName;
        this.constraintName = constraintName;
        this.sqlState = sqlState;
        this.vendorCode = vendorCode;
    }

    /**
     * The entity class name (or table name) the offending write targeted, or
     * {@code null} when not known at the throw site.
     */
    public String getEntityName() {
        return entityName;
    }

    /**
     * The resolved unique-constraint name, or {@code null} when the vendor
     * did not include one in the error message.
     */
    public String getConstraintName() {
        return constraintName;
    }

    /**
     * The SQLState string from the underlying {@link SQLException}, or
     * {@code null} when none was reported. Provided as a convenience for
     * callers that want to inspect the vendor's classification without
     * unwrapping the cause chain.
     */
    public String getSQLState() {
        return sqlState;
    }

    /**
     * The vendor-specific error code from the underlying {@link SQLException},
     * or {@code 0} when none was reported.
     */
    public int getVendorCode() {
        return vendorCode;
    }
}
