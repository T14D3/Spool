package de.t14d3.spool.annotations;

/**
 * Cascade operations for relationships.
 */
public enum CascadeType {
    /**
     * Cascade persist operations.
     */
    PERSIST,

    /**
     * Cascade remove operations.
     */
    REMOVE,

    /**
     * Cascade detach operations.
     */
    DETACH,

    /**
     * Cascade refresh operations.
     */
    REFRESH,

    /**
     * Cascade all operations.
     */
    ALL,

    /**
     * Cascade merge operations.
     */
    MERGE
}
