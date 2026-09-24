package de.t14d3.spool.query.hql;

/**
 * Thrown when an HQL statement cannot be parsed or bound.
 */
public class HqlException extends RuntimeException {
    private final int position;

    public HqlException(String message) {
        this(message, -1);
    }

    public HqlException(String message, int position) {
        super(position >= 0 ? message + " (at position " + position + ")" : message);
        this.position = position;
    }

    /**
     * Character offset in the original statement, or -1 when unknown.
     */
    public int getPosition() {
        return position;
    }
}
