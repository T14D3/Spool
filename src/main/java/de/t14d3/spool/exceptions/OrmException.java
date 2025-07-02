package de.t14d3.spool.exceptions;

public class OrmException extends RuntimeException {
    public OrmException(String message) {
        super(message);
    }

    public OrmException(Throwable cause) {
        super(cause);
    }

    public OrmException(String message, Throwable cause) {
        super(message, cause);
    }
}
