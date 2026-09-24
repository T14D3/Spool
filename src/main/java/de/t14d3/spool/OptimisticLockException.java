package de.t14d3.spool;

/**
 * Thrown when an UPDATE matches no row because the entity's version changed
 * (or the row was deleted) since it was loaded. The surrounding flush is
 * rolled back; re-read the entity and retry the operation.
 */
public class OptimisticLockException extends RuntimeException {
    private final Class<?> entityClass;
    private final Object id;

    public OptimisticLockException(Class<?> entityClass, Object id, String message) {
        super(message);
        this.entityClass = entityClass;
        this.id = id;
    }

    public Class<?> getEntityClass() {
        return entityClass;
    }

    public Object getId() {
        return id;
    }
}
