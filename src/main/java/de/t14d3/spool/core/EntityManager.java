package de.t14d3.spool.core;

import de.t14d3.spool.connection.QueryExecutor;
import de.t14d3.spool.connection.DatabaseConnection;
import de.t14d3.spool.exceptions.OrmException;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.mapping.EntityScanner;

public class EntityManager {
    private final DatabaseConnection db;
    private final UnitOfWork uow;

    private EntityManager(String url) {
        this.db = new DatabaseConnection(url);
        this.uow = new UnitOfWork();
        EntityScanner.scan();
    }

    public static EntityManager create(String jdbcUrl) {
        return new EntityManager(jdbcUrl);
    }

    /**
     * Persist an entity: automatically INSERT if new, UPDATE if existing.
     */
    public <T> void persist(T entity) {
        EntityMetadata md = EntityMetadata.of(entity.getClass());
        Object id = md.idValue(entity);
        boolean exists = false;
        if (id != null) {
            try {
                @SuppressWarnings("unchecked") T found = db.getExecutor().find((Class<T>) entity.getClass(), id);
                exists = found != null;
            } catch (Exception e) {
                throw new OrmException("Failed to check existing entity", e);
            }
        }
        if (id == null || !exists) {
            uow.registerNew(entity);
        } else {
            uow.registerDirty(entity);
        }
    }

    public <T> T find(Class<T> clazz, Object id) {
        try {
            return db.getExecutor().find(clazz, id);
        } catch (OrmException e) {
            throw e;
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }

    public <T> void remove(T entity) {
        uow.registerRemoved(entity);
    }

    public void flush() {
        uow.commit(db.getPersister());
    }

    public void close() {
        db.close();
    }

    public QueryExecutor getExecutor() {
        return db.getExecutor();
    }
}
