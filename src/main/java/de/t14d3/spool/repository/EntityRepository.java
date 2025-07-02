package de.t14d3.spool.repository;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.core.Persister;
import de.t14d3.spool.mapping.EntityMetadata;

import java.util.stream.Collectors;

/**
 * Base repository that auto-generates table schema on initialization.
 */
public abstract class EntityRepository<T> {
    protected final EntityManager em;
    protected final Persister persister;
    protected final Class<T> clazz;
    protected final EntityMetadata md;

    protected EntityRepository(EntityManager em, Class<T> clazz) {
        this.em = em;
        this.clazz = clazz;
        this.md = EntityMetadata.of(clazz);
        this.persister = new Persister(em.getExecutor());
        ensureSchema();
    }

    // Creates table based on entity metadata
    private void ensureSchema() {
        String idCol = md.getIdColumn();
        String table = md.getTableName();
        String otherCols = md.getColumns().stream()
                .map(c -> c + " VARCHAR(255)")
                .collect(Collectors.joining(", "));
        String ddl = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s BIGINT PRIMARY KEY%s)",
                table,
                idCol,
                otherCols.isEmpty() ? "" : ", " + otherCols
        );
        em.getExecutor().execute(ddl, java.util.List.of());
    }

    public T findById(Object id) {
        return em.find(clazz, id);
    }

    public java.util.List<T> findAll() {
        return em.getExecutor().findAll(clazz);
    }

    public void save(T entity) {
        em.persist(entity);
    }

    public void delete(T entity) {
        em.remove(entity);
    }
}
