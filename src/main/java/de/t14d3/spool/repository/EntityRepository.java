package de.t14d3.spool.repository;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.core.Persister;
import de.t14d3.spool.mapping.EntityMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Base repository that auto-generates table schema on initialization.
 */
public abstract class EntityRepository<T> {
    // Repository registry (class -> repository instance)
    private static final Map<Class<?>, EntityRepository<?>> REPOSITORY_REGISTRY = new ConcurrentHashMap<>();

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
        // Register instance in registry
        REPOSITORY_REGISTRY.put(clazz, this);
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
        em.getExecutor().execute(ddl, List.of());
    }

    /**
     * Gets or creates a repository instance for the given entity class.
     *
     * @param <T> Entity type
     * @param em EntityManager instance
     * @param entityClass Entity class
     * @return Existing or new repository instance
     * @throws IllegalStateException if repository can't be created
     */
    @SuppressWarnings("unchecked")
    public static <T> EntityRepository<T> getRepository(EntityManager em, Class<T> entityClass) {
        return (EntityRepository<T>) REPOSITORY_REGISTRY.computeIfAbsent(
                entityClass,
                cls -> createRepository(em, entityClass)
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> EntityRepository<T> createRepository(EntityManager em, Class<T> entityClass) {
        String pkg = entityClass.getPackageName();
        String simpleName = entityClass.getSimpleName();
        // derive possible base names: exact simpleName, and if it endsWith "Entity" also the trimmed variant
        List<String> candidates = new ArrayList<>();
        candidates.add(simpleName);
        if (simpleName.endsWith("Entity")) {
            candidates.add(simpleName.substring(0, simpleName.length() - "Entity".length()));
        }

        for (String base : candidates) {
            String repoFqn = pkg + "." + base + "Repository";
            try {
                Class<?> repoClass = Class.forName(repoFqn);
                if (!EntityRepository.class.isAssignableFrom(repoClass)) {
                    throw new IllegalStateException(repoFqn + " is not an EntityRepository");
                }
                return (EntityRepository<T>) repoClass
                        .getDeclaredConstructor(EntityManager.class, Class.class)
                        .newInstance(em, entityClass);
            } catch (ClassNotFoundException ignored) {
                // try next candidate
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to instantiate " + repoFqn, e);
            }
        }

        throw new IllegalStateException(
                "No repository found for entity " + entityClass.getName() +
                        ". Tried: " + candidates.stream()
                        .map(c -> pkg + "." + c + "Repository")
                        .collect(Collectors.joining(", "))
        );
    }

    public T findById(Object id) {
        return em.find(clazz, id);
    }

    public List<T> findBy(Map<String, Object> criteria) {
        return em.getExecutor().findBy(clazz, criteria);
    }

    public List<T> findBy(Map<String, Object> criteria, String orderBy, int limit, int offset) {
        return em.getExecutor().findBy(clazz, criteria, orderBy, limit, offset);
    }

    public T findOneBy(Map<String, Object> criteria) {
        return em.getExecutor().findOneBy(clazz, criteria);
    }

    public List<T> findAll() {
        return em.getExecutor().findAll(clazz);
    }

    public void save(T entity) {
        em.persist(entity);
    }

    public void delete(T entity) {
        em.remove(entity);
    }
}