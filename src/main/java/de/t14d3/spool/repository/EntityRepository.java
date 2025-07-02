package de.t14d3.spool.repository;

import de.t14d3.spool.annotations.Repository;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.core.Persister;
import de.t14d3.spool.mapping.EntityMetadata;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Base repository that auto-generates table schema on initialization.
 */
public abstract class EntityRepository<T> {
    private static final Map<Class<?>, EntityRepository<?>> REPOSITORY_REGISTRY = new ConcurrentHashMap<>();
    private static final Set<Class<?>> CREATING_REPOSITORIES = ConcurrentHashMap.newKeySet();

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
        EntityRepository<?> repo = REPOSITORY_REGISTRY.get(entityClass);
        if (repo != null) {
            return (EntityRepository<T>) repo;
        }

        // Prevent recursive creation attempts
        if (CREATING_REPOSITORIES.contains(entityClass)) {
            throw new IllegalStateException("Circular dependency detected while creating repository for " + entityClass.getName());
        }

        try {
            CREATING_REPOSITORIES.add(entityClass);
            return (EntityRepository<T>) REPOSITORY_REGISTRY.computeIfAbsent(
                    entityClass,
                    cls -> createRepository(em, entityClass)
            );
        } finally {
            CREATING_REPOSITORIES.remove(entityClass);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> EntityRepository<T> createRepository(EntityManager em, Class<T> entityClass) {
        String entityPkg = entityClass.getPackageName();
        String repoPkg = entityPkg + ".repository";
        Set<URL> urls = new HashSet<>();
        urls.addAll(ClasspathHelper.forPackage(repoPkg));
        urls.addAll(ClasspathHelper.forPackage(entityPkg));

        Reflections reflections = new Reflections(
                new ConfigurationBuilder()
                        .setUrls(urls)
                        .setScanners(Scanners.TypesAnnotated)
        );

        for (Class<?> repoCls : reflections.getTypesAnnotatedWith(Repository.class)) {
            Repository ann = repoCls.getAnnotation(Repository.class);
            if (ann.value().equals(entityClass)) {
                try {
                    return (EntityRepository<T>) repoCls
                            .getDeclaredConstructor(EntityManager.class, Class.class)
                            .newInstance(em, entityClass);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("Cannot instantiate " + repoCls, e);
                }
            }
        }

        throw new IllegalStateException(
                "No @Repository(" + entityClass.getName() + ") found in packages: "
                        + entityPkg + " or " + repoPkg
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