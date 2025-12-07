package de.t14d3.spool.repository;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.mapping.EntityMetadata;

import java.util.List;

/**
 * Base repository class for entity CRUD operations.
 * 
 * Provides a convenient base class for entity-specific repositories, implementing
 * common CRUD (Create, Read, Update, Delete) operations. The repository pattern
 * abstracts database access logic and provides a cleaner separation of concerns
 * between business logic and data access.
 * 
 * @param <T> The entity type managed by this repository
 * 
 * @see EntityManager
 * @see EntityMetadata
 */
public class EntityRepository<T> {
    protected final EntityManager entityManager;
    protected final Class<T> entityClass;
    protected final EntityMetadata metadata;

    /**
     * Creates a new repository for the specified entity class.
     * 
     * @param entityManager the EntityManager to use for database operations
     * @param entityClass the class of entities managed by this repository
     */
    public EntityRepository(EntityManager entityManager, Class<T> entityClass) {
        this.entityManager = entityManager;
        this.entityClass = entityClass;
        this.metadata = EntityMetadata.of(entityClass);
    }

    /**
     * Save an entity (insert or update).
     * <p>
     * If the entity has no ID or doesn't exist in the database, it will be inserted.
     * Otherwise, it will be updated. The entity is marked for persistence and will
     * be synchronized with the database when flush() is called.
     * 
     * @param entity the entity to save
     * @return the same entity instance
     */
    public T save(T entity) {
        entityManager.persist(entity);
        return entity;
    }

    /**
     * Find an entity by its ID.
     * 
     * @param id the ID of the entity
     * @return the entity instance if found, null otherwise
     */
    public T findById(Object id) {
        return entityManager.find(entityClass, id);
    }

    /**
     * Find all entities of this type.
     * 
     * @return a list containing all entities of this type
     */
    public List<T> findAll() {
        return entityManager.getExecutor().findAll(entityClass);
    }

    /**
     * Delete an entity.
     * 
     * Marks the entity for deletion. The deletion will be performed when flush() is called.
     * 
     * @param entity the entity to delete
     */
    public void delete(T entity) {
        entityManager.remove(entity);
    }

    /**
     * Delete an entity by its ID.
     * 
     * First retrieves the entity by ID, then marks it for deletion. If no entity
     * with the given ID exists, this method does nothing.
     * 
     * @param id the ID of the entity to delete
     */
    public void deleteById(Object id) {
        T entity = findById(id);
        if (entity != null) {
            delete(entity);
        }
    }

    /**
     * Check if an entity exists by ID.
     * 
     * @param id the ID to check
     * @return true if an entity with the given ID exists, false otherwise
     */
    public boolean existsById(Object id) {
        return findById(id) != null;
    }

    /**
     * Count all entities of this type.
     * 
     * @return the total number of entities
     */
    public long count() {
        return findAll().size();
    }

    /**
     * Get the EntityManager instance.
     * 
     * @return the EntityManager instance
     */
    protected EntityManager getEntityManager() {
        return entityManager;
    }

    /**
     * Get the entity class.
     * 
     * @return the entity class managed by this repository
     */
    protected Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * Get the entity metadata.
     * 
     * @return the metadata for the entity class
     */
    protected EntityMetadata getMetadata() {
        return metadata;
    }
}
