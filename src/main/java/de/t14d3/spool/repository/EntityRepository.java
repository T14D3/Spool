package de.t14d3.spool.repository;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.query.LockMode;
import de.t14d3.spool.query.Query;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

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
        return entityManager.findAll(entityClass);
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
        if (entityManager.isManaged(entityClass, id)) {
            return true;
        }
        return entityManager.getExecutor().existsById(entityClass, id);
    }

    /**
     * Refresh an entity's state from the database, discarding in-memory changes.
     *
     * @param entity the entity to refresh
     */
    public void refresh(T entity) {
        entityManager.refresh(entity);
    }

    /**
     * Acquire a database row lock for an entity.
     * <p>
     * The lock is held until the surrounding transaction commits or rolls back,
     * so this method must be called inside an active transaction.
     *
     * @param entity the entity to lock
     * @param mode   the lock mode
     * @return the same entity instance
     */
    public T lock(T entity, LockMode mode) {
        entityManager.lock(entity, mode);
        return entity;
    }

    /**
     * Count all entities of this type.
     * 
     * @return the total number of entities
     */
    public long count() {
        return entityManager.getExecutor().count(metadata.getTableName());
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
     * Find entities by criteria.
     * <p>
     * Searches for entities matching the specified criteria. Criteria is a map
     * of field names to their expected values. All criteria must match (AND logic).
     *
     * @param criteria a map of field names to values
     * @return a list of entities matching the criteria
     */
    public List<T> findBy(Map<String, Object> criteria) {
        return findBy(criteria, null, null, null);
    }

    /**
     * Find entities by criteria with ordering and pagination.
     * <p>
     * {@code orderBy} is a comma-separated list of field names with an optional
     * {@code ASC}/{@code DESC} suffix (for example {@code "name DESC, id ASC"}).
     * {@code limit} and {@code offset} may be null to leave the clause out;
     * offset-only pagination is supported on every dialect.
     *
     * @param criteria a map of field names to values; may be null or empty
     * @param orderBy  ordering expression, or null
     * @param limit    maximum number of rows, or null
     * @param offset   number of rows to skip, or null/0
     * @return a list of entities matching the criteria
     */
    public List<T> findBy(Map<String, Object> criteria, String orderBy, Integer limit, Integer offset) {
        Query.SelectBuilder builder = Query.select(entityManager.getDialect(), "*").from(metadata.getTableName());

        if (criteria != null) {
            for (Map.Entry<String, Object> entry : criteria.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                String columnName = getColumnNameByField(fieldName);
                if (columnName != null) {
                    builder.where(entityManager.getDialect().quoteIdentifier(columnName) + " = ?", value);
                } else {
                    throw new IllegalArgumentException("Unknown field: " + fieldName);
                }
            }
        }

        if (orderBy != null && !orderBy.isBlank()) {
            builder.orderBy(resolveOrderBy(orderBy));
        }
        if (limit != null) {
            if (limit < 1) {
                throw new IllegalArgumentException("limit must be >= 1");
            }
            builder.limit(Integer.toString(limit));
        }
        if (offset != null && offset > 0) {
            builder.offset(Integer.toString(offset));
        }

        return executeSelectQuery(builder.build());
    }

    /**
     * Find entities by criteria with ordering.
     */
    public List<T> findBy(Map<String, Object> criteria, String orderBy) {
        return findBy(criteria, orderBy, null, null);
    }

    /**
     * Find entities by a single field value with ordering and pagination.
     */
    public List<T> findBy(String field, Object value, String orderBy, Integer limit, Integer offset) {
        return findBy(Map.of(field, value), orderBy, limit, offset);
    }

    private String resolveOrderBy(String orderBy) {
        StringBuilder resolved = new StringBuilder();
        for (String segment : orderBy.split(",")) {
            String[] tokens = segment.trim().split("\\s+");
            if (tokens.length == 0 || tokens.length > 2
                    || (tokens.length == 2
                    && !tokens[1].equalsIgnoreCase("ASC") && !tokens[1].equalsIgnoreCase("DESC"))) {
                throw new IllegalArgumentException(
                        "orderBy must be 'field [ASC|DESC][, ...]', found '" + segment.trim() + "'");
            }
            String columnName = getColumnNameByField(tokens[0]);
            if (columnName == null) {
                throw new IllegalArgumentException("Unknown order field: " + tokens[0]);
            }
            if (resolved.length() > 0) {
                resolved.append(", ");
            }
            resolved.append(columnName);
            if (tokens.length == 2) {
                resolved.append(tokens[1].equalsIgnoreCase("DESC") ? " DESC" : " ASC");
            }
        }
        return resolved.toString();
    }

    /**
     * Find entities by a single field value.
     * <p>
     * Searches for entities where the specified field matches the given value.
     *
     * @param field the field name to search by
     * @param value the value to match
     * @return a list of entities matching the criteria
     */
    public List<T> findBy(String field, Object value) {
        return findBy(Map.of(field, value));
    }

    /**
     * Find a single entity by a single field value.
     * <p>
     * Searches for the first entity where the specified field matches the given value.
     * Returns null if no entity matches.
     *
     * @param field the field name to search by
     * @param value the value to match
     * @return the first entity matching the criteria, or null if none found
     */
    public T findOneBy(String field, Object value) {
        return findOneBy(Map.of(field, value));
    }

    /**
     * Find a single entity by criteria.
     * <p>
     * Searches for the first entity matching the specified criteria. Criteria is a map
     * of field names to their expected values. All criteria must match (AND logic).
     * Returns null if no entity matches.
     *
     * @param criteria a map of field names to values
     * @return the first entity matching the criteria, or null if none found
     */
    public T findOneBy(Map<String, Object> criteria) {
        if (criteria == null || criteria.isEmpty()) {
            List<T> all = findAll();
            return all.isEmpty() ? null : all.get(0);
        }

        Query.SelectBuilder builder = Query.select(entityManager.getDialect(), "*").from(metadata.getTableName());

        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            String columnName = getColumnNameByField(fieldName);
            if (columnName != null) {
                builder.where(entityManager.getDialect().quoteIdentifier(columnName) + " = ?", value);
            } else {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
        }

        builder.limit("1");

        Query query = builder.build();
        List<T> results = executeSelectQuery(query);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Get the entity metadata.
     *
     * @return the metadata for the entity class
     */
    protected EntityMetadata getMetadata() {
        return metadata;
    }

    /**
     * Get column name for a given field name.
     *
     * @param fieldName the field name
     * @return the column name, or null if field not found
     */
    private String getColumnNameByField(String fieldName) {
        for (Field field : metadata.getFields()) {
            if (field.getName().equals(fieldName)) {
                return metadata.getColumnName(field);
            }
        }
        return null;
    }

    /**
     * Execute a select query and map results to entities.
     *
     * @param query the query to execute
     * @return list of mapped entities
     */
    private List<T> executeSelectQuery(Query query) {
        return entityManager.executeSelectQuery(query, entityClass);
    }
}
