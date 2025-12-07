package de.t14d3.spool.core;

import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.migration.MigrationManager;
import de.t14d3.spool.migration.PersistentMigrationManager;
import de.t14d3.spool.migration.MigrationFile;
import de.t14d3.spool.migration.SchemaIntrospector;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.query.Query;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The main interface for interacting with the database.
 * Manages entity lifecycle and persistence operations.
 * Similar to Doctrine's EntityManager.
 */
public class EntityManager {
    private final SqlExecutor executor;
    private final Connection connection;
    private final Dialect dialect;
    private final ConcurrentHashMap<EntityKey, Object> identityMap;
    private final Set<Object> pendingInserts;
    private final Set<Object> pendingUpdates;
    private final Set<Object> pendingDeletes;
    private final RelationshipManager relationshipManager;
    private boolean transactionActive;
    private boolean originalAutoCommit;
    private MigrationManager migrationManager;
    private PersistentMigrationManager persistentMigrationManager;
    private SchemaIntrospector introspector;

    private EntityManager(Connection connection, Dialect dialect) {
        this.connection = connection;
        this.dialect = dialect;
        this.executor = new SqlExecutor(connection);
        this.identityMap = new ConcurrentHashMap<>();
        this.pendingInserts = new LinkedHashSet<>();
        this.pendingUpdates = new LinkedHashSet<>();
        this.pendingDeletes = new LinkedHashSet<>();
        this.relationshipManager = new RelationshipManager(this);
        this.transactionActive = false;
        this.migrationManager = null; // Lazy initialized
        this.persistentMigrationManager = null; // Lazy initialized
        this.introspector = null; // Lazy initialized
    }

    /**
     * Create a new EntityManager with the given database connection URL.
     */
    public static EntityManager create(String jdbcUrl) {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl);
            Dialect dialect = Dialect.detectFromUrl(jdbcUrl);
            return new EntityManager(connection, dialect);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create EntityManager connection", e);
        }
    }

    /**
     * Create a new EntityManager with an existing connection.
     */
    public static EntityManager create(Connection connection) {
        Dialect dialect = detectDialect(connection);
        return new EntityManager(connection, dialect);
    }

    /**
     * Detect the dialect from a database connection.
     */
    private static Dialect detectDialect(Connection connection) {
        try {
            String productName = connection.getMetaData().getDatabaseProductName().toLowerCase();
            if (productName.contains("mysql")) {
                return Dialect.MYSQL;
            } else if (productName.contains("postgresql")) {
                return Dialect.POSTGRESQL;
            } else if (productName.contains("sqlite")) {
                return Dialect.SQLITE;
            } else if (productName.contains("h2")) {
                return Dialect.H2;
            }
        } catch (SQLException e) {
            // Fall back to generic if detection fails
        }
        return Dialect.GENERIC;
    }

    /**
     * Begin a new transaction.
     * Disables auto-commit mode so that changes can be rolled back if needed.
     *
     * @throws IllegalStateException if a transaction is already active
     * @throws RuntimeException if the transaction cannot be started
     */
    public void beginTransaction() {
        if (transactionActive) {
            throw new IllegalStateException("Transaction already active");
        }
        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            transactionActive = true;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }
    }

    /**
     * Commit the current transaction.
     * All changes made since beginTransaction() will be permanently saved.
     *
     * @throws IllegalStateException if no transaction is active
     * @throws RuntimeException if the commit fails
     */
    public void commit() {
        if (!transactionActive) {
            throw new IllegalStateException("No active transaction to commit");
        }
        try {
            connection.commit();
            connection.setAutoCommit(originalAutoCommit);
            transactionActive = false;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to commit transaction", e);
        }
    }

    /**
     * Rollback the current transaction.
     * All changes made since beginTransaction() will be discarded.
     *
     * @throws IllegalStateException if no transaction is active
     * @throws RuntimeException if the rollback fails
     */
    public void rollback() {
        if (!transactionActive) {
            throw new IllegalStateException("No active transaction to rollback");
        }
        try {
            connection.rollback();
            connection.setAutoCommit(originalAutoCommit);
            transactionActive = false;
            // Clear pending operations since they've been rolled back
            clear();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to rollback transaction", e);
        }
    }

    /**
     * Check if a transaction is currently active.
     *
     * @return true if a transaction is active, false otherwise
     */
    public boolean isTransactionActive() {
        return transactionActive;
    }

    /**
     * Execute a unit of work within a transaction.
     * Automatically begins a transaction, executes the work, flushes changes,
     * and commits. If any exception occurs, the transaction is rolled back.
     *
     * @param work the unit of work to execute
     * @throws RuntimeException if the work fails (transaction will be rolled back)
     */
    public void transactional(Runnable work) {
        beginTransaction();
        try {
            work.run();
            flush();
            commit();
        } catch (Exception e) {
            rollback();
            throw new RuntimeException("Transaction failed and was rolled back", e);
        }
    }

    /**
     * Execute a unit of work within a transaction and return a result.
     * Automatically begins a transaction, executes the work, flushes changes,
     * and commits. If any exception occurs, the transaction is rolled back.
     *
     * @param <T> the return type
     * @param work the unit of work to execute
     * @return the result of the work
     * @throws RuntimeException if the work fails (transaction will be rolled back)
     */
    public <T> T transactional(java.util.function.Supplier<T> work) {
        beginTransaction();
        try {
            T result = work.get();
            flush();
            commit();
            return result;
        } catch (Exception e) {
            rollback();
            throw new RuntimeException("Transaction failed and was rolled back", e);
        }
    }

    /**
     * Mark an entity for persistence. The entity will be inserted or updated on flush.
     */
    public void persist(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Cannot persist null entity");
        }

        EntityMetadata metadata = EntityMetadata.of(entity.getClass());
        Object id = metadata.getIdValue(entity);

        if (id == null || !isInDatabase(entity, metadata)) {
            // New entity - mark for insert
            pendingInserts.add(entity);
            pendingUpdates.remove(entity); // Remove from updates if present
        } else {
            // Existing entity - mark for update
            if (!pendingInserts.contains(entity)) {
                pendingUpdates.add(entity);
            }
        }
    }

    /**
     * Internal method for RelationshipManager to persist an entity immediately during cascade operations.
     * This ensures cascaded entities get processed in the same flush cycle.
     */
    void persistCascade(Object entity) {
        if (entity == null) {
            return;
        }

        // If already processed or in pending inserts, skip
        if (pendingInserts.contains(entity)) {
            return;
        }

        EntityMetadata metadata = EntityMetadata.of(entity.getClass());
        Object id = metadata.getIdValue(entity);

        if (id == null || !isInDatabase(entity, metadata)) {
            // Add to pending inserts so it gets processed in this flush cycle
            pendingInserts.add(entity);
        } else {
            // Existing entity - mark for update
            pendingUpdates.add(entity);
        }
    }

    /**
     * Mark an entity for removal. The entity will be deleted on flush.
     */
    public void remove(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Cannot remove null entity");
        }

        pendingDeletes.add(entity);
        pendingInserts.remove(entity);
        pendingUpdates.remove(entity);

        EntityMetadata metadata = EntityMetadata.of(entity.getClass());
        Object id = metadata.getIdValue(entity);
        if (id != null) {
            identityMap.remove(new EntityKey(entity.getClass(), id));
        }
    }

    /**
     * Find an entity by its ID.
     */
    public <T> T find(Class<T> entityClass, Object id) {
        if (id == null) {
            return null;
        }

        // Check identity map first
        EntityKey key = new EntityKey(entityClass, id);
        @SuppressWarnings("unchecked")
        T cached = (T) identityMap.get(key);
        if (cached != null) {
            return cached;
        }

        // Load from database
        T entity = executor.findById(entityClass, id);
        if (entity != null) {
            identityMap.put(key, entity);
        }
        return entity;
    }

    /**
     * Flush all pending changes to the database.
     * If a transaction is active and an exception occurs, the transaction
     * will be automatically rolled back.
     *
     * @throws RuntimeException if flush fails (transaction will be rolled back if active)
     */
    public void flush() {
        // Take snapshots of pending operations for potential rollback recovery
        Set<Object> insertSnapshot = new LinkedHashSet<>(pendingInserts);
        Set<Object> updateSnapshot = new LinkedHashSet<>(pendingUpdates);
        Set<Object> deleteSnapshot = new LinkedHashSet<>(pendingDeletes);
        Map<EntityKey, Object> identityMapSnapshot = new HashMap<>(identityMap);

        try {
            // Process deletes first with cascade remove
            // Iterate over copy to avoid concurrent modification
            List<Object> deletesToProcess = new ArrayList<>(pendingDeletes);
            for (Object entity : deletesToProcess) {
                EntityMetadata metadata = EntityMetadata.of(entity.getClass());
                relationshipManager.cascadeRemove(entity, metadata);
                executor.delete(entity, metadata);
            }
            pendingDeletes.clear();

            // Process inserts with cascade persist
            // Use a local queue/drain approach so cascadePersist can safely add new entities
            Deque<Object> insertQueue = new ArrayDeque<>(pendingInserts);
            pendingInserts.clear();

            while (!insertQueue.isEmpty()) {
                Object entity = insertQueue.pollFirst();
                EntityMetadata metadata = EntityMetadata.of(entity.getClass());

                // Process cascade persist - this may add more entities to pendingInserts
                relationshipManager.cascadePersist(entity, metadata);

                // Insert the entity
                executor.insert(entity, metadata);

                // Add to identity map after insert
                Object id = metadata.getIdValue(entity);
                if (id != null) {
                    identityMap.put(new EntityKey(entity.getClass(), id), entity);
                }

                // Move any newly-added pendingInserts into the queue for processing
                if (!pendingInserts.isEmpty()) {
                    insertQueue.addAll(pendingInserts);
                    pendingInserts.clear();
                }
            }

            // Process updates
            List<Object> updatesToProcess = new ArrayList<>(pendingUpdates);
            for (Object entity : updatesToProcess) {
                EntityMetadata metadata = EntityMetadata.of(entity.getClass());
                executor.update(entity, metadata);
            }


        } catch (Exception e) {
            // Restore pending operations state for retry or inspection
            pendingInserts.clear();
            pendingInserts.addAll(insertSnapshot);
            pendingUpdates.clear();
            pendingUpdates.addAll(updateSnapshot);
            pendingDeletes.clear();
            pendingDeletes.addAll(deleteSnapshot);
            identityMap.clear();
            identityMap.putAll(identityMapSnapshot);

            // If transaction is active, rollback the database changes
            if (transactionActive) {
                try {
                    connection.rollback();
                    connection.setAutoCommit(originalAutoCommit);
                    transactionActive = false;
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
            }

            throw new RuntimeException("Flush failed" + (transactionActive ? "" : " and transaction was rolled back"), e);
        }
    }

    /**
     * Detach an entity from the EntityManager's control.
     * The entity will no longer be tracked by the identity map.
     */
    public void detach(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Cannot detach null entity");
        }

        EntityMetadata metadata = EntityMetadata.of(entity.getClass());
        Object id = metadata.getIdValue(entity);
        if (id != null) {
            EntityKey key = new EntityKey(entity.getClass(), id);
            identityMap.remove(key);
        }

        // Handle cascade detach
        relationshipManager.cascadeDetach(entity, metadata);

        // Remove from any pending operations
        pendingInserts.remove(entity);
        pendingUpdates.remove(entity);
        pendingDeletes.remove(entity);
    }

    /**
     * Refresh an entity's state from the database.
     * Any changes made to the entity in memory will be lost.
     */
    public void refresh(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Cannot refresh null entity");
        }

        EntityMetadata metadata = EntityMetadata.of(entity.getClass());
        Object id = metadata.getIdValue(entity);
        if (id == null) {
            throw new IllegalStateException("Cannot refresh transient entity");
        }

        // Reload the entity from database
        Object refreshedEntity = executor.findById(entity.getClass(), id);
        if (refreshedEntity == null) {
            throw new IllegalStateException("Entity no longer exists in database");
        }

        // Copy reloaded data to the existing entity instance
        for (Field field : metadata.getFields()) {
            Object value = metadata.getFieldValue(refreshedEntity, field);
            metadata.setFieldValue(entity, field, value);
        }

        // Ensure it's in identity map
        EntityKey key = new EntityKey(entity.getClass(), id);
        identityMap.put(key, entity);

        // Handle cascade refresh
        relationshipManager.cascadeRefresh(entity, metadata);
    }

    /**
     * Merge the state of a detached entity into the persistence context.
     * Returns the managed entity instance.
     */
    public <T> T merge(T entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Cannot merge null entity");
        }

        EntityMetadata metadata = EntityMetadata.of(entity.getClass());
        Object id = metadata.getIdValue(entity);

        if (id == null) {
            // Transient entity - treat as persist
            persist(entity);
            return entity;
        }

        // Check if already managed
        EntityKey key = new EntityKey(entity.getClass(), id);
        @SuppressWarnings({"unchecked", "ReassignedVariable"})
        T managedEntity = (T) identityMap.get(key);
        if (managedEntity != null && managedEntity == entity) {
            // Already managed, return as-is
            return entity;
        }

        if (managedEntity == null) {
            // Load from database or create managed copy
            //noinspection unchecked
            managedEntity = (T) executor.findById(entity.getClass(), id);
            if (managedEntity == null) {
                // Entity doesn't exist in DB, treat as new
                persist(entity);
                return entity;
            }
            // Put in identity map
            identityMap.put(key, managedEntity);
        }

        // Copy state from the detached entity to the managed one
        for (Field field : metadata.getFields()) {
            if (field != metadata.getIdField()) {
                Object value = metadata.getFieldValue(entity, field);
                metadata.setFieldValue(managedEntity, field, value);
            }
        }

        // Mark for update
        pendingUpdates.add(managedEntity);

        // Handle cascade merge
        relationshipManager.cascadeMerge(managedEntity, metadata);

        return managedEntity;
    }

    /**
     * Clear all pending operations and the identity map.
     */
    public void clear() {
        pendingInserts.clear();
        pendingUpdates.clear();
        pendingDeletes.clear();
        identityMap.clear();
    }

    /**
     * Check if an entity exists in the database.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean isInDatabase(Object entity, EntityMetadata metadata) {
        Object id = metadata.getIdValue(entity);
        if (id == null) {
            return false;
        }

        // Check identity map
        EntityKey key = new EntityKey(entity.getClass(), id);
        if (identityMap.containsKey(key)) {
            return true;
        }

        // Check database
        Object found = executor.findById(entity.getClass(), id);
        return found != null;
    }

    /**
     * Get the SQL executor for direct database access.
     */
    public SqlExecutor getExecutor() {
        return executor;
    }

    // ========================================================================
    // Automatic Migration Methods (Original functionality)
    // ========================================================================

    /**
     * Get or create the MigrationManager for this EntityManager.
     *
     * @return the MigrationManager instance
     */
    public MigrationManager getMigrationManager() {
        if (migrationManager == null) {
            migrationManager = new MigrationManager(connection);
        }
        return migrationManager;
    }

    /**
     * Register entity classes for automatic schema management.
     *
     * @param entityClasses the entity classes to register
     * @return this EntityManager for method chaining
     */
    @SuppressWarnings("UnusedReturnValue")
    public EntityManager registerEntities(Class<?>... entityClasses) {
        getMigrationManager().registerEntities(entityClasses);
        return this;
    }

    /**
     * Automatically update the database schema to match registered entities.
     * This will create missing tables and add missing columns.
     *
     * @return the number of schema changes applied
     * @throws RuntimeException if migration fails
     */
    public int updateSchema() {
        try {
            return getMigrationManager().updateSchema();
        } catch (Exception e) {
            throw new RuntimeException("Failed to update schema", e);
        }
    }

    /**
     * Validate that the database schema matches the registered entity definitions.
     *
     * @return true if schema is in sync, false otherwise
     * @throws RuntimeException if validation fails
     */
    public boolean validateSchema() {
        try {
            return getMigrationManager().validateSchema();
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate schema", e);
        }
    }

    /**
     * Get a report of schema differences between entities and database.
     *
     * @return a human-readable report of schema differences
     * @throws RuntimeException if report generation fails
     */
    public String getSchemaReport() {
        try {
            return getMigrationManager().getSchemaReport();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate schema report", e);
        }
    }

    // ========================================================================
    // Persistent Migration Methods (New functionality)
    // ========================================================================

    /**
     * Get or create the PersistentMigrationManager for this EntityManager.
     *
     * @return the PersistentMigrationManager instance
     */
    public PersistentMigrationManager getPersistentMigrationManager() {
        if (persistentMigrationManager == null) {
            persistentMigrationManager = new PersistentMigrationManager(connection);
        }
        return persistentMigrationManager;
    }

    /**
     * Register entity classes for persistent migration management.
     *
     * @param entityClasses the entity classes to register
     * @return this EntityManager for method chaining
     */
    @SuppressWarnings("UnusedReturnValue")
    public EntityManager registerEntitiesForPersistentMigration(Class<?>... entityClasses) {
        getPersistentMigrationManager().registerEntities(entityClasses);
        return this;
    }

    /**
     * Create a persistent migration file from current entity schema changes.
     * This generates a new migration file that can be version controlled.
     *
     * @param description Human-readable description of the migration
     * @return The filename of the created migration
     * @throws RuntimeException if migration creation fails
     */
    public String createMigration(String description) {
        try {
            return getPersistentMigrationManager().createMigration(description);
        } catch (IOException | SQLException e) {
            throw new RuntimeException("Failed to create migration", e);
        }
    }

    /**
     * Apply all pending migrations (migrations that exist as files but haven't been applied).
     *
     * @return list of applied migration filenames
     * @throws RuntimeException if migration fails
     */
    public List<String> applyPendingMigrations() {
        try {
            return getPersistentMigrationManager().applyPendingMigrations();
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply pending migrations", e);
        }
    }

    /**
     * Apply a specific migration file by name.
     *
     * @param filename the migration file to apply
     * @throws RuntimeException if migration fails
     */
    public void applyMigration(String filename) {
        try {
            getPersistentMigrationManager().applyMigration(filename);
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply migration: " + filename, e);
        }
    }

    /**
     * Get pending migrations (files that exist but haven't been applied).
     *
     * @return list of pending migration filenames
     * @throws RuntimeException if operation fails
     */
    public List<String> getPendingMigrations() {
        try {
            return getPersistentMigrationManager().getPendingMigrations();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get pending migrations", e);
        }
    }

    /**
     * Get migration status report.
     *
     * @return a status report of migrations
     * @throws RuntimeException if report generation fails
     */
    public String getMigrationStatus() {
        try {
            return getPersistentMigrationManager().getMigrationStatus();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate migration status", e);
        }
    }

    /**
     * Load a specific migration file for inspection.
     *
     * @param filename the migration file to load
     * @return the MigrationFile object
     * @throws RuntimeException if loading fails
     */
    public MigrationFile loadMigrationFile(String filename) {
        try {
            return getPersistentMigrationManager().loadMigrationFile(filename);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load migration file: " + filename, e);
        }
    }

    /**
     * Close the EntityManager and its database connection.
     */
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close EntityManager connection", e);
        }
    }

    /**
     * Get the schema introspector for advanced usage.
     */
    public SchemaIntrospector getIntrospector() {
        if (introspector == null) {
            introspector = new SchemaIntrospector(connection);
        }
        return introspector;
    }

    /**
     * Get the detected database dialect for this EntityManager.
     */
    public Dialect getDialect() {
        return dialect;
    }

    // ========================================================================
    // Query Builder Convenience Methods
    // ========================================================================

    /**
     * Create a new SELECT query builder with the dialect already configured.
     */
    public Query.SelectBuilder createSelectBuilder() {
        return Query.builder(dialect);
    }

    /**
     * Create a new SELECT query builder for specific columns with the dialect already configured.
     */
    public Query.SelectBuilder createSelectQuery(String... columns) {
        return Query.select(dialect, columns);
    }

    /**
     * Create a new INSERT query builder for a table with the dialect already configured.
     */
    public Query.InsertBuilder createInsertQuery(String table) {
        return Query.insertInto(dialect, table);
    }

    /**
     * Create a new UPDATE query builder for a table with the dialect already configured.
     */
    public Query.UpdateBuilder createUpdateQuery(String table) {
        return Query.update(dialect, table);
    }

    /**
     * Create a new DELETE query builder for a table with the dialect already configured.
     */
    public Query.DeleteBuilder createDeleteQuery(String table) {
        return Query.deleteFrom(dialect, table);
    }

    /**
     * Key for the identity map, combining entity class and ID.
     */
    @SuppressWarnings("ClassCanBeRecord")
    private static class EntityKey {
        private final Class<?> entityClass;
        private final Object id;

        public EntityKey(Class<?> entityClass, Object id) {
            this.entityClass = entityClass;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EntityKey entityKey = (EntityKey) o;
            return Objects.equals(entityClass, entityKey.entityClass) &&
                    Objects.equals(id, entityKey.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityClass, id);
        }
    }
}
