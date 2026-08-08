package de.t14d3.spool.cache;

import com.google.gson.Gson;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * JDBC-based cache invalidator that uses a database table for broadcasting
 * cache invalidations across nodes. This implementation polls the database
 * for new invalidation messages.
 *
 * <p>This implementation is suitable when Redis is not available but a shared
 * database is accessible from all nodes.</p>
 *
 * @see CacheInvalidator
 */
public class JdbcCacheInvalidator implements CacheInvalidator {

    private static final String DEFAULT_TABLE_NAME = "spool_cache_invalidation";
    private static final long DEFAULT_POLL_INTERVAL_MS = 1000;
    private static final long DEFAULT_MESSAGE_TTL_MS = 60000; // 1 minute

    private final DataSource dataSource;
    private final String tableName;
    private final String nodeId;
    private final long pollIntervalMs;
    private final long messageTtlMs;
    private final Gson gson;
    private final CopyOnWriteArrayList<BiConsumer<CacheKey, String>> listeners;
    private final ScheduledExecutorService executor;
    private volatile long lastPollTime;
    private volatile boolean running;

    /**
     * Creates a new JDBC cache invalidator.
     *
     * @param dataSource the data source for database connections
     * @param nodeId unique identifier for this node
     */
    public JdbcCacheInvalidator(DataSource dataSource, String nodeId) {
        this(dataSource, nodeId, DEFAULT_TABLE_NAME, DEFAULT_POLL_INTERVAL_MS, DEFAULT_MESSAGE_TTL_MS);
    }

    /**
     * Creates a new JDBC cache invalidator with custom settings.
     *
     * @param dataSource the data source for database connections
     * @param nodeId unique identifier for this node
     * @param tableName the database table name for invalidation messages
     * @param pollIntervalMs the polling interval in milliseconds
     * @param messageTtlMs the TTL for messages in milliseconds
     */
    public JdbcCacheInvalidator(DataSource dataSource, String nodeId, String tableName,
                                 long pollIntervalMs, long messageTtlMs) {
        this.dataSource = dataSource;
        this.nodeId = nodeId;
        this.tableName = tableName;
        this.pollIntervalMs = pollIntervalMs;
        this.messageTtlMs = messageTtlMs;
        this.gson = new Gson();
        this.listeners = new CopyOnWriteArrayList<>();
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "JdbcCacheInvalidator-Poller");
            t.setDaemon(true);
            return t;
        });
        this.running = true;
        this.lastPollTime = System.currentTimeMillis() - pollIntervalMs;

        // Ensure table exists
        ensureTableExists();

        // Start polling
        executor.scheduleWithFixedDelay(
            this::poll,
            pollIntervalMs,
            pollIntervalMs,
            TimeUnit.MILLISECONDS
        );

        // Schedule cleanup
        executor.scheduleWithFixedDelay(
            this::cleanup,
            messageTtlMs,
            messageTtlMs,
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void broadcastInvalidation(CacheKey key, String sourceNode) {
        InvalidationMessage message = InvalidationMessage.from(key, sourceNode, System.currentTimeMillis());
        insertMessage(message);
    }

    @Override
    public void broadcastClear(String sourceNode) {
        InvalidationMessage message = InvalidationMessage.clear(sourceNode, System.currentTimeMillis());
        insertMessage(message);
    }

    @Override
    public void registerListener(BiConsumer<CacheKey, String> listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(BiConsumer<CacheKey, String> listener) {
        listeners.remove(listener);
    }

    @Override
    public void close() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Ensures the invalidation table exists in the database.
     */
    private void ensureTableExists() {
        String sql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
            "entity_class VARCHAR(255) NOT NULL," +
            "entity_id VARCHAR(255) NOT NULL," +
            "source_node VARCHAR(255) NOT NULL," +
            "timestamp BIGINT NOT NULL," +
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",
            tableName
        );

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create invalidation table", e);
        }
    }

    /**
     * Inserts an invalidation message into the database.
     *
     * @param message the message to insert
     */
    private void insertMessage(InvalidationMessage message) {
        String sql = String.format(
            "INSERT INTO %s (entity_class, entity_id, source_node, timestamp) VALUES (?, ?, ?, ?)",
            tableName
        );

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, message.entityClassName());
            stmt.setString(2, message.id());
            stmt.setString(3, message.sourceNode());
            stmt.setLong(4, message.timestamp());
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Failed to insert invalidation message: " + e.getMessage());
        }
    }

    /**
     * Polls the database for new invalidation messages.
     */
    private void poll() {
        if (!running) {
            return;
        }

        String sql = String.format(
            "SELECT entity_class, entity_id, source_node, timestamp FROM %s " +
            "WHERE timestamp > ? AND source_node != ? " +
            "ORDER BY timestamp ASC",
            tableName
        );

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, lastPollTime);
            stmt.setString(2, nodeId);

            try (ResultSet rs = stmt.executeQuery()) {
                long maxTimestamp = lastPollTime;

                while (rs.next()) {
                    String entityClass = rs.getString("entity_class");
                    String entityId = rs.getString("entity_id");
                    String sourceNode = rs.getString("source_node");
                    long timestamp = rs.getLong("timestamp");

                    InvalidationMessage message = new InvalidationMessage(entityClass, entityId, sourceNode, timestamp);

                    // Check for clear command
                    if (message.isClear()) {
                        for (BiConsumer<CacheKey, String> listener : listeners) {
                            listener.accept(null, sourceNode);
                        }
                    } else {
                        CacheKey key = message.toCacheKey();
                        for (BiConsumer<CacheKey, String> listener : listeners) {
                            listener.accept(key, sourceNode);
                        }
                    }

                    maxTimestamp = Math.max(maxTimestamp, timestamp);
                }

                lastPollTime = maxTimestamp;
            }
        } catch (SQLException e) {
            System.err.println("Failed to poll invalidation messages: " + e.getMessage());
        }
    }

    /**
     * Cleans up old invalidation messages from the database.
     */
    private void cleanup() {
        if (!running) {
            return;
        }

        long cutoff = System.currentTimeMillis() - messageTtlMs;
        String sql = String.format(
            "DELETE FROM %s WHERE timestamp < ?",
            tableName
        );

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, cutoff);
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Failed to cleanup invalidation messages: " + e.getMessage());
        }
    }
}
