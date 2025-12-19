package de.t14d3.spool.cache.jdbc;

import de.t14d3.spool.cache.CacheEvent;
import de.t14d3.spool.cache.CacheEventSink;
import de.t14d3.spool.cache.CacheKey;
import de.t14d3.spool.query.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Stores cache invalidation events in the application database.
 *
 * Other nodes can poll for new events; writes occur on the same JDBC connection
 * as the entity writes, so events are only visible after commit.
 */
public final class JdbcCacheEventStore implements CacheEventSink {
    public static final String TABLE = "spool_cache_events";

    public record EventRow(long id, CacheEvent event) {}

    private final Connection connection;
    private final Dialect dialect;
    private boolean schemaEnsured;

    public JdbcCacheEventStore(Connection connection, Dialect dialect) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.schemaEnsured = false;
    }

    @Override
    public void append(CacheEvent event) {
        ensureSchema();

        String sql = "INSERT INTO " + dialect.quoteIdentifier(TABLE) + " (created_at, entity_class, entity_id, op) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(Instant.now()));
            ps.setString(2, event.key().entityClassName());
            ps.setString(3, event.key().id());
            ps.setString(4, event.operation().name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to append cache event", e);
        }
    }

    public List<EventRow> readSince(long lastId, int limit) {
        ensureSchema();
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }

        String sql = "SELECT id, entity_class, entity_id, op FROM " + dialect.quoteIdentifier(TABLE) + " WHERE id > ? ORDER BY id ASC LIMIT " + limit;
        List<EventRow> events = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, lastId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("id");
                    String entityClass = rs.getString("entity_class");
                    String entityId = rs.getString("entity_id");
                    String op = rs.getString("op");
                    CacheEvent.Operation operation = CacheEvent.Operation.valueOf(op);
                    CacheKey key = new CacheKey(entityClass, entityId);
                    events.add(new EventRow(id, new CacheEvent(operation, key)));
                }
            }
            return events;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read cache events", e);
        }
    }

    public void ensureSchema() {
        if (schemaEnsured) {
            return;
        }
        createTableIfMissing();
        schemaEnsured = true;
    }

    public long currentMaxId() {
        ensureSchema();
        String sql = "SELECT COALESCE(MAX(id), 0) AS max_id FROM " + dialect.quoteIdentifier(TABLE);
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                return 0L;
            }
            return rs.getLong("max_id");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read max cache event id", e);
        }
    }

    private void createTableIfMissing() {
        String idColumn = switch (dialect) {
            case POSTGRESQL -> "BIGSERIAL PRIMARY KEY";
            case SQLITE -> "INTEGER PRIMARY KEY AUTOINCREMENT";
            case H2, MYSQL, GENERIC -> "BIGINT AUTO_INCREMENT PRIMARY KEY";
        };

        String sql = "CREATE TABLE IF NOT EXISTS " + dialect.quoteIdentifier(TABLE) + " (" +
                dialect.quoteIdentifier("id") + " " + idColumn + ", " +
                dialect.quoteIdentifier("created_at") + " TIMESTAMP NOT NULL, " +
                dialect.quoteIdentifier("entity_class") + " VARCHAR(255) NOT NULL, " +
                dialect.quoteIdentifier("entity_id") + " VARCHAR(255) NOT NULL, " +
                dialect.quoteIdentifier("op") + " VARCHAR(16) NOT NULL" +
                ")";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to ensure cache events table exists", e);
        }

        // Best-effort index creation. Not all dialects support IF NOT EXISTS.
        try (PreparedStatement ps = connection.prepareStatement(
                "CREATE INDEX spool_cache_events_entity_idx ON " + dialect.quoteIdentifier(TABLE) + " (" +
                        dialect.quoteIdentifier("entity_class") + ", " + dialect.quoteIdentifier("entity_id") + ")")) {
            ps.execute();
        } catch (SQLException ignored) {
            // ignore
        }
    }
}
