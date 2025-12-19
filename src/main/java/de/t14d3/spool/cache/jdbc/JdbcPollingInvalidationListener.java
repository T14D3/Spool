package de.t14d3.spool.cache.jdbc;

import de.t14d3.spool.cache.CacheEvent;
import de.t14d3.spool.cache.CacheProvider;
import de.t14d3.spool.cache.CacheKey;
import de.t14d3.spool.query.Dialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls {@link JdbcCacheEventStore} for new events and invalidates a local {@link CacheProvider}.
 *
 * This provides cross-process cache freshness without requiring external infrastructure.
 */
public final class JdbcPollingInvalidationListener implements AutoCloseable {
    private final Connection connection;
    private final JdbcCacheEventStore store;
    private final CacheProvider cacheProvider;
    private final Duration pollInterval;
    private final AtomicBoolean running;
    private volatile Thread thread;
    private volatile long lastSeenId;

    public static JdbcPollingInvalidationListener connect(String jdbcUrl, CacheProvider cacheProvider, Duration pollInterval) {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl);
            Dialect dialect = Dialect.detectFromUrl(jdbcUrl);
            return new JdbcPollingInvalidationListener(connection, dialect, cacheProvider, pollInterval);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create invalidation listener connection", e);
        }
    }

    public JdbcPollingInvalidationListener(Connection connection, Dialect dialect, CacheProvider cacheProvider, Duration pollInterval) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.store = new JdbcCacheEventStore(connection, Objects.requireNonNull(dialect, "dialect"));
        this.cacheProvider = Objects.requireNonNull(cacheProvider, "cacheProvider");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.running = new AtomicBoolean(false);
        this.lastSeenId = 0L;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        thread = new Thread(this::runLoop, "spool-cache-invalidation");
        thread.setDaemon(true);
        thread.start();
    }

    private void runLoop() {
        store.ensureSchema();
        lastSeenId = store.currentMaxId();

        while (running.get()) {
            try {
                List<JdbcCacheEventStore.EventRow> rows = store.readSince(lastSeenId, 500);
                for (JdbcCacheEventStore.EventRow row : rows) {
                    lastSeenId = Math.max(lastSeenId, row.id());
                    CacheEvent event = row.event();
                    CacheKey key = event.key();
                    cacheProvider.invalidate(key);
                }

                Thread.sleep(Math.max(1L, pollInterval.toMillis()));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ignored) {
                // Best-effort; try again next tick.
                try {
                    Thread.sleep(Math.max(1L, pollInterval.toMillis()));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
        try {
            connection.close();
        } catch (Exception ignored) {
            // ignore
        }
    }
}
