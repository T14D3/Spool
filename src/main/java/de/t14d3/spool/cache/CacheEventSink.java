package de.t14d3.spool.cache;

/**
 * Persists or broadcasts invalidation events so other processes can invalidate their caches.
 *
 * Implementations should be commit-aware: if used with JDBC transactions, inserts must occur on the
 * same connection/transaction so events are only visible after commit.
 */
public interface CacheEventSink {
    void append(CacheEvent event);
}

