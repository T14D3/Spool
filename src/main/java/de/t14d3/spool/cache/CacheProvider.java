package de.t14d3.spool.cache;

import java.time.Duration;
import java.util.Optional;

/**
 * Stores entity snapshots for fast entity-by-id reads.
 *
 * Implementations must not return mutable snapshots that can be modified by callers.
 */
public interface CacheProvider extends AutoCloseable {

    Optional<EntitySnapshot> get(CacheKey key);

    void put(CacheKey key, EntitySnapshot snapshot, Duration ttl);

    void invalidate(CacheKey key);

    void clear();

    /**
     * Gets cache statistics for monitoring and debugging.
     *
     * <p>The default implementation returns empty statistics.
     * Implementations that support statistics should override this method.</p>
     *
     * @return the cache statistics
     */
    default CacheStatistics getStats() {
        return CacheStatistics.empty();
    }

    /**
     * Sets an event handler for cache operations.
     *
     * <p>This allows external systems to be notified of cache events
     * such as puts, invalidations, and hits/misses.</p>
     *
     * <p>The default implementation is a no-op.</p>
     *
     * @param handler the event handler, or null to remove
     */
    default void setEventHandler(CacheEventHandler handler) {
        // no-op by default
    }

    /**
     * Checks if this cache provider supports distributed caching.
     *
     * <p>Distributed cache providers can synchronize cache state across
     * multiple servers or processes.</p>
     *
     * <p>The default implementation returns false.</p>
     *
     * @return true if this is a distributed cache provider
     */
    default boolean isDistributed() {
        return false;
    }

    @Override
    default void close() {
        // no-op
    }
}
