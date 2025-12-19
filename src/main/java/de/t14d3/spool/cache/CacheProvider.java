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

    @Override
    default void close() {
        // no-op
    }
}

