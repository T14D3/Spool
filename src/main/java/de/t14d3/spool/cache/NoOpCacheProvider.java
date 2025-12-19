package de.t14d3.spool.cache;

import java.time.Duration;
import java.util.Optional;

public final class NoOpCacheProvider implements CacheProvider {
    @Override
    public Optional<EntitySnapshot> get(CacheKey key) {
        return Optional.empty();
    }

    @Override
    public void put(CacheKey key, EntitySnapshot snapshot, Duration ttl) {
        // no-op
    }

    @Override
    public void invalidate(CacheKey key) {
        // no-op
    }

    @Override
    public void clear() {
        // no-op
    }
}

