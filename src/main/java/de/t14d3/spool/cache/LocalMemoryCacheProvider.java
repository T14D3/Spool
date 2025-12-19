package de.t14d3.spool.cache;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalMemoryCacheProvider implements CacheProvider {

    private static final class Entry {
        private final EntitySnapshot snapshot;
        private final long expiresAtMillis; // Long.MAX_VALUE means no expiry

        private Entry(EntitySnapshot snapshot, long expiresAtMillis) {
            this.snapshot = snapshot;
            this.expiresAtMillis = expiresAtMillis;
        }
    }

    private final ConcurrentHashMap<CacheKey, Entry> map = new ConcurrentHashMap<>();

    @Override
    public Optional<EntitySnapshot> get(CacheKey key) {
        Entry entry = map.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (entry.expiresAtMillis != Long.MAX_VALUE && System.currentTimeMillis() > entry.expiresAtMillis) {
            map.remove(key, entry);
            return Optional.empty();
        }
        return Optional.of(entry.snapshot);
    }

    @Override
    public void put(CacheKey key, EntitySnapshot snapshot, Duration ttl) {
        long expiresAtMillis;
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            expiresAtMillis = Long.MAX_VALUE;
        } else {
            expiresAtMillis = System.currentTimeMillis() + ttl.toMillis();
        }
        map.put(key, new Entry(snapshot, expiresAtMillis));
    }

    @Override
    public void invalidate(CacheKey key) {
        map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }
}

