package de.t14d3.spool.cache;

/**
 * Handler for cache events.
 *
 * <p>Implementations can be registered with {@link CacheProvider#setEventHandler(CacheEventHandler)}
 * to receive notifications about cache operations.</p>
 *
 * @see CacheProvider#setEventHandler(CacheEventHandler)
 */
public interface CacheEventHandler {

    /**
     * Called when a cache entry is retrieved (hit or miss).
     *
     * @param key the cache key
     * @param hit true if it was a cache hit, false if miss
     */
    default void onGet(CacheKey key, boolean hit) {
        // no-op by default
    }

    /**
     * Called when a cache entry is stored.
     *
     * @param key the cache key
     * @param snapshot the stored snapshot
     */
    default void onPut(CacheKey key, EntitySnapshot snapshot) {
        // no-op by default
    }

    /**
     * Called when a cache entry is invalidated.
     *
     * @param key the cache key
     */
    default void onInvalidate(CacheKey key) {
        // no-op by default
    }

    /**
     * Called when the cache is cleared.
     */
    default void onClear() {
        // no-op by default
    }
}
