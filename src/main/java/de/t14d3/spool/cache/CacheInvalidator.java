package de.t14d3.spool.cache;

import java.util.function.BiConsumer;

/**
 * Interface for broadcasting cache invalidations across nodes.
 *
 * <p>Implementations handle the transport mechanism (Redis, JDBC, etc.)
 * for notifying other nodes about cache changes.</p>
 *
 * @see DistributedCacheProvider
 */
public interface CacheInvalidator extends AutoCloseable {

    /**
     * Broadcasts an invalidation for a specific cache key.
     *
     * @param key the cache key to invalidate
     * @param sourceNode the node initiating the invalidation
     */
    void broadcastInvalidation(CacheKey key, String sourceNode);

    /**
     * Broadcasts a clear command to all nodes.
     *
     * @param sourceNode the node initiating the clear
     */
    void broadcastClear(String sourceNode);

    /**
     * Registers a listener for remote invalidations.
     *
     * @param listener the listener to register
     */
    void registerListener(BiConsumer<CacheKey, String> listener);

    /**
     * Unregisters a listener.
     *
     * @param listener the listener to unregister
     */
    void unregisterListener(BiConsumer<CacheKey, String> listener);
}
