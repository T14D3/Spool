package de.t14d3.spool.cache;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cache provider that wraps a local cache and adds distributed invalidation.
 *
 * <p>This provider maintains a local cache for fast access while broadcasting
 * invalidation events to other nodes when entries are modified or deleted.</p>
 *
 * @see CacheProvider
 * @see CacheInvalidator
 */
public class DistributedCacheProvider implements CacheProvider {

    private final CacheProvider localCache;
    private final CacheInvalidator invalidator;
    private final String nodeId;
    private final AtomicLong hits;
    private final AtomicLong misses;
    private final AtomicLong invalidationsSent;
    private final AtomicLong invalidationsReceived;
    private volatile CacheEventHandler eventHandler;

    /**
     * Creates a new distributed cache provider.
     *
     * @param localCache the local cache to wrap
     * @param invalidator the invalidator for broadcasting changes
     * @param nodeId the unique identifier for this node
     */
    public DistributedCacheProvider(CacheProvider localCache, CacheInvalidator invalidator, String nodeId) {
        this.localCache = localCache;
        this.invalidator = invalidator;
        this.nodeId = nodeId;
        this.hits = new AtomicLong(0);
        this.misses = new AtomicLong(0);
        this.invalidationsSent = new AtomicLong(0);
        this.invalidationsReceived = new AtomicLong(0);

        // Register to receive remote invalidations
        this.invalidator.registerListener(this::onRemoteInvalidation);
    }

    @Override
    public Optional<EntitySnapshot> get(CacheKey key) {
        Optional<EntitySnapshot> result = localCache.get(key);

        if (result.isPresent()) {
            hits.incrementAndGet();
            if (eventHandler != null) {
                eventHandler.onGet(key, true);
            }
        } else {
            misses.incrementAndGet();
            if (eventHandler != null) {
                eventHandler.onGet(key, false);
            }
        }

        return result;
    }

    @Override
    public void put(CacheKey key, EntitySnapshot snapshot, Duration ttl) {
        localCache.put(key, snapshot, ttl);

        if (eventHandler != null) {
            eventHandler.onPut(key, snapshot);
        }

        // Note: We don't broadcast puts, only invalidations
        // This is because puts are local operations and other nodes
        // will fetch from DB when they need the data
    }

    @Override
    public void invalidate(CacheKey key) {
        // Invalidate locally first
        localCache.invalidate(key);
        invalidationsSent.incrementAndGet();

        if (eventHandler != null) {
            eventHandler.onInvalidate(key);
        }

        // Broadcast to other nodes
        invalidator.broadcastInvalidation(key, nodeId);
    }

    @Override
    public void clear() {
        localCache.clear();

        if (eventHandler != null) {
            eventHandler.onClear();
        }

        // Broadcast clear to other nodes
        invalidator.broadcastClear(nodeId);
    }

    /**
     * Handles invalidation messages from remote nodes.
     *
     * @param key the cache key to invalidate
     * @param sourceNode the node that sent the invalidation
     */
    private void onRemoteInvalidation(CacheKey key, String sourceNode) {
        // Don't process our own invalidations
        if (nodeId.equals(sourceNode)) {
            return;
        }

        invalidationsReceived.incrementAndGet();
        localCache.invalidate(key);
    }

    @Override
    public CacheStatistics getStats() {
        long localSize = localCache.getStats().size();
        return new CacheStatistics(
            hits.get(),
            misses.get(),
            invalidationsSent.get() + invalidationsReceived.get(),
            localSize
        );
    }

    @Override
    public void setEventHandler(CacheEventHandler handler) {
        this.eventHandler = handler;
        localCache.setEventHandler(handler);
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public void close() {
        try {
            invalidator.close();
        } catch (Exception ignored) {
        }
        try {
            localCache.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Gets the number of invalidations sent to other nodes.
     *
     * @return invalidations sent
     */
    public long getInvalidationsSent() {
        return invalidationsSent.get();
    }

    /**
     * Gets the number of invalidations received from other nodes.
     *
     * @return invalidations received
     */
    public long getInvalidationsReceived() {
        return invalidationsReceived.get();
    }
}
