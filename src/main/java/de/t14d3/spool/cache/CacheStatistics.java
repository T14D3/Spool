package de.t14d3.spool.cache;

/**
 * Statistics for cache operations.
 *
 * <p>This class provides metrics about cache performance including
 * hits, misses, and invalidations.</p>
 *
 * @see CacheProvider#getStats()
 */
public final class CacheStatistics {

    private final long hits;
    private final long misses;
    private final long invalidations;
    private final long size;

    /**
     * Creates cache statistics.
     *
     * @param hits the number of cache hits
     * @param misses the number of cache misses
     * @param invalidations the number of invalidations
     * @param size the current cache size
     */
    public CacheStatistics(long hits, long misses, long invalidations, long size) {
        this.hits = hits;
        this.misses = misses;
        this.invalidations = invalidations;
        this.size = size;
    }

    /**
     * Returns empty statistics (all zeros).
     *
     * @return empty statistics
     */
    public static CacheStatistics empty() {
        return new CacheStatistics(0, 0, 0, 0);
    }

    /**
     * Gets the number of cache hits.
     *
     * @return cache hits
     */
    public long hits() {
        return hits;
    }

    /**
     * Gets the number of cache misses.
     *
     * @return cache misses
     */
    public long misses() {
        return misses;
    }

    /**
     * Gets the number of invalidations.
     *
     * @return invalidations
     */
    public long invalidations() {
        return invalidations;
    }

    /**
     * Gets the current cache size.
     *
     * @return cache size
     */
    public long size() {
        return size;
    }

    /**
     * Gets the hit ratio (hits / (hits + misses)).
     *
     * @return hit ratio between 0.0 and 1.0
     */
    public double hitRatio() {
        long total = hits + misses;
        return total == 0 ? 0.0 : (double) hits / total;
    }

    @Override
    public String toString() {
        return String.format("CacheStatistics{hits=%d, misses=%d, invalidations=%d, size=%d, hitRatio=%.2f}",
            hits, misses, invalidations, size, hitRatio());
    }
}
