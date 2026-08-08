package de.t14d3.spool.cache;

/**
 * Message format for cache invalidation broadcasts.
 *
 * <p>This record is used to serialize and deserialize invalidation messages
 * when broadcasting across nodes via Redis, JDBC, or other transports.</p>
 *
 * @see RedisCacheInvalidator
 * @see JdbcCacheInvalidator
 */
public record InvalidationMessage(
    String entityClassName,
    String id,
    String sourceNode,
    long timestamp
) {
    
    /**
     * Creates a cache key from this invalidation message.
     *
     * @return the cache key
     */
    public CacheKey toCacheKey() {
        return new CacheKey(entityClassName, id);
    }
    
    /**
     * Creates an invalidation message from a cache key.
     *
     * @param key the cache key
     * @param sourceNode the source node
     * @param timestamp the timestamp
     * @return the invalidation message
     */
    public static InvalidationMessage from(CacheKey key, String sourceNode, long timestamp) {
        return new InvalidationMessage(key.entityClassName(), key.id(), sourceNode, timestamp);
    }
    
    /**
     * Checks if this message represents a clear command.
     *
     * @return true if this is a clear command
     */
    public boolean isClear() {
        return "__CLEAR__".equals(entityClassName);
    }
    
    /**
     * Creates a clear command message.
     *
     * @param sourceNode the source node
     * @param timestamp the timestamp
     * @return the clear command message
     */
    public static InvalidationMessage clear(String sourceNode, long timestamp) {
        return new InvalidationMessage("__CLEAR__", "__CLEAR__", sourceNode, timestamp);
    }
}
