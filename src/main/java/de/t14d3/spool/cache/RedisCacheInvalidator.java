package de.t14d3.spool.cache;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * Redis-based cache invalidator that uses Redis pub/sub for broadcasting
 * cache invalidations across nodes.
 *
 * <p>This implementation requires a Redis server and uses the Jedis client.
 * Jedis is an optional dependency: it is not declared transitively by Spool.
 * Applications that use this class must add {@code redis.clients:jedis} to
 * their own runtime classpath.</p>
 *
 * @see CacheInvalidator
 */
public class RedisCacheInvalidator implements CacheInvalidator {

    private static final String DEFAULT_CHANNEL = "spool:cache:invalidate";

    private final JedisPool jedisPool;
    private final String channel;
    private final Gson gson;
    private final CopyOnWriteArrayList<BiConsumer<CacheKey, String>> listeners;
    private volatile SubscriptionThread subscriptionThread;
    private volatile boolean running;

    /**
     * Creates a new Redis cache invalidator.
     *
     * @param jedisPool the Jedis pool for Redis connections
     */
    public RedisCacheInvalidator(JedisPool jedisPool) {
        this(jedisPool, DEFAULT_CHANNEL);
    }

    /**
     * Creates a new Redis cache invalidator with a custom channel.
     *
     * @param jedisPool the Jedis pool for Redis connections
     * @param channel the Redis pub/sub channel to use
     */
    public RedisCacheInvalidator(JedisPool jedisPool, String channel) {
        this.jedisPool = jedisPool;
        this.channel = channel;
        this.gson = new Gson();
        this.listeners = new CopyOnWriteArrayList<>();
        this.running = true;

        // Start subscription thread
        this.subscriptionThread = new SubscriptionThread();
        this.subscriptionThread.start();
    }

    @Override
    public void broadcastInvalidation(CacheKey key, String sourceNode) {
        InvalidationMessage message = InvalidationMessage.from(key, sourceNode, System.currentTimeMillis());

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(channel, gson.toJson(message));
        }
    }

    @Override
    public void broadcastClear(String sourceNode) {
        InvalidationMessage message = InvalidationMessage.clear(sourceNode, System.currentTimeMillis());

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(channel, gson.toJson(message));
        }
    }

    @Override
    public void registerListener(BiConsumer<CacheKey, String> listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(BiConsumer<CacheKey, String> listener) {
        listeners.remove(listener);
    }

    @Override
    public void close() {
        running = false;

        if (subscriptionThread != null) {
            subscriptionThread.unsubscribe();
            subscriptionThread.interrupt();
        }

        jedisPool.close();
    }

    /**
     * Handles an incoming invalidation message.
     *
     * @param json the JSON message
     */
    private void handleMessage(String json) {
        try {
            InvalidationMessage message = gson.fromJson(json, InvalidationMessage.class);

            // Check for clear command
            if (message.isClear()) {
                // Notify all listeners with null key (indicates clear)
                for (BiConsumer<CacheKey, String> listener : listeners) {
                    listener.accept(null, message.sourceNode());
                }
                return;
            }

            CacheKey key = message.toCacheKey();

            for (BiConsumer<CacheKey, String> listener : listeners) {
                listener.accept(key, message.sourceNode());
            }
        } catch (Exception e) {
            // Log error but don't fail
            System.err.println("Failed to handle cache invalidation message: " + e.getMessage());
        }
    }

    /**
     * Thread that subscribes to Redis pub/sub channel.
     */
    private class SubscriptionThread extends Thread {
        private volatile JedisPubSub pubSub;

        SubscriptionThread() {
            super("RedisCacheInvalidator-Subscriber");
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running) {
                try (Jedis jedis = jedisPool.getResource()) {
                    pubSub = new JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            handleMessage(message);
                        }
                    };

                    jedis.subscribe(pubSub, channel);
                } catch (Exception e) {
                    if (running) {
                        System.err.println("Redis subscription error: " + e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }

        void unsubscribe() {
            JedisPubSub current = pubSub;
            if (current != null) {
                current.unsubscribe();
            }
        }
    }
}
