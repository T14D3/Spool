package de.t14d3.spool.test;

import de.t14d3.spool.cache.CacheKey;
import de.t14d3.spool.cache.LocalMemoryCacheProvider;
import de.t14d3.spool.cache.jdbc.JdbcCacheEventStore;
import de.t14d3.spool.cache.jdbc.JdbcPollingInvalidationListener;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class CacheInvalidationTest {

    @Test
    void testCrossEntityManagerInvalidationViaJdbcEventStore() {
        String dbName = "cache_invalidation_" + UUID.randomUUID().toString().replace("-", "");
        String jdbcUrl = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";

        LocalMemoryCacheProvider cacheWriter = new LocalMemoryCacheProvider();
        LocalMemoryCacheProvider cacheReader = new LocalMemoryCacheProvider();

        EntityManager emWriter = EntityManager.create(jdbcUrl).withCacheProvider(cacheWriter);
        JdbcCacheEventStore writerEventStore = new JdbcCacheEventStore(emWriter.getExecutor().getConnection(), emWriter.getDialect());
        emWriter.withCacheEventSink(writerEventStore);

        EntityManager emReader = EntityManager.create(jdbcUrl).withCacheProvider(cacheReader);

        // Create schema
        try {
            emWriter.getExecutor().execute("DROP TABLE IF EXISTS users", List.of());
        } catch (Exception ignored) {
            // ignore
        }
        emWriter.getExecutor().execute(
                "CREATE TABLE users (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                        "username VARCHAR(50), " +
                        "email VARCHAR(50)" +
                        ")", List.of()
        );

        try (JdbcPollingInvalidationListener listener = JdbcPollingInvalidationListener.connect(jdbcUrl, cacheReader, Duration.ofMillis(20))) {
            listener.start();

            EntityRepository<User> writerRepo = new EntityRepository<>(emWriter, User.class);

            User user = new User("cached_user", "v1@example.com");
            writerRepo.save(user);
            emWriter.flush();

            CacheKey key = CacheKey.of(User.class, user.getId());

            // Reader loads and caches v1
            User readV1 = emReader.find(User.class, user.getId());
            assertEquals("v1@example.com", readV1.getEmail());
            assertTrue(cacheReader.get(key).isPresent());

            // Clear first-level cache so next find would hit L2 if still present
            emReader.clear();

            // Writer updates and flushes (appends invalidation event)
            User toUpdate = emWriter.find(User.class, user.getId());
            toUpdate.setEmail("v2@example.com");
            writerRepo.save(toUpdate);
            emWriter.flush();

            // Wait for listener to invalidate reader cache
            long deadline = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < deadline && cacheReader.get(key).isPresent()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            assertFalse(cacheReader.get(key).isPresent(), "expected cache entry to be invalidated");

            // Next read should come from DB (and repopulate L2 with v2)
            User readV2 = emReader.find(User.class, user.getId());
            assertEquals("v2@example.com", readV2.getEmail());
            assertTrue(cacheReader.get(key).isPresent());
        } finally {
            emWriter.close();
            emReader.close();
        }
    }
}
