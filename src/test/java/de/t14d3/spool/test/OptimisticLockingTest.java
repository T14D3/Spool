package de.t14d3.spool.test;

import de.t14d3.spool.OptimisticLockException;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.VersionedCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OptimisticLockingTest {
    private static final String URL = "jdbc:h2:mem:optimistic_lock_test;DB_CLOSE_DELAY=-1";

    private EntityManager em;

    @BeforeEach
    void setup() {
        em = EntityManager.create(URL);
        em.getExecutor().execute("DROP TABLE IF EXISTS versioned_counters", java.util.List.of());
        em.registerEntities(VersionedCounter.class);
        em.updateSchema();
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void versionStartsAtZeroAndIncrementsOnUpdate() {
        VersionedCounter counter = new VersionedCounter("first");
        em.persist(counter);
        em.flush();
        assertEquals(0L, counter.getVersion(), "a new entity starts at version 0");

        counter.setName("second");
        em.flush();
        assertEquals(1L, counter.getVersion());

        em.clear();
        VersionedCounter loaded = em.find(VersionedCounter.class, counter.getId());
        assertEquals(1L, loaded.getVersion());
        assertEquals("second", loaded.getName());
    }

    @Test
    void staleUpdateIsRejected() {
        VersionedCounter counter = new VersionedCounter("original");
        em.persist(counter);
        em.flush();
        Long id = counter.getId();

        EntityManager other = EntityManager.create(URL);
        try {
            VersionedCounter winner = em.find(VersionedCounter.class, id);
            VersionedCounter loser = other.find(VersionedCounter.class, id);
            assertNotNull(winner);
            assertNotNull(loser);

            winner.setName("winner");
            em.flush();

            loser.setName("loser");
            RuntimeException failure = assertThrows(RuntimeException.class, other::flush);
            assertTrue(causeChainContainsOptimisticLock(failure),
                    "expected an OptimisticLockException, got: " + failure);

            // The losing write must not have been applied.
            EntityManager reader = EntityManager.create(URL);
            try {
                VersionedCounter reloaded = reader.find(VersionedCounter.class, id);
                assertEquals("winner", reloaded.getName());
                assertEquals(1L, reloaded.getVersion());
            } finally {
                reader.close();
            }
        } finally {
            other.close();
        }
    }

    @Test
    void updateOfDeletedRowIsRejected() {
        VersionedCounter counter = new VersionedCounter("doomed");
        em.persist(counter);
        em.flush();
        Long id = counter.getId();

        EntityManager other = EntityManager.create(URL);
        try {
            VersionedCounter deleted = em.find(VersionedCounter.class, id);
            VersionedCounter stale = other.find(VersionedCounter.class, id);

            em.remove(deleted);
            em.flush();

            stale.setName("zombie");
            RuntimeException failure = assertThrows(RuntimeException.class, other::flush);
            assertTrue(causeChainContainsOptimisticLock(failure),
                    "expected an OptimisticLockException, got: " + failure);
        } finally {
            other.close();
        }
    }

    private static boolean causeChainContainsOptimisticLock(Throwable failure) {
        Throwable current = failure;
        int hops = 0;
        while (current != null && hops++ < 16) {
            if (current instanceof OptimisticLockException) {
                return true;
            }
            if (current.getCause() == current) {
                return false;
            }
            current = current.getCause();
        }
        return false;
    }
}
