package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.DirtyCheckEntity;
import de.t14d3.spool.test.entities.LifecycleEntity;
import de.t14d3.spool.test.entities.UqSingleColumn;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlushSemanticsTest {
    private static final String URL = "jdbc:h2:mem:flush_semantics_test;DB_CLOSE_DELAY=-1";

    private EntityManager em;

    @BeforeEach
    void setup() {
        LifecycleEntity.events.clear();
        DirtyCheckEntity.preUpdateCount = 0;
        em = EntityManager.create(URL);
        // Drop dependent tables before their parents (DB_CLOSE_DELAY keeps
        // the in-memory database alive across tests in this class).
        em.getExecutor().execute("DROP TABLE IF EXISTS dirty_check_entities", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS users", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS uq_single_column", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS lifecycle_entities", List.of());
        em.getExecutor().execute(
                "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(50), email VARCHAR(50))",
                List.of());
        em.getExecutor().execute(
                "CREATE TABLE uq_single_column (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255), "
                        + "CONSTRAINT uq_single_email UNIQUE (email))",
                List.of());
        em.getExecutor().execute(
                "CREATE TABLE lifecycle_entities (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))",
                List.of());
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void mutateAfterFindIsPersistedByFlush() {
        User user = new User("dirty", "old@example.com");
        em.persist(user);
        em.flush();

        // No persist() call: the change must be detected from the snapshot.
        user.setEmail("new@example.com");
        em.flush();

        assertEquals("new@example.com", readEmail(user.getId()));
    }

    @Test
    void mutateAfterFindIsPersistedInsideTransaction() {
        User user = new User("tx", "old@example.com");
        em.persist(user);
        em.flush();
        Long id = user.getId();

        em.transactional(() -> em.find(User.class, id).setEmail("committed@example.com"));

        assertEquals("committed@example.com", readEmail(id));
    }

    @Test
    void cleanFlushDoesNotWriteOrFireUpdateCallbacks() {
        LifecycleEntity entity = new LifecycleEntity("clean");
        em.persist(entity);
        em.flush();

        LifecycleEntity.events.clear();
        em.flush();
        assertTrue(LifecycleEntity.events.isEmpty(), "a clean managed entity must not be updated");

        em.clear();
        LifecycleEntity loaded = em.find(LifecycleEntity.class, entity.getId());
        assertNotNull(loaded);
        LifecycleEntity.events.clear();
        em.flush();
        assertTrue(LifecycleEntity.events.isEmpty(), "an unmodified loaded entity must not be updated");
    }

    @Test
    void relationAndArrayColumnsDoNotCauseSpuriousUpdates() {
        DirtyCheckEntity.preUpdateCount = 0;
        em.registerEntities(User.class, DirtyCheckEntity.class);
        em.updateSchema();

        User owner = new User("owner", "owner@example.com");
        DirtyCheckEntity entity = new DirtyCheckEntity("clean", new byte[]{9, 8, 7}, owner);
        em.persist(owner);
        em.persist(entity);
        em.flush();
        Long id = entity.getId();

        em.clear();
        DirtyCheckEntity loaded = em.find(DirtyCheckEntity.class, id);
        assertNotNull(loaded);
        assertNotNull(loaded.getOwner());

        DirtyCheckEntity.preUpdateCount = 0;
        em.flush();
        assertEquals(0, DirtyCheckEntity.preUpdateCount,
                "an unmodified entity with a relation and a byte[] column must not be updated");

        loaded.setName("changed");
        em.flush();
        assertEquals(1, DirtyCheckEntity.preUpdateCount);
    }

    @Test
    void failedFlushRollsBackAllWrites() {
        em.persist(new UqSingleColumn("duplicate@example.com"));
        em.persist(new UqSingleColumn("duplicate@example.com"));

        assertThrows(RuntimeException.class, em::flush);

        // The first INSERT must have been rolled back with the failed second one.
        assertEquals(0, em.getExecutor().count("uq_single_column"));
    }

    @Test
    void failedFlushCanBeRetriedAfterCancellingTheOffendingEntity() {
        UqSingleColumn first = new UqSingleColumn("retry@example.com");
        UqSingleColumn second = new UqSingleColumn("retry@example.com");
        em.persist(first);
        em.persist(second);

        assertThrows(RuntimeException.class, em::flush);
        assertEquals(0, em.getExecutor().count("uq_single_column"));

        // The failed flush requeued both inserts; drop the duplicate and retry.
        em.cancelPending(second);
        em.flush();

        assertEquals(1, em.getExecutor().count("uq_single_column"));
    }

    @Test
    void flushFailureInsideExplicitTransactionRollsBackAndDeactivatesIt() {
        em.beginTransaction();
        assertTrue(em.isTransactionActive());

        em.persist(new UqSingleColumn("explicit@example.com"));
        em.persist(new UqSingleColumn("explicit@example.com"));

        assertThrows(RuntimeException.class, em::flush);
        assertFalse(em.isTransactionActive(), "a failed flush must not leave the transaction dangling");
    }

    private String readEmail(Long id) {
        EntityManager reader = EntityManager.create(URL);
        try {
            User loaded = reader.find(User.class, id);
            assertNotNull(loaded);
            return loaded.getEmail();
        } finally {
            reader.close();
        }
    }
}
