package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.LifecycleEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LifecycleCallbackTest {
    private EntityManager em;

    @BeforeEach
    void setup() throws SQLException {
        LifecycleEntity.events.clear();
        LifecycleEntity.failOnPrePersist = false;
        em = EntityManager.create("jdbc:h2:mem:lifecycle_test;DB_CLOSE_DELAY=-1");
        em.getExecutor().execute("DROP TABLE IF EXISTS lifecycle_entities", List.of());
        em.getExecutor().execute(
                "CREATE TABLE lifecycle_entities (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))",
                List.of()
        );
    }

    @AfterEach
    void teardown() {
        LifecycleEntity.failOnPrePersist = false;
        em.close();
    }

    @Test
    void persistFiresPreAndPostInOrder() {
        LifecycleEntity entity = new LifecycleEntity("alice");
        em.persist(entity);
        em.flush();

        assertNotNull(entity.getId());
        assertEquals(List.of("prePersist:alice", "postPersist:true"), LifecycleEntity.events);
    }

    @Test
    void preLoadFiresOnDatabaseHydrationOnly() {
        LifecycleEntity entity = new LifecycleEntity("bob");
        em.persist(entity);
        em.flush();

        LifecycleEntity.events.clear();
        em.find(LifecycleEntity.class, entity.getId());
        assertTrue(LifecycleEntity.events.isEmpty(), "identity map hit must not fire @PreLoad");

        em.detach(entity);
        LifecycleEntity reloaded = em.find(LifecycleEntity.class, entity.getId());
        assertNotNull(reloaded);
        assertEquals(List.of("preLoad:bob"), LifecycleEntity.events);
    }

    @Test
    void updateFiresPreAndPostWhenDirty() {
        LifecycleEntity entity = new LifecycleEntity("carol");
        em.persist(entity);
        em.flush();

        LifecycleEntity.events.clear();
        entity.setName("carol-updated");
        em.persist(entity);
        em.flush();

        assertEquals(List.of("preUpdate:carol-updated", "postUpdate:carol-updated"), LifecycleEntity.events);
    }

    @Test
    void updateDoesNotFireWhenClean() {
        LifecycleEntity entity = new LifecycleEntity("dave");
        em.persist(entity);
        em.flush();

        LifecycleEntity.events.clear();
        em.persist(entity);
        em.flush();

        assertTrue(LifecycleEntity.events.isEmpty(), "no dirty columns must not fire @PreUpdate/@PostUpdate");
    }

    @Test
    void removeFiresPreAndPost() {
        LifecycleEntity entity = new LifecycleEntity("erin");
        em.persist(entity);
        em.flush();

        LifecycleEntity.events.clear();
        em.remove(entity);
        em.flush();

        assertEquals(List.of("preRemove:erin", "postRemove:erin"), LifecycleEntity.events);
    }

    @Test
    void failingPrePersistAbortsFlush() {
        LifecycleEntity.failOnPrePersist = true;
        LifecycleEntity entity = new LifecycleEntity("frank");
        em.persist(entity);

        RuntimeException failure = assertThrows(RuntimeException.class, em::flush);
        assertNotNull(failure.getMessage());

        LifecycleEntity.failOnPrePersist = false;
    }
}
