package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityManagerTest {
    private EntityManager em;

    static {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("H2 Driver not found", e);
        }
    }

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        // Ensure table is created
        EntityMetadata.of(User.class);
        em.getExecutor().execute(
                "CREATE TABLE users (id BIGINT PRIMARY KEY, username VARCHAR(50), email VARCHAR(50))", List.of()
        );
    }

    @AfterEach
    void teardown() {
        em.getExecutor().execute("DROP TABLE users", List.of());
        em.close();
    }

    @Test
    void testPersistAndFind() {
        User u = new User(1L, "alice", "alice@example.com");
        em.persist(u);
        em.flush();

        User found = em.find(User.class, u.getId());
        assertNotNull(found);
        assertEquals("alice", found.getUsername());
        assertEquals("alice@example.com", found.getEmail());
    }

    @Test
    void testRemove() {
        User u = new User(2L, "bob", "bob@example.com");
        em.persist(u);
        em.flush();

        User found = em.find(User.class, u.getId());
        assertNotNull(found);

        em.remove(found);
        em.flush();

        User deleted = em.find(User.class, 2L);
        assertNull(deleted);
    }

    @Test
    void testFindAll() {
        User u1 = new User(1L, "carol", "carol@example.com");
        User u2 = new User(2L, "dave", "dave@example.com");
        em.persist(u1);
        em.persist(u2);
        em.flush();

        List<User> all = em.getExecutor().findAll(User.class);
        assertTrue(all.size() >= 2);
    }
}
