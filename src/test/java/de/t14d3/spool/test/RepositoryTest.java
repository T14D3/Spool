package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UserRepository extends EntityRepository<User> {
    public UserRepository(EntityManager em) {
        super(em, User.class);
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RepositoryTest {
    private EntityManager em;
    private UserRepository repo;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:test2;DB_CLOSE_DELAY=-1");
        repo = new UserRepository(em);
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void testSaveAndFindById() {
        User u = new User(10L, "eve", "eve@example.com");
        repo.save(u);
        em.flush();

        User found = repo.findById(u.getId());
        assertNotNull(found);
        assertEquals("eve", found.getUsername());
    }

    @Test
    void testFindAll() {
        User u1 = new User(null, "frank", "frank@example.com");
        User u2 = new User(null, "grace", "grace@example.com");
        repo.save(u1);
        repo.save(u2);
        em.flush();

        List<User> list = repo.findAll();
        assertTrue(list.stream().anyMatch(u -> u.getId().equals(u1.getId())));
        assertTrue(list.stream().anyMatch(u -> u.getId().equals(u2.getId())));
    }

    @Test
    void testUpdateEntity() {
        User u = new User(5L, "erin", "erin@old.com");
        em.persist(u);
        em.flush();

        User loaded = em.find(User.class, 5L);
        assertEquals("erin@old.com", loaded.getEmail());
        loaded.setEmail("erin@new.com");
        // flag for update
        em.persist(loaded);
        em.flush();

        User updated = em.find(User.class, 5L);
        assertNotNull(updated);
        assertEquals("erin@new.com", updated.getEmail());
    }

    @Test
    void testDelete() {
        User u = new User(13L, "heidi", "heidi@example.com");
        repo.save(u);
        em.flush();

        User found = repo.findById(13L);
        assertNotNull(found);

        repo.delete(found);
        em.flush();

        assertNull(repo.findById(13L));
    }

    @Test
    void testUpdateViaRepository() {
        User u = new User(20L, "ivan", "ivan@old.com");
        repo.save(u);
        em.flush();

        User loaded = repo.findById(20L);
        assertEquals("ivan@old.com", loaded.getEmail());
        loaded.setEmail("ivan@new.com");
        repo.save(loaded);
        em.flush();

        User updated = repo.findById(20L);
        assertNotNull(updated);
        assertEquals("ivan@new.com", updated.getEmail());
    }
}
