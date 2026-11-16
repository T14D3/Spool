package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the contract of the convenience constructor that takes the entity
 * class from the type argument the concrete repository subclass binds.
 */
public class RepositoryTypeInferenceTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        // A per-test database name: DB_CLOSE_DELAY keeps an in-memory
        // database alive for the whole JVM, so a shared name would leak
        // rows between tests.
        em = EntityManager.create("jdbc:h2:mem:repository_type_inference_"
                + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        em.registerEntities(User.class);
        em.updateSchema();
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void subclassBindsTheEntityClass() {
        UserRepository repository = new UserRepository(em);

        repository.save(new User("inferred", "inferred@example.com"));
        em.flush();

        assertEquals(1, repository.count());
        assertNotNull(repository.findOneBy("username", "inferred"));
    }

    @Test
    void anIntermediateRepositoryClassStillResolves() {
        // The binding sits on the grand-parent, two levels up.
        InheritedUserRepository repository = new InheritedUserRepository(em);

        repository.save(new User("inherited", "inherited@example.com"));
        em.flush();

        assertEquals(1, repository.count());
    }

    @Test
    void theExplicitConstructorStillWorks() {
        EntityRepository<User> repository = new EntityRepository<>(em, User.class);

        repository.save(new User("explicit", "explicit@example.com"));
        em.flush();

        assertEquals(1, repository.count());
    }

    @Test
    void anUnbindableRepositoryFailsWithAClearMessage() {
        // A repository that binds a type variable cannot be resolved, and
        // must say so instead of failing with an obscure error.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new GenericRepository<>(em));
        assertTrue(ex.getMessage().contains("GenericRepository"), ex.getMessage());
        assertTrue(ex.getMessage().contains("EntityRepository"), ex.getMessage());
    }

    static class UserRepository extends EntityRepository<User> {
        UserRepository(EntityManager em) {
            super(em);
        }
    }

    static class InheritedUserRepository extends MiddleRepository<User> {
        InheritedUserRepository(EntityManager em) {
            super(em);
        }
    }

    static class MiddleRepository<T> extends EntityRepository<T> {
        MiddleRepository(EntityManager em) {
            super(em);
        }
    }

    static class GenericRepository<T> extends EntityRepository<T> {
        GenericRepository(EntityManager em) {
            super(em);
        }
    }
}
