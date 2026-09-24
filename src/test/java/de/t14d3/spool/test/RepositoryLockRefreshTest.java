package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.query.LockMode;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RepositoryLockRefreshTest {
    private EntityManager em;
    private EntityRepository<User> userRepo;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:repo_lock_test;DB_CLOSE_DELAY=-1");
        userRepo = new EntityRepository<>(em, User.class);
        em.getExecutor().execute("DROP TABLE IF EXISTS users", List.of());
        em.getExecutor().execute(
                "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(50), email VARCHAR(50))",
                List.of()
        );
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void refreshDiscardsInMemoryChanges() {
        User user = new User("refresh_user", "original@example.com");
        userRepo.save(user);
        em.flush();

        user.setEmail("changed@example.com");
        userRepo.refresh(user);

        assertEquals("original@example.com", user.getEmail());
        assertEquals("original@example.com", userRepo.findById(user.getId()).getEmail());
    }

    @Test
    void lockRequiresActiveTransaction() {
        User user = new User("lock_user", "lock@example.com");
        userRepo.save(user);
        em.flush();

        assertThrows(IllegalStateException.class,
                () -> userRepo.lock(user, LockMode.PESSIMISTIC_WRITE));
    }

    @Test
    void lockAcquiresRowLockInsideTransaction() {
        User user = new User("tx_user", "tx@example.com");
        userRepo.save(user);
        em.flush();

        em.transactional(() -> {
            User locked = userRepo.lock(user, LockMode.PESSIMISTIC_WRITE);
            assertSame(user, locked);
            locked.setEmail("tx-updated@example.com");
            userRepo.save(locked);
        });

        assertEquals("tx-updated@example.com", userRepo.findById(user.getId()).getEmail());
    }

    @Test
    void lockFailsForMissingRow() {
        User user = new User(9999L, "ghost", "ghost@example.com");

        // transactional() wraps the lock failure after rolling back.
        assertThrows(RuntimeException.class, () -> em.transactional(
                () -> userRepo.lock(user, LockMode.PESSIMISTIC_WRITE)));
    }

    @Test
    void sqliteHasNoLockingClause() {
        assertNull(Dialect.SQLITE.lockClause(LockMode.PESSIMISTIC_WRITE));
    }
}
