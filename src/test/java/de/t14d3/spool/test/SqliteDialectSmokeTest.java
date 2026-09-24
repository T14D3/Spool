package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.OrderLine;
import de.t14d3.spool.test.entities.User;
import de.t14d3.spool.test.entities.VersionedCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end smoke coverage of the SQLite dialect against the real xerial
 * driver: schema generation, generated keys, CRUD, dirty-checking flush,
 * HQL, lazy relationships and composite primary keys.
 */
public class SqliteDialectSmokeTest {
    private Path databaseFile;
    private EntityManager em;

    @BeforeEach
    void setup() throws Exception {
        databaseFile = Files.createTempFile("spool-sqlite-", ".db");
        Files.deleteIfExists(databaseFile);
        em = EntityManager.create("jdbc:sqlite:" + databaseFile.toAbsolutePath());
        em.registerEntities(User.class, Author.class, Book.class, OrderLine.class, VersionedCounter.class);
        em.updateSchema();
    }

    @AfterEach
    void teardown() throws Exception {
        if (em != null) {
            em.close();
        }
        if (databaseFile != null) {
            Files.deleteIfExists(databaseFile);
        }
    }

    @Test
    void crudAndAutoDirtyCheckingWork() {
        User user = new User("sqlite_user", "sqlite@example.com");
        em.persist(user);
        em.flush();
        assertNotNull(user.getId(), "SQLite generated key must be assigned");

        em.clear();
        User loaded = em.find(User.class, user.getId());
        assertNotNull(loaded);
        assertEquals("sqlite_user", loaded.getUsername());

        // Dirty checking without persist().
        loaded.setEmail("updated@example.com");
        em.flush();

        em.clear();
        assertEquals("updated@example.com", em.find(User.class, user.getId()).getEmail());
    }

    @Test
    void repositoryCrudWorks() {
        EntityRepository<User> repo = new EntityRepository<>(em, User.class);
        repo.save(new User("repo_user", "repo@example.com"));
        em.flush();

        assertEquals(1, repo.count());
        assertNotNull(repo.findOneBy("username", "repo_user"));

        repo.deleteById(repo.findOneBy("username", "repo_user").getId());
        em.flush();
        assertEquals(0, repo.count());
    }

    @Test
    void lazyOneToManyLoads() {
        Author author = new Author("Octavia", "octavia@example.com");
        author.addBook(new Book("Kindred", "sqlite-isbn-1"));
        author.addBook(new Book("Parable of the Sower", "sqlite-isbn-2"));
        em.persist(author);
        em.flush();

        em.clear();
        Author loaded = em.find(Author.class, author.getId());
        assertNotNull(loaded);
        assertEquals(2, loaded.getBooks().size());
        for (Book book : loaded.getBooks()) {
            assertNotNull(book.getAuthor());
        }
    }

    @Test
    void hqlSelectWorks() {
        em.persist(new User("hql_sqlite", "hql@example.com"));
        em.flush();

        List<User> users = em.createQuery("SELECT u FROM User u WHERE u.username = :name")
                .setParameter("name", "hql_sqlite")
                .getResultList();
        assertEquals(1, users.size());
        assertEquals("hql@example.com", users.get(0).getEmail());
    }

    @Test
    void paginationAndCountWork() {
        for (int i = 1; i <= 12; i++) {
            em.persist(new User(String.format("page%02d", i), "page" + i + "@example.com"));
        }
        em.flush();

        List<User> page = em.createQuery("SELECT u FROM User u ORDER BY u.username")
                .setFirstResult(3)
                .setMaxResults(4)
                .getResultList();
        assertEquals(4, page.size());
        assertEquals("page04", page.get(0).getUsername());
        assertEquals("page07", page.get(3).getUsername());

        Long total = em.createQuery("SELECT COUNT(u) FROM User u WHERE u.username LIKE :p")
                .setParameter("p", "page%")
                .getSingleResult();
        assertEquals(12L, total);
    }

    @Test
    void optimisticLockingWorks() {
        VersionedCounter counter = new VersionedCounter("sqlite");
        em.persist(counter);
        em.flush();
        assertEquals(0L, counter.getVersion());

        counter.setName("updated");
        em.flush();
        assertEquals(1L, counter.getVersion());

        em.clear();
        assertEquals(1L, em.find(VersionedCounter.class, counter.getId()).getVersion());
    }

    @Test
    void compositePrimaryKeyWorks() {
        EntityRepository<OrderLine> repo = new EntityRepository<>(em, OrderLine.class);
        repo.save(new OrderLine(1L, 1, "SKU-A", 2));
        repo.save(new OrderLine(1L, 2, "SKU-B", 3));
        em.flush();

        assertEquals(2, repo.count());
        OrderLine loaded = em.find(OrderLine.class, Map.of("orderId", 1L, "lineNo", 2));
        assertNotNull(loaded);
        assertEquals("SKU-B", loaded.getSku());

        repo.deleteById(Map.of("orderId", 1L, "lineNo", 1));
        em.flush();
        assertNull(em.find(OrderLine.class, Map.of("orderId", 1L, "lineNo", 1)));
    }

    @Test
    void schemaUpdateIsIdempotent() {
        assertEquals(0, em.updateSchema(), "a second updateSchema must report no changes");
    }

    @Test
    void migrationToNewColumnIsIdempotent() {
        // The schema generated above must validate cleanly.
        assertTrue(em.validateSchema());
    }
}
