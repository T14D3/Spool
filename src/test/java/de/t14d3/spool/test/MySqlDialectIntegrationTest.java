package de.t14d3.spool.test;

import de.t14d3.spool.OptimisticLockException;
import de.t14d3.spool.UniqueConstraintViolationException;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.query.LockMode;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.Course;
import de.t14d3.spool.test.entities.OrderLine;
import de.t14d3.spool.test.entities.Student;
import de.t14d3.spool.test.entities.TypeMatrix;
import de.t14d3.spool.test.entities.UqSingleColumn;
import de.t14d3.spool.test.entities.User;
import de.t14d3.spool.test.entities.VersionedCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage of the MySQL/MariaDB dialect against a real server.
 * <p>
 * The test is skipped when the server cannot be reached. Override the
 * connection with {@code SPOOL_TEST_MYSQL_URL}, {@code SPOOL_TEST_MYSQL_USER}
 * and {@code SPOOL_TEST_MYSQL_PASSWORD}.
 */
public class MySqlDialectIntegrationTest {
    // Defaults target the MariaDB driver because MySQL Connector/J truncates
    // fractional seconds against MariaDB. Point the env var at a real MySQL
    // server (jdbc:mysql:...) to exercise Connector/J.
    private static final String URL = env("SPOOL_TEST_MYSQL_URL",
            "jdbc:mariadb://127.0.0.1:3306/spool_test");
    private static final String USER = env("SPOOL_TEST_MYSQL_USER", "spool");
    private static final String PASSWORD = env("SPOOL_TEST_MYSQL_PASSWORD", "spool");

    private static final List<String> TABLES = List.of(
            "students_courses", "books", "order_lines", "type_matrix",
            "versioned_counters", "uq_single_column", "courses", "students", "authors", "users");

    private EntityManager em;

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    @BeforeAll
    static void requireServer() {
        try (Connection ignored = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // reachable
        } catch (SQLException e) {
            Assumptions.assumeTrue(false, "MySQL/MariaDB not available: " + e.getMessage());
        }
    }

    @BeforeEach
    void setup() {
        em = EntityManager.create(URL, USER, PASSWORD);
        for (String table : TABLES) {
            em.getExecutor().execute("DROP TABLE IF EXISTS " + table, List.of());
        }
        em.registerEntities(User.class, Author.class, Book.class, Student.class, Course.class,
                OrderLine.class, VersionedCounter.class, TypeMatrix.class, UqSingleColumn.class);
        em.updateSchema();
    }

    @AfterEach
    void teardown() {
        if (em != null) {
            em.close();
        }
    }

    @Test
    void schemaUpdateIsIdempotent() {
        assertEquals(0, em.updateSchema(), "a second updateSchema must report no changes");
        assertTrue(em.validateSchema());
    }

    @Test
    void crudGeneratedKeysAndDirtyCheckingWork() {
        User user = new User("mysql_user", "mysql@example.com");
        em.persist(user);
        em.flush();
        assertNotNull(user.getId(), "MySQL generated key must be assigned");

        em.clear();
        User loaded = em.find(User.class, user.getId());
        assertNotNull(loaded);
        assertEquals("mysql_user", loaded.getUsername());

        loaded.setEmail("updated@example.com");
        em.flush();

        em.clear();
        assertEquals("updated@example.com", em.find(User.class, user.getId()).getEmail());
    }

    @Test
    void uniqueConstraintViolationIsTyped() {
        EntityRepository<UqSingleColumn> repo = new EntityRepository<>(em, UqSingleColumn.class);
        repo.save(new UqSingleColumn("mysql-duplicate@example.com"));
        em.flush();
        em.clear();

        em.persist(new UqSingleColumn("mysql-duplicate@example.com"));
        RuntimeException failure = assertThrows(RuntimeException.class, em::flush);
        assertTrue(causeChainContains(failure, UniqueConstraintViolationException.class),
                "expected a UniqueConstraintViolationException, got: " + failure);
    }

    @Test
    void lazyOneToManyLoads() {
        Author author = new Author("Mysql Author", "author@example.com");
        author.addBook(new Book("Book One", "mysql-isbn-1"));
        author.addBook(new Book("Book Two", "mysql-isbn-2"));
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
    void lazyManyToManyLoadsAndPersistsJoinRowDeltas() {
        Student student = new Student("Mysql Student");
        Course first = new Course("Databases");
        Course second = new Course("Compilers");
        student.getCourses().add(first);
        student.getCourses().add(second);
        em.persist(first);
        em.persist(second);
        em.persist(student);
        em.flush();
        assertEquals(2, em.getExecutor().count("students_courses"));

        em.clear();
        Student loaded = em.find(Student.class, student.getId());
        assertEquals(2, loaded.getCourses().size());
        for (Course course : loaded.getCourses()) {
            assertTrue(course.getStudents().contains(loaded));
        }

        loaded.getCourses().remove(loaded.getCourses().get(0));
        em.flush();
        assertEquals(1, em.getExecutor().count("students_courses"));
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
    void optimisticLockingWorks() {
        VersionedCounter counter = new VersionedCounter("first");
        em.persist(counter);
        em.flush();
        assertEquals(0L, counter.getVersion());

        counter.setName("second");
        em.flush();
        assertEquals(1L, counter.getVersion());

        EntityManager other = EntityManager.create(URL, USER, PASSWORD);
        try {
            em.clear();
            VersionedCounter winner = em.find(VersionedCounter.class, counter.getId());
            VersionedCounter loser = other.find(VersionedCounter.class, counter.getId());

            winner.setName("winner");
            em.flush();

            loser.setName("loser");
            RuntimeException failure = assertThrows(RuntimeException.class, other::flush);
            assertTrue(causeChainContains(failure, OptimisticLockException.class),
                    "expected an OptimisticLockException, got: " + failure);
        } finally {
            other.close();
        }
    }

    @Test
    void allSupportedTypesRoundTrip() {
        TypeMatrix entity = new TypeMatrix();
        entity.setTextValue("hello mysql");
        entity.setIntValue(42);
        entity.setLongValue(9_000_000_000L);
        entity.setDoubleValue(1.5d);
        entity.setFlag(true);
        entity.setAmount(new BigDecimal("1234567890.12345"));
        entity.setLocalDate(LocalDate.of(2026, 1, 2));
        entity.setLocalDateTime(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_000));
        entity.setInstantValue(Instant.parse("2026-01-02T03:04:05.123456Z"));
        entity.setOffsetDateTime(OffsetDateTime.parse("2026-01-02T03:04:05.123456Z"));
        entity.setUuidValue(UUID.fromString("11111111-2222-3333-4444-555555555555"));
        entity.setBlobValue(new byte[]{1, 2, 3, 4});
        entity.setStatus(TypeMatrix.Status.ARCHIVED);

        em.persist(entity);
        em.flush();

        em.clear();
        TypeMatrix loaded = em.find(TypeMatrix.class, entity.getId());
        assertNotNull(loaded);

        assertEquals("hello mysql", loaded.getTextValue());
        assertEquals(42, loaded.getIntValue());
        assertEquals(9_000_000_000L, loaded.getLongValue());
        assertEquals(1.5d, loaded.getDoubleValue());
        assertEquals(true, loaded.getFlag());
        assertEquals(0, new BigDecimal("1234567890.12345").compareTo(loaded.getAmount()));
        assertEquals(LocalDate.of(2026, 1, 2), loaded.getLocalDate());
        assertEquals(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_000), loaded.getLocalDateTime());
        assertEquals(Instant.parse("2026-01-02T03:04:05.123456Z"), loaded.getInstantValue());
        assertEquals(OffsetDateTime.parse("2026-01-02T03:04:05.123456Z"), loaded.getOffsetDateTime());
        assertEquals(UUID.fromString("11111111-2222-3333-4444-555555555555"), loaded.getUuidValue());
        assertArrayEquals(new byte[]{1, 2, 3, 4}, loaded.getBlobValue());
        assertEquals(TypeMatrix.Status.ARCHIVED, loaded.getStatus());
    }

    @Test
    void hqlPaginationAndCountWork() {
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

        // Offset without a limit (MySQL requires a LIMIT before OFFSET).
        List<User> tail = em.createQuery("SELECT u FROM User u ORDER BY u.username")
                .setFirstResult(10)
                .getResultList();
        assertEquals(2, tail.size());
        assertEquals("page11", tail.get(0).getUsername());

        Long total = em.createQuery("SELECT COUNT(u) FROM User u WHERE u.username LIKE :p")
                .setParameter("p", "page%")
                .getSingleResult();
        assertEquals(12L, total);
    }

    @Test
    void pessimisticLockWorks() {
        User user = new User("lock_user", "lock@example.com");
        em.persist(user);
        em.flush();

        em.transactional(() -> {
            em.lock(user, LockMode.PESSIMISTIC_WRITE);
            user.setEmail("locked@example.com");
        });

        em.clear();
        assertEquals("locked@example.com", em.find(User.class, user.getId()).getEmail());
    }

    private static boolean causeChainContains(Throwable failure, Class<? extends Throwable> type) {
        Throwable current = failure;
        int hops = 0;
        while (current != null && hops++ < 16) {
            if (type.isInstance(current)) {
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
