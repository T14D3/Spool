package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T6.3 (first step): HQL projections, aggregates and GROUP BY.
 */
public class HqlProjectionTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        String url = "jdbc:h2:mem:hql_projection_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        em = EntityManager.create(url);
        em.registerEntities(User.class, Author.class, Book.class);
        em.updateSchema();

        em.persist(new User("ada", "ada@example.com"));
        em.persist(new User("bob", "bob@example.com"));
        em.persist(new User("cyd", "cyd@example.com"));

        Author first = new Author("sam", "sam@example.com");
        Book a = new Book("One", "isbn-1");
        Book b = new Book("Two", "isbn-2");
        first.addBook(a);
        first.addBook(b);
        Author second = new Author("tara", "tara@example.com");
        second.addBook(new Book("Three", "isbn-3"));
        em.persist(first);
        em.persist(second);
        em.flush();
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void singleColumnProjectionReturnsScalars() {
        List<String> names = em.createQuery("SELECT u.username FROM User u ORDER BY u.username")
                .getResultList();
        assertEquals(List.of("ada", "bob", "cyd"), names);
    }

    @Test
    void multipleColumnProjectionReturnsTuples() {
        List<Object[]> rows = em.createQuery("SELECT u.id, u.username FROM User u ORDER BY u.id")
                .getResultList();
        assertEquals(3, rows.size());
        assertEquals("ada", rows.get(0)[1]);
        assertNotNull(rows.get(0)[0]);
    }

    @Test
    void nestedPathProjectionTraversesManyToOne() {
        List<String> authorNames = em.createQuery("SELECT b.author.name FROM Book b ORDER BY b.title")
                .getResultList();
        assertEquals(List.of("sam", "tara", "sam"), authorNames);
    }

    @Test
    void countAggregatesReturnSingleLong() {
        Long total = em.createQuery("SELECT COUNT(u) FROM User u").getSingleResult();
        assertEquals(3L, total);

        Long star = em.createQuery("SELECT COUNT(*) FROM User u").getSingleResult();
        assertEquals(3L, star);
    }

    @Test
    void minMaxSumAvgAggregates() {
        assertEquals(1L, number(em.createQuery("SELECT MIN(u.id) FROM User u").getSingleResult()));
        assertEquals(3L, number(em.createQuery("SELECT MAX(u.id) FROM User u").getSingleResult()));
        assertEquals(6L, number(em.createQuery("SELECT SUM(u.id) FROM User u").getSingleResult()));
        assertEquals(2.0, ((Number) em.createQuery("SELECT AVG(u.id) FROM User u").getSingleResult()).doubleValue(), 0.001);
    }

    @Test
    void groupByWithAggregateReturnsTuples() {
        List<Object[]> rows = em.createQuery(
                        "SELECT b.author.id, COUNT(b) FROM Book b GROUP BY b.author.id ORDER BY b.author.id")
                .getResultList();
        assertEquals(2, rows.size());
        assertEquals(2L, number(rows.get(0)[1]));
        assertEquals(1L, number(rows.get(1)[1]));
    }

    @Test
    void entitySelectStillWorks() {
        List<User> users = em.createQuery("SELECT u FROM User u WHERE u.username = :name")
                .setParameter("name", "ada")
                .getResultList();
        assertEquals(1, users.size());
        assertEquals("ada", users.get(0).getUsername());
    }

    private static long number(Object value) {
        return ((Number) value).longValue();
    }
}
