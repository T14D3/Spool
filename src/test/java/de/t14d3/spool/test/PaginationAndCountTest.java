package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.query.hql.HqlException;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaginationAndCountTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:pagination_test;DB_CLOSE_DELAY=-1");
        em.getExecutor().execute("DROP TABLE IF EXISTS users", List.of());
        em.getExecutor().execute(
                "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(50), email VARCHAR(50))",
                List.of());
        for (int i = 1; i <= 25; i++) {
            em.persist(new User(String.format("user%02d", i), "user" + i + "@example.com"));
        }
        em.flush();
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void limitAndOffsetReturnTheExpectedSlice() {
        List<User> page = em.createQuery("SELECT u FROM User u ORDER BY u.username")
                .setFirstResult(5)
                .setMaxResults(10)
                .getResultList();

        assertEquals(10, page.size());
        assertEquals("user06", page.get(0).getUsername());
        assertEquals("user15", page.get(9).getUsername());
    }

    @Test
    void offsetWithoutLimitSkipsRows() {
        List<User> tail = em.createQuery("SELECT u FROM User u ORDER BY u.username")
                .setFirstResult(20)
                .getResultList();

        assertEquals(5, tail.size());
        assertEquals("user21", tail.get(0).getUsername());
        assertEquals("user25", tail.get(4).getUsername());
    }

    @Test
    void offsetBeyondTheEndReturnsEmpty() {
        List<User> page = em.createQuery("SELECT u FROM User u ORDER BY u.username")
                .setFirstResult(100)
                .getResultList();
        assertTrue(page.isEmpty());
    }

    @Test
    void rawSelectBuilderSupportsOffset() {
        var query = em.createSelectQuery("*")
                .from("users")
                .orderBy("username")
                .limit("3")
                .offset("2")
                .build();

        assertTrue(query.getSql().toUpperCase().contains("OFFSET 2"), query.getSql());
    }

    @Test
    void countQueryWithParameter() {
        Long total = em.createQuery("SELECT COUNT(u) FROM User u WHERE u.username LIKE :pattern")
                .setParameter("pattern", "user0%")
                .getSingleResult();
        assertEquals(9L, total);

        Long all = em.createQuery("SELECT COUNT(*) FROM User u").getSingleResult();
        assertEquals(25L, all);
    }

    @Test
    void countQueryRejectsOrderByAndLimit() {
        assertThrows(HqlException.class,
                () -> em.createQuery("SELECT COUNT(u) FROM User u ORDER BY u.username"));
        assertThrows(HqlException.class,
                () -> em.createQuery("SELECT COUNT(u) FROM User u LIMIT 5"));
    }

    @Test
    void countAliasMustMatchFromAlias() {
        assertThrows(HqlException.class,
                () -> em.createQuery("SELECT COUNT(x) FROM User u"));
    }

    @Test
    void entityManagerCountDoesNotLoadEntities() {
        assertEquals(25L, em.count(User.class));
    }
}
