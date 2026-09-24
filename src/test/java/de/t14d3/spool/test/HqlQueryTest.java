package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.query.hql.HqlException;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HqlQueryTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:hql_test;DB_CLOSE_DELAY=-1");
        em.getExecutor().execute("DROP TABLE IF EXISTS users", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS books", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS authors", List.of());
        em.getExecutor().execute(
                "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(50), email VARCHAR(50))",
                List.of());
        em.getExecutor().execute(
                "CREATE TABLE authors (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), email VARCHAR(50))",
                List.of());
        em.getExecutor().execute(
                "CREATE TABLE books (id BIGINT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(50), isbn VARCHAR(50), author_id BIGINT)",
                List.of());
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void selectWithNamedParameter() {
        em.persist(new User("alice", "alice@example.com"));
        em.persist(new User("bob", "bob@example.com"));
        em.flush();

        List<User> users = em.createQuery("SELECT u FROM User u WHERE u.username = :name")
                .setParameter("name", "alice")
                .getResultList();

        assertEquals(1, users.size());
        assertEquals("alice", users.get(0).getUsername());
        assertEquals("alice@example.com", users.get(0).getEmail());
    }

    @Test
    void selectWithPositionalParameter() {
        em.persist(new User("carol", "carol@example.com"));
        em.flush();

        User user = em.createQuery("SELECT u FROM User u WHERE u.email = ?1")
                .setParameter(1, "carol@example.com")
                .getSingleResult();

        assertEquals("carol", user.getUsername());
    }

    @Test
    void orderByAndLimit() {
        em.persist(new User("dave", "d@example.com"));
        em.persist(new User("erin", "e@example.com"));
        em.persist(new User("frank", "f@example.com"));
        em.flush();

        List<User> users = em.createQuery(
                        "SELECT u FROM User u ORDER BY u.username DESC LIMIT 2")
                .getResultList();

        assertEquals(2, users.size());
        assertEquals("frank", users.get(0).getUsername());
        assertEquals("erin", users.get(1).getUsername());
    }

    @Test
    void setMaxResultsOverridesLimit() {
        em.persist(new User("gina", "g@example.com"));
        em.persist(new User("hank", "h@example.com"));
        em.persist(new User("iris", "i@example.com"));
        em.flush();

        List<User> users = em.createQuery("SELECT u FROM User u ORDER BY u.username")
                .setMaxResults(1)
                .getResultList();

        assertEquals(1, users.size());
        assertEquals("gina", users.get(0).getUsername());
    }

    @Test
    void inWithCollectionParameter() {
        em.persist(new User("jack", "j@example.com"));
        em.persist(new User("kate", "k@example.com"));
        em.persist(new User("leo", "l@example.com"));
        em.flush();

        List<User> users = em.createQuery("SELECT u FROM User u WHERE u.username IN (:names) ORDER BY u.username")
                .setParameter("names", List.of("jack", "leo"))
                .getResultList();

        assertEquals(2, users.size());
        assertEquals("jack", users.get(0).getUsername());
        assertEquals("leo", users.get(1).getUsername());
    }

    @Test
    void inWithEmptyCollectionMatchesNothing() {
        em.persist(new User("mia", "m@example.com"));
        em.flush();

        List<User> users = em.createQuery("SELECT u FROM User u WHERE u.username IN (:names)")
                .setParameter("names", List.of())
                .getResultList();

        assertTrue(users.isEmpty());
    }

    @Test
    void likeAndNullPredicates() {
        User withEmail = new User("nina", "nina@example.com");
        User withoutEmail = new User("omar", null);
        em.persist(withEmail);
        em.persist(withoutEmail);
        em.flush();

        List<User> like = em.createQuery("SELECT u FROM User u WHERE u.email LIKE :pattern")
                .setParameter("pattern", "nina%")
                .getResultList();
        assertEquals(1, like.size());
        assertEquals("nina", like.get(0).getUsername());

        List<User> missing = em.createQuery("SELECT u FROM User u WHERE u.email IS NULL")
                .getResultList();
        assertEquals(1, missing.size());
        assertEquals("omar", missing.get(0).getUsername());

        List<User> present = em.createQuery("SELECT u FROM User u WHERE u.email IS NOT NULL")
                .getResultList();
        assertEquals(1, present.size());
        assertEquals("nina", present.get(0).getUsername());
    }

    @Test
    void andOrPrecedence() {
        em.persist(new User("pat", "p@example.com"));
        em.persist(new User("quinn", "q@example.com"));
        em.persist(new User("rita", "r@example.com"));
        em.flush();

        List<User> users = em.createQuery(
                        "SELECT u FROM User u WHERE (u.username = :a OR u.username = :b) AND u.email = :email")
                .setParameter("a", "pat")
                .setParameter("b", "quinn")
                .setParameter("email", "q@example.com")
                .getResultList();

        assertEquals(1, users.size());
        assertEquals("quinn", users.get(0).getUsername());
    }

    @Test
    void implicitJoinThroughManyToOne() {
        Author author = new Author("sam", "sam@example.com");
        Book book = new Book("Spool Internals", "isbn-1");
        book.setAuthor(author);
        em.persist(book);
        em.flush();

        List<Book> books = em.createQuery("SELECT b FROM Book b WHERE b.author.name = :author")
                .setParameter("author", "sam")
                .getResultList();

        assertEquals(1, books.size());
        assertEquals("Spool Internals", books.get(0).getTitle());
    }

    @Test
    void selectStarWithoutAliasInProjection() {
        em.persist(new User("tara", "t@example.com"));
        em.flush();

        List<User> users = em.createQuery("SELECT * FROM User u WHERE u.username = :name")
                .setParameter("name", "tara")
                .getResultList();

        assertEquals(1, users.size());
    }

    @Test
    void queryResultsAreManaged() {
        User user = new User("ulric", "u@example.com");
        em.persist(user);
        em.flush();

        User byQuery = em.createQuery("SELECT u FROM User u WHERE u.username = :name")
                .setParameter("name", "ulric")
                .getSingleResult();

        assertNotNull(em.find(User.class, user.getId()));
        assertEquals("ulric", byQuery.getUsername());
    }

    @Test
    void missingParameterFails() {
        em.persist(new User("vera", "v@example.com"));
        em.flush();

        var query = em.createQuery("SELECT u FROM User u WHERE u.username = :name");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, query::getResultList);
        assertTrue(failure.getMessage().contains("name"));
    }

    @Test
    void parseErrors() {
        assertThrows(HqlException.class, () -> em.createQuery("SELECT u FROM Missing u"));
        assertThrows(HqlException.class, () -> em.createQuery("SELECT u.name FROM User u"));
        assertThrows(HqlException.class, () -> em.createQuery("SELECT u FROM User u WHERE u.nope = 1"));
        assertThrows(HqlException.class, () -> em.createQuery("SELECT a FROM User u"));
    }

    @Test
    void singleResultRejectsEmptyAndMultiple() {
        assertThrows(IllegalStateException.class,
                () -> em.createQuery("SELECT u FROM User u").getSingleResult());

        em.persist(new User("wade", "w@example.com"));
        em.persist(new User("xena", "x@example.com"));
        em.flush();

        assertThrows(IllegalStateException.class,
                () -> em.createQuery("SELECT u FROM User u").getSingleResult());
    }
}
