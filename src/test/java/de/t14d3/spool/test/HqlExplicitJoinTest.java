package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.query.hql.HqlException;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * T6.3 (second step): explicit {@code LEFT JOIN}/{@code INNER JOIN} with an
 * alias, usable in projections, WHERE, ORDER BY and GROUP BY.
 */
public class HqlExplicitJoinTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        String url = "jdbc:h2:mem:hql_join_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        em = EntityManager.create(url);
        em.registerEntities(Author.class, Book.class);
        em.updateSchema();

        Author ada = new Author("ada", "ada@example.com");
        Book one = new Book("One", "isbn-1");
        ada.addBook(one);
        Author bob = new Author("bob", "bob@example.com");
        Book two = new Book("Two", "isbn-2");
        bob.addBook(two);
        Book orphan = new Book("Orphan", "isbn-3");
        em.persist(ada);
        em.persist(bob);
        em.persist(orphan);
        em.flush();
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void leftJoinKeepsRowsWithoutReference() {
        List<String> orphanTitles = em.createQuery(
                        "SELECT b.title FROM Book b LEFT JOIN b.author a WHERE a.name IS NULL")
                .getResultList();
        assertEquals(List.of("Orphan"), orphanTitles);
    }

    @Test
    void innerJoinDropsRowsWithoutReference() {
        List<String> titles = em.createQuery(
                        "SELECT b.title FROM Book b JOIN b.author a ORDER BY b.title")
                .getResultList();
        assertEquals(List.of("One", "Two"), titles);
    }

    @Test
    void joinedAliasWorksInTuplesAndOrdering() {
        List<Object[]> rows = em.createQuery(
                        "SELECT b.title, a.name FROM Book b LEFT JOIN b.author a ORDER BY b.title")
                .getResultList();
        assertEquals(3, rows.size());
        assertEquals("One", rows.get(0)[0]);
        assertEquals("ada", rows.get(0)[1]);
        assertEquals("Orphan", rows.get(1)[0]);
        assertNull(rows.get(1)[1]);
        assertEquals("bob", rows.get(2)[1]);
    }

    @Test
    void unknownAliasAndDuplicateAliasAreRejected() {
        assertThrows(HqlException.class, () ->
                em.createQuery("SELECT x.title FROM Book b LEFT JOIN b.author a").getResultList());
        assertThrows(HqlException.class, () ->
                em.createQuery("SELECT b.title FROM Book b JOIN b.author b").getResultList());
    }
}
