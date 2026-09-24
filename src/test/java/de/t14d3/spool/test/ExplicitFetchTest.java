package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.StubAuthor;
import de.t14d3.spool.test.entities.StubBook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T2.1 explicit fetch API and T3.1 stub contract: LAZY single refs never
 * self-load, {@code initialize}/{@code initializeAll}/`JOIN FETCH` resolve
 * them with bounded query counts, and lazy collections expose their load state.
 */
public class ExplicitFetchTest {
    private EntityManager em;
    private QueryCountingConnection counting;

    @BeforeEach
    void setup() throws Exception {
        String dbName = "explicit_fetch_" + UUID.randomUUID().toString().replace("-", "");
        String url = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";

        EntityManager seed = EntityManager.create(url);
        try {
            seed.registerEntities(StubAuthor.class, StubBook.class);
            seed.updateSchema();
            for (int authorIndex = 0; authorIndex < 20; authorIndex++) {
                StubAuthor author = new StubAuthor("Author " + authorIndex);
                for (int bookIndex = 0; bookIndex < 3; bookIndex++) {
                    StubBook book = new StubBook("Book " + authorIndex + "-" + bookIndex);
                    book.setAuthor(author);
                    author.getBooks().add(book);
                }
                seed.persist(author);
            }
            seed.flush();
        } finally {
            seed.close();
        }

        counting = new QueryCountingConnection(DriverManager.getConnection(url));
        em = EntityManager.create(counting.getProxy());
        em.registerEntities(StubAuthor.class, StubBook.class);
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void initializeAllBatchLoadsLazyCollections() {
        List<StubAuthor> authors = em.findAll(StubAuthor.class);
        assertEquals(20, authors.size());
        for (StubAuthor author : authors) {
            assertFalse(em.isLoaded(author.getBooks()), "collection must be lazy after load");
        }

        counting.reset();
        em.initializeAll(authors);

        for (StubAuthor author : authors) {
            assertTrue(em.isLoaded(author.getBooks()));
            assertEquals(3, author.getBooks().size());
        }
        assertTrue(counting.getPreparedStatements() <= 3,
                "initializeAll must batch: one pair query plus one IN load, got "
                        + counting.getPreparedStatements());
    }

    @Test
    void lazySingleRefStaysAStubUntilInitialized() {
        StubBook book = em.findAll(StubBook.class).get(0);
        assertNotNull(book.getAuthor(), "stub with id must exist");
        assertNull(book.getAuthor().getName(), "stub must not self-load on field access");

        counting.reset();
        em.initialize(book);
        assertNotNull(book.getAuthor().getName(), "initialize() must resolve the stub");
        assertEquals(1, counting.getPreparedStatements(), "one batched id load resolves the stub");
    }

    @Test
    void initializeAllBatchesLazySingleRefStubs() {
        List<StubBook> books = em.findAll(StubBook.class);
        counting.reset();
        em.initializeAll(books);
        for (StubBook book : books) {
            assertNotNull(book.getAuthor().getName());
        }
        assertTrue(counting.getPreparedStatements() <= 2,
                "initializeAll over stubs must use one IN query, got " + counting.getPreparedStatements());
    }

    @Test
    void hqlJoinFetchResolvesLazySingleRefs() {
        counting.reset();
        List<StubBook> books = em.createQuery("SELECT b FROM StubBook b JOIN FETCH b.author")
                .getResultList();
        assertEquals(60, books.size());
        for (StubBook book : books) {
            assertNotNull(book.getAuthor().getName());
        }
        assertTrue(counting.getPreparedStatements() <= 2,
                "JOIN FETCH on a single ref must add one batched query, got " + counting.getPreparedStatements());
    }

    @Test
    void hqlJoinFetchLoadsLazyCollections() {
        counting.reset();
        List<StubAuthor> authors = em.createQuery("SELECT a FROM StubAuthor a JOIN FETCH a.books")
                .getResultList();
        assertEquals(20, authors.size());
        for (StubAuthor author : authors) {
            assertEquals(3, author.getBooks().size());
        }
        assertTrue(counting.getPreparedStatements() <= 4,
                "JOIN FETCH on a collection must stay bounded, got " + counting.getPreparedStatements());
    }

    @Test
    void lazyWrapperIsLoadedIntrospectionAndEquals() {
        StubAuthor author = em.findAll(StubAuthor.class).get(0);
        assertFalse(em.isLoaded(author.getBooks()));
        author.getBooks().size();
        assertTrue(em.isLoaded(author.getBooks()));

        List<StubBook> copy = new ArrayList<>(author.getBooks());
        assertEquals(copy, author.getBooks(), "wrappers compare by contents like the List contract");
        assertTrue(em.isLoaded(new ArrayList<>()), "plain collections count as loaded");
        assertFalse(em.isLoaded(null));

        assertThrows(NotSerializableException.class, () -> serialize(author.getBooks()),
                "lazy wrappers are not Serializable; serializing cannot silently trigger a load");
    }

    private static byte[] serialize(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }
}
