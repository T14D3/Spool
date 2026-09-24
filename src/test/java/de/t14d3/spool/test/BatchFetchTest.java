package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that eager {@code @ManyToOne} references are loaded in batches
 * rather than one query per row.
 */
public class BatchFetchTest {
    private static final String URL = "jdbc:h2:mem:batch_fetch_test;DB_CLOSE_DELAY=-1";

    private EntityManager em;
    private QueryCountingConnection counting;

    @BeforeEach
    void setup() {
        EntityManager setup = EntityManager.create(URL);
        setup.getExecutor().execute("DROP TABLE IF EXISTS books", List.of());
        setup.getExecutor().execute("DROP TABLE IF EXISTS authors", List.of());
        setup.registerEntities(Author.class, Book.class);
        setup.updateSchema();

        for (int authorIndex = 0; authorIndex < 5; authorIndex++) {
            Author author = new Author("Author " + authorIndex, "author" + authorIndex + "@example.com");
            for (int bookIndex = 0; bookIndex < 4; bookIndex++) {
                author.addBook(new Book("Book " + authorIndex + "-" + bookIndex, "isbn-" + authorIndex + "-" + bookIndex));
            }
            setup.persist(author);
        }
        setup.flush();
        setup.close();

        try {
            Connection real = DriverManager.getConnection(URL);
            counting = new QueryCountingConnection(real);
            em = EntityManager.create(counting.getProxy());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void findAllBatchesEagerManyToOneTargets() {
        counting.reset();
        List<Book> books = em.findAll(Book.class);

        assertEquals(20, books.size());
        int queries = counting.getPreparedStatements();
        assertTrue(queries <= 3,
                "expected 1 query for books plus 1 batched query for authors, got " + queries);

        for (Book book : books) {
            assertNotNull(book.getAuthor());
            assertNotNull(book.getAuthor().getName());
        }
    }

    @Test
    void hqlQueriesBatchEagerManyToOneTargets() {
        em.clear();
        counting.reset();
        List<Book> books = em.createQuery("SELECT b FROM Book b WHERE b.title LIKE :prefix")
                .setParameter("prefix", "Book%")
                .getResultList();

        assertEquals(20, books.size());
        int queries = counting.getPreparedStatements();
        assertTrue(queries <= 3,
                "expected 1 query for books plus 1 batched query for authors, got " + queries);
    }

    @Test
    void batchedEntitiesAreManagedAndIdentityStable() {
        em.clear();
        List<Book> firstRead = em.findAll(Book.class);
        List<Book> secondRead = em.findAll(Book.class);

        assertSame(firstRead.get(0), secondRead.get(0));
        for (Book book : firstRead) {
            assertNotNull(book.getAuthor());
        }
    }

    private static void assertSame(Object expected, Object actual) {
        org.junit.jupiter.api.Assertions.assertSame(expected, actual);
    }

    /**
     * Counts calls to {@code prepareStatement} on the shared JDBC connection.
     */
    private static final class QueryCountingConnection implements InvocationHandler {
        private final Connection delegate;
        private final AtomicInteger preparedStatements = new AtomicInteger();
        private final Connection proxy;

        private QueryCountingConnection(Connection delegate) {
            this.delegate = delegate;
            this.proxy = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    this);
        }

        Connection getProxy() {
            return proxy;
        }

        void reset() {
            preparedStatements.set(0);
        }

        int getPreparedStatements() {
            return preparedStatements.get();
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("prepareStatement".equals(method.getName())) {
                preparedStatements.incrementAndGet();
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}
