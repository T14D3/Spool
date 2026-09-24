package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T1.4: pins detach/cancelPending semantics.
 * <ul>
 *   <li>{@code detach} removes the entity from the identity map and skips
 *       queued inserts/updates; queued deletes are not cancelled.</li>
 *   <li>{@code cancelPending} drops queued inserts/updates by identity and
 *       evicts the identity-map entry; queued deletes are not cancelled.</li>
 * </ul>
 */
public class DetachSemanticsTest {
    private EntityManager open() {
        String url = "jdbc:h2:mem:detach_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        EntityManager em = EntityManager.create(url);
        em.registerEntities(Author.class, Book.class);
        em.updateSchema();
        return em;
    }

    @Test
    void detachSkipsQueuedInsert() {
        EntityManager em = open();
        try {
            Author author = new Author("skipped", "skipped@example.com");
            em.persist(author);
            em.detach(author);
            em.flush();

            assertEquals(0, em.getExecutor().count("authors"), "detached insert must not run");
            assertFalse(em.isManaged(Author.class, 1L));
        } finally {
            em.close();
        }
    }

    @Test
    void detachDropsManagedStateAndLaterFlushDoesNotUpdate() {
        EntityManager em = open();
        try {
            Author author = new Author("ada", "ada@example.com");
            em.persist(author);
            em.flush();
            Long id = author.getId();

            Author managed = em.find(Author.class, id);
            em.detach(managed);
            assertFalse(em.isManaged(Author.class, id), "detach must evict the identity-map entry");

            managed.setName("changed");
            em.flush();

            Author reloaded = em.find(Author.class, id);
            assertEquals("ada", reloaded.getName(), "detached changes must not be written");
            assertNotSame(managed, reloaded);
        } finally {
            em.close();
        }
    }

    @Test
    void detachDoesNotCancelQueuedDelete() {
        EntityManager em = open();
        try {
            Author author = new Author("gone", "gone@example.com");
            em.persist(author);
            em.flush();
            Long id = author.getId();

            Author managed = em.find(Author.class, id);
            em.remove(managed);
            em.detach(managed);
            em.flush();

            assertEquals(0, em.getExecutor().count("authors"), "queued delete must survive detach");
        } finally {
            em.close();
        }
    }

    @Test
    void detachCascadesToLoadedRelatedEntities() {
        EntityManager em = open();
        try {
            Author author = new Author("ada", "ada@example.com");
            Book book = new Book("b", "isbn");
            author.addBook(book);
            em.persist(author);
            em.flush();
            Long bookId = book.getId();

            Author managed = em.find(Author.class, author.getId());
            managed.getBooks().size(); // materialize so cascade detach sees the child
            Book managedBook = em.find(Book.class, bookId);
            assertTrue(em.isManaged(Book.class, bookId));

            em.detach(managed);
            assertFalse(em.isManaged(Author.class, author.getId()));
            assertFalse(em.isManaged(Book.class, bookId), "cascade=ALL includes DETACH");
            assertNotNull(managedBook);
        } finally {
            em.close();
        }
    }

    @Test
    void cancelPendingDropsQueuedInsert() {
        EntityManager em = open();
        try {
            Author author = new Author("dropped", "dropped@example.com");
            em.persist(author);
            em.cancelPending(author);
            em.flush();

            assertEquals(0, em.getExecutor().count("authors"), "cancelPending must drop the queued insert");
        } finally {
            em.close();
        }
    }

    @Test
    void cancelPendingDoesNotCancelQueuedDelete() {
        EntityManager em = open();
        try {
            Author author = new Author("gone", "gone@example.com");
            em.persist(author);
            em.flush();
            Long id = author.getId();

            Author managed = em.find(Author.class, id);
            em.remove(managed);
            em.cancelPending(managed);
            em.flush();

            assertEquals(0, em.getExecutor().count("authors"), "queued delete must survive cancelPending");
        } finally {
            em.close();
        }
    }
}
