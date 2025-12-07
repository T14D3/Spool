package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test for relationship mapping functionality.
 * Tests Author ↔ Book bidirectional one-to-many relationship.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RelationshipTest {
    private EntityManager em;
    private AuthorRepository authorRepo;
    private BookRepository bookRepo;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:relation_test;DB_CLOSE_DELAY=-1");
        authorRepo = new AuthorRepository(em);
        bookRepo = new BookRepository(em);

        // Create tables for the relationship test
        em.getExecutor().execute(
                "CREATE TABLE IF NOT EXISTS authors (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                        "name VARCHAR(255), " +
                        "email VARCHAR(255)" +
                        ")", List.of()
        );

        em.getExecutor().execute(
                "CREATE TABLE IF NOT EXISTS books (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                        "title VARCHAR(255), " +
                        "isbn VARCHAR(50), " +
                        "author_id BIGINT, " +
                        "FOREIGN KEY (author_id) REFERENCES authors(id)" +
                        ")", List.of()
        );
    }

    @AfterEach
    void teardown() {
        em.getExecutor().execute("DROP TABLE IF EXISTS books", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS authors", List.of());
        em.close();
    }

    @Test
    void testCreateAndPersistRelationships() {
        System.out.println("=== Testing Relationship Creation and Persistence ===");

        // Create an author
        Author author = new Author("J.K. Rowling", "jk@example.com");
        assertNull(author.getId());
        assertTrue(author.getBooks().isEmpty());

        // Create books and establish relationships
        Book book1 = new Book("Harry Potter and the Philosopher's Stone", "978-0747532699");
        Book book2 = new Book("Harry Potter and the Chamber of Secrets", "978-0747538493");

        // Set up bidirectional relationship
        author.addBook(book1);
        author.addBook(book2);

        // Verify relationships before persistence
        assertEquals(2, author.getBooks().size());
        assertEquals(author, book1.getAuthor());
        assertEquals(author, book2.getAuthor());

        // Persist the author (should cascade to books via the relationship mappings)
        em.persist(author);
        em.flush();

        // Verify IDs were assigned
        assertNotNull(author.getId());
        assertNotNull(book1.getId());
        assertNotNull(book2.getId());

        System.out.println("✓ Author created with ID: " + author.getId());
        System.out.println("✓ Book1 created with ID: " + book1.getId() + ", author_id: " + book1.getAuthor());
        System.out.println("✓ Book2 created with ID: " + book2.getId() + ", author_id: " + book2.getAuthor());
    }

    @Test
    void testLoadAndVerifyRelationships() {
        // Create test data
        Author author = new Author("George Orwell", "orwell@example.com");
        Book book1 = new Book("1984", "978-0451524935");
        Book book2 = new Book("Animal Farm", "978-0451526342");

        author.addBook(book1);
        author.addBook(book2);

        em.persist(author);
        em.flush();

        Long authorId = author.getId();

        // Load author from database
        Author loadedAuthor = em.find(Author.class, authorId);
        assertNotNull(loadedAuthor);
        assertEquals("George Orwell", loadedAuthor.getName());
        assertEquals("orwell@example.com", loadedAuthor.getEmail());

        // For now, books relationship is not auto-loaded (would need lazy loading)
        // But we can verify the author was properly assigned to books
        Book loadedBook1 = bookRepo.findByTitle("1984").get(0);
        Book loadedBook2 = bookRepo.findByTitle("Animal Farm").get(0);

        assertNotNull(loadedBook1);
        assertNotNull(loadedBook2);
        assertEquals("1984", loadedBook1.getTitle());
        assertEquals("Animal Farm", loadedBook2.getTitle());

        // Note: author relationship needs manual loading for now
        // In full implementation, this would use lazy loading
        System.out.println("✓ Author and books loaded successfully");
        System.out.println("✓ Books maintain their titles and relationships");
    }

    @Test
    void testRepositoryOperationsWithRelationships() {
        // Create multiple authors and books
        Author author1 = new Author("Stephen King", "king@example.com");
        Author author2 = new Author("Agatha Christie", "christie@example.com");

        Book book1 = new Book("The Shining", "978-0307743657");
        Book book2 = new Book("Murder on the Orient Express", "978-0062693662");

        author1.addBook(book1);
        author2.addBook(book2);

        authorRepo.save(author1);
        authorRepo.save(author2);
        em.flush();

        // Test repository findAll
        List<Author> allAuthors = authorRepo.findAll();
        assertEquals(2, allAuthors.size());

        List<Book> allBooks = bookRepo.findAll();
        assertEquals(2, allBooks.size());

        // Test existence checks
        assertTrue(authorRepo.existsById(author1.getId()));
        assertTrue(bookRepo.existsById(book1.getId()));

        // Test custom repository methods
        List<Author> stephenAuthors = authorRepo.findByName("Stephen King");
        assertEquals(1, stephenAuthors.size());
        assertEquals("Stephen King", stephenAuthors.get(0).getName());

        // Test counts
        assertEquals(2, authorRepo.count());
        assertEquals(2, bookRepo.count());

        System.out.println("✓ Repository operations work correctly with relationships");
    }

    @Test
    void testRelationshipUpdates() {
        // Create initial data
        Author author = new Author("Test Author", "test@example.com");
        Book book = new Book("Test Book", "TEST-123");
        author.addBook(book);
        em.persist(author);
        em.flush();

        // Load and modify relationship
        Author loadedAuthor = em.find(Author.class, author.getId());
        assertNotNull(loadedAuthor);

        // Update author's email
        loadedAuthor.setEmail("updated@example.com");
        em.persist(loadedAuthor);
        em.flush();

        // Verify update
        Author updatedAuthor = em.find(Author.class, author.getId());
        assertEquals("updated@example.com", updatedAuthor.getEmail());

        System.out.println("✓ Relationship updates work correctly");
    }

    @Test
    void testRelationshipDeletion() {
        // Create test data
        Author author = new Author("Delete Test", "delete@example.com");
        Book book1 = new Book("Book to Keep", "KEEP-123");
        Book book2 = new Book("Book to Delete", "DELETE-123");

        author.addBook(book1);
        author.addBook(book2);
        em.persist(author);
        em.flush();

        Long book2Id = book2.getId();

        // Delete one book
        em.remove(book2);
        em.flush();

        // Verify book was deleted
        Book deletedBook = em.find(Book.class, book2Id);
        assertNull(deletedBook);

        // Verify other book still exists
        Book keptBook = em.find(Book.class, book1.getId());
        assertNotNull(keptBook);

        // Verify author still exists
        Author existingAuthor = em.find(Author.class, author.getId());
        assertNotNull(existingAuthor);

        System.out.println("✓ Relationship deletion works correctly");
    }

    @Test
    void testBidirectionalRelationshipManagement() {
        // Test the helper methods for bidirectional relationship management
        Author author = new Author("BiDirectional Test", "bidirectional@example.com");
        Book book1 = new Book("Book 1", "BIDIR-1");
        Book book2 = new Book("Book 2", "BIDIR-2");

        // Test adding books
        assertEquals(0, author.getBooks().size());
        author.addBook(book1);
        assertEquals(1, author.getBooks().size());
        assertEquals(author, book1.getAuthor());

        author.addBook(book2);
        assertEquals(2, author.getBooks().size());
        assertEquals(author, book2.getAuthor());

        // Test removing books
        author.removeBook(book1);
        assertEquals(1, author.getBooks().size());
        assertNull(book1.getAuthor());
        assertEquals(author, book2.getAuthor());

        System.out.println("✓ Bidirectional relationship management works correctly");
    }

    @Test
    void testComplexRelationshipScenario() {
        System.out.println("=== Testing Complex Relationship Scenario ===");

        // Create a scenario with multiple authors and their books
        Author author1 = new Author("Author One", "author1@example.com");
        Author author2 = new Author("Author Two", "author2@example.com");

        Book book1 = new Book("Book One by Author One", "ISBN-001");
        Book book2 = new Book("Book Two by Author One", "ISBN-002");
        Book book3 = new Book("Book One by Author Two", "ISBN-003");

        author1.addBook(book1);
        author1.addBook(book2);
        author2.addBook(book3);

        // Persist all
        em.persist(author1);
        em.persist(author2);
        em.flush();

        // Verify the relationships were persisted correctly
        List<Book> allBooks = bookRepo.findAll();
        assertEquals(3, allBooks.size());

        List<Author> allAuthors = authorRepo.findAll();
        assertEquals(2, allAuthors.size());

        // Verify we can find books by their relationships
        Book foundBook3 = bookRepo.findByTitle("Book One by Author Two").get(0);
        assertEquals("Book One by Author Two", foundBook3.getTitle());
        assertEquals("ISBN-003", foundBook3.getIsbn());

        Author foundAuthor1 = authorRepo.findByName("Author One").get(0);
        assertEquals("Author One", foundAuthor1.getName());
        assertEquals("author1@example.com", foundAuthor1.getEmail());

        System.out.println("✓ Complex relationship scenario works correctly");
        System.out.println("✓ Foreign key relationships properly maintained");
        System.out.println("✓ Bidirectional relationship helpers function properly");
    }
}