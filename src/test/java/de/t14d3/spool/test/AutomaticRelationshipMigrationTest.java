package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.migration.SchemaDiff;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that automatic migration generation includes foreign key constraints
 * for relationship annotations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AutomaticRelationshipMigrationTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:auto_relation_test;DB_CLOSE_DELAY=-1");
    }

    @AfterEach
    void teardown() {
        if (em != null) {
            em.close();
        }
    }

    @Test
    void testAutomaticMigrationGeneratesForeignKeyConstraints() throws SQLException {
        System.out.println("=== Testing Automatic Foreign Key Constraint Generation ===");

        // Register entities for automatic migration
        em.registerEntities(Author.class, Book.class);

        // Get the schema diff to see what would be generated
        SchemaDiff diff = em.getMigrationManager().getSchemaDiff();
        
        System.out.println("Schema changes detected:");
        for (SchemaDiff.SchemaChange change : diff.getChanges()) {
            System.out.println("- " + change.getType() + ": " + change.getTableName());
            if (change.getTableDefinition() != null) {
                change.getTableDefinition().getColumns().values().forEach(col -> {
                    System.out.println("  Column: " + col.getName() + 
                        (col.isForeignKey() ? " (FK to " + col.getReferencedTable() + "." + col.getReferencedColumn() + ")" : ""));
                });
            }
        }

        // Apply the automatic migration
        int changesApplied = em.updateSchema();
        System.out.println("Applied " + changesApplied + " schema changes");

        // Verify tables exist
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("authors"), "Authors table should exist");
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("books"), "Books table should exist");
        System.out.println("✓ Both tables created successfully");

        // Get connection from EntityManager's executor
        Connection connection = em.getExecutor().getConnection();
        
        // Verify foreign key constraint exists in database metadata
        DatabaseMetaData metadata = connection.getMetaData();
        
        try (ResultSet rs = metadata.getImportedKeys(null, null, "BOOKS")) {
            boolean foundForeignKey = false;
            while (rs.next()) {
                String fkColumnName = rs.getString("FKCOLUMN_NAME");
                String pkTableName = rs.getString("PKTABLE_NAME");
                String pkColumnName = rs.getString("PKCOLUMN_NAME");
                
                System.out.println("Found foreign key: " + fkColumnName + " -> " + pkTableName + "." + pkColumnName);
                
                if ("AUTHOR_ID".equalsIgnoreCase(fkColumnName) && 
                    "AUTHORS".equalsIgnoreCase(pkTableName) && 
                    "ID".equalsIgnoreCase(pkColumnName)) {
                    foundForeignKey = true;
                }
            }
            
            assertTrue(foundForeignKey, "Foreign key constraint AUTHOR_ID -> AUTHORS(ID) should exist");
            System.out.println("✓ Foreign key constraint properly created in database");
        }

        // Test that relationship persistence works with the auto-generated schema
        Author author = new Author("Test Author", "test@example.com");
        Book book = new Book("Test Book", "TEST-123");
        author.addBook(book);
        
        em.persist(author);
        em.flush();

        assertNotNull(author.getId(), "Author should have an ID");
        assertNotNull(book.getId(), "Book should have an ID");
        assertEquals(author.getId(), book.getAuthor().getId(), "Book should reference the correct author");
        
        System.out.println("✓ Relationship persistence works with auto-generated schema");
        System.out.println("✓ Automatic foreign key constraint generation is working correctly!");
    }

    @Test
    void testMigrationSqlGeneration() throws SQLException {
        System.out.println("=== Testing Migration SQL Generation ===");

        // Register entities
        em.registerEntities(Author.class, Book.class);

        // Get the SQL that would be generated
        List<String> sqlStatements = em.getMigrationManager().generateMigrationSql();
        
        System.out.println("Generated SQL statements:");
        for (String sql : sqlStatements) {
            System.out.println(sql);
        }

        // Should have at least one CREATE TABLE statement for books with foreign key
        boolean foundBooksCreateWithFk = sqlStatements.stream()
            .anyMatch(sql -> sql.contains("CREATE TABLE") && 
                             (sql.contains("books") || sql.contains("BOOKS")) && 
                             sql.contains("FOREIGN KEY") &&
                             sql.contains("author_id"));

        assertTrue(foundBooksCreateWithFk, "Should generate CREATE TABLE for books with foreign key constraint");
        System.out.println("✓ Migration SQL includes foreign key constraints");

    }
}
