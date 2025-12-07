package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.migration.*;
import de.t14d3.spool.test.entities.User;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the persistent migration system.
 * This test suite covers both the new persistent migrations and ensures
 * the original automatic migrations still work correctly.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PersistentMigrationTest {

    private static final Path MIGRATIONS_DIR = Paths.get("src/main/resources/migrations");
    private Connection connection;
    private EntityManager em;

    @BeforeEach
    void setUp() throws SQLException {
        // Use in-memory H2 database for testing - unique name per test to avoid state sharing
        connection = DriverManager.getConnection("jdbc:h2:mem:" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        em = EntityManager.create(connection);
        
        // Clean up migrations directory before each test
        cleanupMigrationsDirectory();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clean up migrations directory after each test
        cleanupMigrationsDirectory();
        
        // Drop all tables for clean state before closing
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP ALL OBJECTS");
            }
        }
        if (em != null) {
            em.close();
        }
    }

    private void cleanupMigrationsDirectory() {
        try {
            if (Files.exists(MIGRATIONS_DIR)) {
                Files.list(MIGRATIONS_DIR)
                    .filter(Files::isRegularFile)
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            System.err.println("Failed to delete migration file: " + path);
                        }
                    });
            }
        } catch (IOException e) {
            System.err.println("Failed to cleanup migrations directory: " + e.getMessage());
        }
    }

    @Test
    @Order(1)
    void testCreateMigrationFile() throws SQLException, IOException {
        System.out.println("=== Testing Persistent Migration File Creation ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create a migration file
        String migrationFilename = em.createMigration("Create users table");
        System.out.println("Created migration: " + migrationFilename);
        
        // Verify the migration file was created
        Path migrationPath = MIGRATIONS_DIR.resolve(migrationFilename);
        assertTrue(Files.exists(migrationPath), "Migration file should exist");
        System.out.println("✓ Migration file created successfully");
        
        // Load and inspect the migration file
        MigrationFile migration = em.loadMigrationFile(migrationFilename);
        System.out.println("Migration summary:\n" + migration.getSummary());
        
        assertEquals("Create users table", migration.getDescription());
        assertTrue(migration.getVersion().matches("\\d{14}")); // Should be timestamp format
        assertFalse(migration.getSqlStatements().isEmpty(), "Should have SQL statements");
        assertNotNull(migration.getChecksum(), "Should have checksum");
        System.out.println("✓ Migration file contents are valid");
    }

    @Test
    @Order(2)
    void testApplyPendingMigrations() throws SQLException, IOException {
        System.out.println("=== Testing Apply Pending Migrations ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create migration file
        String migrationFilename = em.createMigration("Create users table");
        System.out.println("Created migration: " + migrationFilename);
        
        // Verify no tables exist yet
        assertFalse(em.getMigrationManager().getIntrospector().tableExists("users"));
        System.out.println("✓ Users table does not exist before migration");
        
        // Apply pending migrations
        List<String> appliedMigrations = em.applyPendingMigrations();
        System.out.println("Applied migrations: " + appliedMigrations);
        
        assertEquals(1, appliedMigrations.size(), "Should apply one migration");
        assertTrue(appliedMigrations.contains(migrationFilename), "Should contain our migration");
        System.out.println("✓ Migration applied successfully");
        
        // Verify table now exists
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
        System.out.println("✓ Users table created successfully");
        
        // Verify migration is tracked as applied
        Set<String> applied = em.getPersistentMigrationManager().getAppliedMigrations();
        MigrationFile migration = em.loadMigrationFile(migrationFilename);
        assertTrue(applied.contains(migration.getVersion()), "Migration should be tracked as applied");
        System.out.println("✓ Migration properly tracked in database");
    }

    @Test
    @Order(3)
    void testMigrationStatus() throws SQLException, IOException {
        System.out.println("=== Testing Migration Status ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create migration file
        String migrationFilename = em.createMigration("Create users table");
        
        // Get status before applying
        String statusBefore = em.getMigrationStatus();
        System.out.println("Status before applying:\n" + statusBefore);
        assertTrue(statusBefore.contains("Pending migrations: 1"), "Should show 1 pending migration");
        assertTrue(statusBefore.contains(migrationFilename), "Should show the migration filename");
        
        // Apply migration
        em.applyPendingMigrations();
        
        // Get status after applying
        String statusAfter = em.getMigrationStatus();
        System.out.println("Status after applying:\n" + statusAfter);
        assertTrue(statusAfter.contains("Pending migrations: 0"), "Should show 0 pending migrations");
        assertTrue(statusAfter.contains("Applied migrations: 1"), "Should show 1 applied migration");
        System.out.println("✓ Migration status reporting works correctly");
    }

    @Test
    @Order(4)
    void testIdempotentMigrationApplication() throws SQLException, IOException {
        System.out.println("=== Testing Idempotent Migration Application ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create and apply migration
        String migrationFilename = em.createMigration("Create users table");
        List<String> applied1 = em.applyPendingMigrations();
        
        assertEquals(1, applied1.size(), "Should apply migration first time");
        System.out.println("✓ Migration applied first time");
        
        // Try to apply again - should not apply since already applied
        List<String> applied2 = em.applyPendingMigrations();
        assertEquals(0, applied2.size(), "Should not apply migration second time");
        System.out.println("✓ Migration correctly skipped on second application");
        
        // Verify table still exists and schema is correct
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
        System.out.println("✓ Table still exists and schema is valid");
    }

    @Test
    @Order(5)
    void testMultipleMigrations() throws SQLException, IOException {
        System.out.println("=== Testing Multiple Migrations ===");

        // Register multiple entities
        em.registerEntitiesForPersistentMigration(User.class, Author.class, Book.class);

        // Create multiple migrations
        String migration1 = em.createMigration("Create users and authors tables");
        String migration2 = em.createMigration("Create books table");

        List<String> allMigrations = em.getPersistentMigrationManager().getMigrationFiles();
        assertEquals(2, allMigrations.size(), "Should have 2 migration files");
        System.out.println("✓ Created 2 migration files: " + allMigrations);

        // Apply all pending migrations
        List<String> applied = em.applyPendingMigrations();
        assertEquals(2, applied.size(), "Should apply both migrations");
        System.out.println("✓ Applied both migrations");

        // Verify all tables exist
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("authors"));
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("books"));
        System.out.println("✓ All tables created successfully");
    }

    @Test
    @Order(6)
    void testMigrationFileContent() throws SQLException, IOException {
        System.out.println("=== Testing Migration File Content Format ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create migration
        String migrationFilename = em.createMigration("Create users table");
        
        // Read the file content directly
        Path migrationPath = MIGRATIONS_DIR.resolve(migrationFilename);
        String content = Files.readString(migrationPath);
        System.out.println("Migration file content:\n" + content);
        
        // Verify file format
        assertTrue(content.contains("-- Migration: " + migrationFilename), "Should have migration header");
        assertTrue(content.contains("-- Description: Create users table"), "Should have description");
        assertTrue(content.contains("-- Version: "), "Should have version");
        assertTrue(content.contains("-- Timestamp: "), "Should have timestamp");
        assertTrue(content.contains("-- Checksum: "), "Should have checksum");
        assertTrue(content.contains("CREATE TABLE"), "Should contain SQL statements");
        System.out.println("✓ Migration file format is correct");
    }

    @Test
    @Order(7)
    void testCompatibilityWithAutomaticMigrations() throws SQLException {
        System.out.println("=== Testing Compatibility with Automatic Migrations ===");
        
        // Test that original automatic migration functionality still works
        em.registerEntities(User.class);
        
        // Use automatic migration
        int changes = em.updateSchema();
        System.out.println("Applied " + changes + " automatic schema changes");
        
        // Verify table exists
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
        assertTrue(em.validateSchema(), "Schema should be valid");
        System.out.println("✓ Automatic migrations still work correctly");
        
        // Test that we can use both systems together
        em.registerEntitiesForPersistentMigration(Author.class);
        
        // Create persistent migration for new entity
        String migrationFilename = em.createMigration("Add authors table");
        assertNotNull(migrationFilename, "Should be able to create persistent migration");
        System.out.println("✓ Can use both migration systems together");
    }

    @Test
    @Order(8)
    void testMigrationWithTransaction() {
        System.out.println("=== Testing Persistent Migration with Transaction ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create migration within transaction
        em.beginTransaction();
        try {
            String migrationFilename = em.createMigration("Create users table in transaction");
            em.commit();
            
            // Apply migration
            List<String> applied = em.applyPendingMigrations();
            assertEquals(1, applied.size(), "Should apply transaction migration");
            
            // Verify table exists
            assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
            System.out.println("✓ Persistent migrations work within transactions");
            
        } catch (Exception e) {
            em.rollback();
            fail("Migration in transaction should not fail: " + e.getMessage());
        }
    }

    @Test
    @Order(9)
    void testNoChangesMigration() {
        System.out.println("=== Testing No Changes Migration ===");
        
        // Register entities for persistent migration
        em.registerEntitiesForPersistentMigration(User.class);
        
        // Create initial migration and apply it
        String migration1 = em.createMigration("Create users table");
        em.applyPendingMigrations();
        
        // Try to create another migration when no changes exist
        assertThrows(IllegalStateException.class, () -> {
            em.createMigration("No changes migration");
        }, "Should throw exception when no changes exist");
        
        System.out.println("✓ Correctly prevents creating migrations when no changes exist");
    }

    @Test
    @Order(10)
    void testApplySpecificMigration() throws SQLException {
        System.out.println("=== Testing Apply Specific Migration ===");
        System.out.println("Current tables in database: " + em.getMigrationManager().getIntrospector().getTableNames());
        
        // Register first entity
        em.registerEntitiesForPersistentMigration(User.class);
        String migration1 = em.createMigration("Create users table");

        // Wait a second to make sure the second migration gets a unique timestamp
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        // Register second entity
        em.registerEntitiesForPersistentMigration(Author.class);
        String migration2 = em.createMigration("Create authors table");

        // Apply only the first migration
        em.applyMigration(migration1);
        System.out.println("Current tables in database: " + em.getMigrationManager().getIntrospector().getTableNames());
        
        // Verify only users table exists
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
        assertFalse(em.getMigrationManager().getIntrospector().tableExists("authors"));
        
        // Apply the second migration
        em.applyMigration(migration2);
        
        // Verify both tables exist
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"));
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("authors"));
        
        System.out.println("✓ Can apply specific migrations individually");
    }
}
