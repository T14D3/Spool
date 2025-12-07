package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.migration.*;
import de.t14d3.spool.test.entities.User;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the automatic migration system.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MigrationTest {

    private Connection connection;
    private EntityManager em;

    @BeforeEach
    void setUp() throws SQLException {
        // Use in-memory H2 database for testing - unique name per test to avoid state sharing
        connection = DriverManager.getConnection("jdbc:h2:mem:" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        em = EntityManager.create(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
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

    @Test
    @Order(1)
    void testCreateTableMigration() throws SQLException {
        System.out.println("=== Testing CREATE TABLE Migration ===");
        
        // Register entity
        em.registerEntities(User.class);
        
        // Debug: Check what tables exist before migration
        MigrationManager mm = em.getMigrationManager();
        SchemaIntrospector introspector = mm.getIntrospector();
        
        // Debug: Print all tables found
        Set<String> allTables = introspector.getTableNames();
        System.out.println("Tables found in database before migration: " + allTables);
        
        // Verify users table does NOT exist yet
        assertFalse(allTables.contains("users"), 
            "Users table should NOT exist before migration. Found tables: " + allTables);
        
        // Verify getTableDefinition returns null for non-existent table
        TableDefinition usersDef = introspector.getTableDefinition("users");
        System.out.println("getTableDefinition('users') returned: " + usersDef);
        assertNull(usersDef, "getTableDefinition should return null for non-existent table");
        
        // Get the schema diff
        SchemaDiff diff = mm.getSchemaDiff();
        System.out.println("Schema diff: " + diff);
        assertTrue(diff.hasChanges(), "Should have schema changes");
        
        // Verify it's a CREATE_TABLE change, not ADD_COLUMN
        boolean hasCreateTable = diff.getChanges().stream()
                .anyMatch(c -> c.getType() == SchemaDiff.ChangeType.CREATE_TABLE && 
                              c.getTableName().equalsIgnoreCase("users"));
        assertTrue(hasCreateTable, "Should have CREATE_TABLE change for users, but got: " + diff);
        
        // Now update schema
        System.out.println("Applying schema migration...");
        int changes = em.updateSchema();
        System.out.println("✓ Applied " + changes + " schema changes");

        // Should have created the users table
        assertTrue(changes > 0, "Should have applied schema changes");

        // Verify table exists
        assertTrue(introspector.tableExists("users"), "Users table should exist after migration");
        System.out.println("✓ Users table created successfully");

        // Schema should now be valid
        assertTrue(em.validateSchema(), "Schema should be valid after migration");
        System.out.println("✓ Schema validation passed");
    }

    @Test
    @Order(2)
    void testSchemaValidation() throws SQLException {
        System.out.println("=== Testing Schema Validation ===");
        
        // Register entity but don't migrate
        em.registerEntities(User.class);

        // Schema should be invalid (table doesn't exist)
        boolean validBefore = em.validateSchema();
        System.out.println("Schema valid before migration: " + validBefore);
        assertFalse(validBefore, "Schema should be invalid before migration");

        // Apply migration
        em.updateSchema();
        System.out.println("✓ Migration applied");

        // Schema should now be valid
        boolean validAfter = em.validateSchema();
        System.out.println("Schema valid after migration: " + validAfter);
        assertTrue(validAfter, "Schema should be valid after migration");
        System.out.println("✓ Schema validation works correctly");
    }

    @Test
    @Order(3)
    void testSchemaReport() throws SQLException {
        System.out.println("=== Testing Schema Report ===");
        
        em.registerEntities(User.class);

        // Get report before migration
        String reportBefore = em.getSchemaReport();
        System.out.println("Report before migration:\n" + reportBefore);
        assertTrue(reportBefore.contains("CREATE TABLE"), "Report should show CREATE TABLE needed");

        // Apply migration
        em.updateSchema();

        // Get report after migration
        String reportAfter = em.getSchemaReport();
        System.out.println("Report after migration:\n" + reportAfter);
        assertTrue(reportAfter.contains("up to date"), "Report should show schema is up to date");
        System.out.println("✓ Schema report generation works correctly");
    }

    @Test
    @Order(4)
    void testMultipleEntities() throws SQLException {
        System.out.println("=== Testing Multiple Entity Migration ===");
        
        // Register multiple entities
        em.registerEntities(User.class, Author.class, Book.class);
        System.out.println("Registered entities: User, Author, Book");

        // Update schema
        int changes = em.updateSchema();
        System.out.println("✓ Applied " + changes + " schema changes");
        assertTrue(changes > 0, "Should have applied schema changes");

        // Verify all tables exist
        SchemaIntrospector introspector = em.getMigrationManager().getIntrospector();
        Set<String> tables = introspector.getTableNames();
        System.out.println("Tables after migration: " + tables);
        
        assertTrue(introspector.tableExists("users"), "Users table should exist");
        System.out.println("✓ Users table exists");
        assertTrue(introspector.tableExists("authors"), "Authors table should exist");
        System.out.println("✓ Authors table exists");
        assertTrue(introspector.tableExists("books"), "Books table should exist");
        System.out.println("✓ Books table exists");
    }

    @Test
    @Order(5)
    void testAddColumnMigration() throws SQLException {
        System.out.println("=== Testing ADD COLUMN Migration ===");
        
        // First create the table with a simple structure
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE users (id BIGINT PRIMARY KEY AUTO_INCREMENT)");
        }
        System.out.println("Created users table with only 'id' column");

        // Register entity (which has more columns)
        em.registerEntities(User.class);

        // Schema should be invalid (missing columns)
        assertFalse(em.validateSchema(), "Schema should be invalid with missing columns");
        System.out.println("✓ Schema correctly detected as invalid (missing columns)");

        // Get the diff
        SchemaDiff diff = em.getMigrationManager().getSchemaDiff();
        System.out.println("Schema diff: " + diff);
        assertTrue(diff.hasChanges(), "Should detect missing columns");

        // Check that ADD COLUMN changes are detected
        boolean hasAddColumn = diff.getChanges().stream()
                .anyMatch(c -> c.getType() == SchemaDiff.ChangeType.ADD_COLUMN);
        assertTrue(hasAddColumn, "Should have ADD_COLUMN changes");
        System.out.println("✓ ADD_COLUMN changes detected");

        // Apply migration
        em.updateSchema();
        System.out.println("✓ Migration applied");

        // Schema should now be valid
        assertTrue(em.validateSchema(), "Schema should be valid after adding columns");
        System.out.println("✓ Schema valid after adding columns");
    }

    @Test
    @Order(6)
    void testDryRunMigration() throws SQLException {
        System.out.println("=== Testing Dry Run Migration ===");
        
        em.registerEntities(User.class);

        // Get SQL without executing (dry run)
        List<String> sql = em.getMigrationManager().migrate(true);
        System.out.println("Dry run generated SQL:");
        for (String s : sql) {
            System.out.println("  " + s);
        }
        assertFalse(sql.isEmpty(), "Should generate migration SQL");

        // Table should NOT exist yet
        assertFalse(em.getMigrationManager().getIntrospector().tableExists("users"),
                "Table should not exist after dry run");
        System.out.println("✓ Table does NOT exist after dry run (correct)");

        // Now actually migrate
        em.updateSchema();

        // Table should exist now
        assertTrue(em.getMigrationManager().getIntrospector().tableExists("users"),
                "Table should exist after actual migration");
        System.out.println("✓ Table exists after actual migration");
    }

    @Test
    @Order(7)
    void testMigrationTracking() throws SQLException {
        System.out.println("=== Testing Migration Tracking ===");
        
        em.registerEntities(User.class);

        // Apply migration
        em.updateSchema();

        // Check migration was recorded
        var appliedMigrations = em.getMigrationManager().getAppliedMigrations();
        System.out.println("Applied migrations: " + appliedMigrations);
        assertFalse(appliedMigrations.isEmpty(), "Should have recorded migration");
        System.out.println("✓ Migration tracking works correctly");
    }

    @Test
    @Order(8)
    void testIdempotentMigration() throws SQLException {
        System.out.println("=== Testing Idempotent Migration ===");
        
        em.registerEntities(User.class);

        // Apply migration twice
        int firstChanges = em.updateSchema();
        System.out.println("First migration: " + firstChanges + " changes");
        
        int secondChanges = em.updateSchema();
        System.out.println("Second migration: " + secondChanges + " changes");

        // First should have changes, second should not
        assertTrue(firstChanges > 0, "First migration should have changes");
        assertEquals(0, secondChanges, "Second migration should have no changes");
        System.out.println("✓ Migration is idempotent (safe to run multiple times)");
    }

    @Test
    @Order(9)
    void testSqlGeneratorDialects() {
        System.out.println("=== Testing SQL Generator Dialects ===");
        
        // Test dialect detection
        assertEquals(SqlGenerator.Dialect.H2, SqlGenerator.detectDialect("jdbc:h2:mem:test"));
        System.out.println("✓ H2 dialect detected");
        
        assertEquals(SqlGenerator.Dialect.MYSQL, SqlGenerator.detectDialect("jdbc:mysql://localhost/db"));
        System.out.println("✓ MySQL dialect detected");
        
        assertEquals(SqlGenerator.Dialect.POSTGRESQL, SqlGenerator.detectDialect("jdbc:postgresql://localhost/db"));
        System.out.println("✓ PostgreSQL dialect detected");
        
        assertEquals(SqlGenerator.Dialect.SQLITE, SqlGenerator.detectDialect("jdbc:sqlite:test.db"));
        System.out.println("✓ SQLite dialect detected");
    }

    @Test
    @Order(10)
    void testColumnDefinitionBuilder() {
        System.out.println("=== Testing Column Definition Builder ===");
        
        ColumnDefinition column = new ColumnDefinition.Builder()
                .name("test_column")
                .sqlType("VARCHAR")
                .length(100)
                .nullable(false)
                .primaryKey(false)
                .build();

        assertEquals("test_column", column.getName());
        assertEquals("VARCHAR", column.getSqlType());
        assertEquals(100, column.getLength());
        assertFalse(column.isNullable());
        assertFalse(column.isPrimaryKey());
        assertEquals("VARCHAR(100)", column.getFullSqlType());
        
        System.out.println("✓ Column: " + column.getName() + " " + column.getFullSqlType());
        System.out.println("✓ ColumnDefinition builder works correctly");
    }

    @Test
    @Order(11)
    void testTableDefinition() {
        System.out.println("=== Testing Table Definition ===");
        
        TableDefinition table = new TableDefinition("test_table");

        ColumnDefinition idColumn = new ColumnDefinition.Builder()
                .name("id")
                .sqlType("BIGINT")
                .primaryKey(true)
                .autoIncrement(true)
                .build();

        ColumnDefinition nameColumn = new ColumnDefinition.Builder()
                .name("name")
                .sqlType("VARCHAR")
                .length(255)
                .build();

        table.addColumn(idColumn);
        table.addColumn(nameColumn);

        assertEquals("test_table", table.getName());
        assertEquals(2, table.getColumns().size());
        assertTrue(table.hasColumn("id"));
        assertTrue(table.hasColumn("name"));
        assertEquals(1, table.getPrimaryKeyColumns().size());
        
        System.out.println("✓ Table: " + table.getName() + " with " + table.getColumns().size() + " columns");
        System.out.println("✓ Primary key columns: " + table.getPrimaryKeyColumns().size());
        System.out.println("✓ TableDefinition works correctly");
    }

    @Test
    @Order(12)
    void testMigrationWithTransaction() throws SQLException {
        System.out.println("=== Testing Migration with Transaction ===");
        
        em.registerEntities(User.class);

        // Migration should work within its own transaction
        em.beginTransaction();
        System.out.println("Transaction started");
        try {
            em.updateSchema();
            System.out.println("✓ Schema updated within transaction");
            em.commit();
            System.out.println("✓ Transaction committed");
        } catch (Exception e) {
            em.rollback();
            System.out.println("✗ Transaction rolled back: " + e.getMessage());
            fail("Migration within transaction should not fail: " + e.getMessage());
        }

        assertTrue(em.validateSchema(), "Schema should be valid after transactional migration");
        System.out.println("✓ Migration with transaction works correctly");
    }
}
