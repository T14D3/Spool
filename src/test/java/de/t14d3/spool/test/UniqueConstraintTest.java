package de.t14d3.spool.test;

import de.t14d3.spool.UniqueConstraintViolationException;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.mapping.UniqueConstraintDef;
import de.t14d3.spool.migration.SchemaDiff;
import de.t14d3.spool.migration.TableDefinition;
import de.t14d3.spool.test.entities.UqColumnFlag;
import de.t14d3.spool.test.entities.UqComposite;
import de.t14d3.spool.test.entities.UqSingleColumn;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end coverage of the unique-constraint pipeline:
 * <ol>
 *     <li>Entity metadata surfaces single-column {@code @Column(unique=true)}
 *         and table-level {@code @Table(uniqueConstraints=...)} entries as
 *         {@link UniqueConstraintDef} instances.</li>
 *     <li>Migration tooling emits dialect-appropriate DDL
 *         (inline {@code CONSTRAINT ... UNIQUE} on MySQL/PostgreSQL/H2,
 *         separate {@code CREATE UNIQUE INDEX} on SQLite).</li>
 *     <li>The introspector reads existing unique indexes so the diff stays
 *         empty after a fresh migrate.</li>
 *     <li>The database actually enforces the constraint; a duplicate insert
 *         surfaces as a {@link UniqueConstraintViolationException} on the
 *         consumer's call site (H2 here, the cleanest cross-dialect
 *         target for testing the SQLState path).</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class UniqueConstraintTest {

    private Connection connection;
    private EntityManager em;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
                "jdbc:h2:mem:" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        em = EntityManager.create(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
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
    void singleColumnAnnotationSurfacesAsUniqueConstraintDef() {
        System.out.println("=== @Column(unique=true) surfaces as UniqueConstraintDef ===");
        List<UniqueConstraintDef> defs =
                de.t14d3.spool.mapping.EntityMetadata.of(UqColumnFlag.class).getUniqueConstraints();
        assertEquals(1, defs.size(), "expected one unique constraint from @Column(unique=true)");
        UniqueConstraintDef def = defs.get(0);
        assertEquals("uniq_uq_column_flag_email", def.getName(),
                "auto-generated name from table + column");
        assertEquals(List.of("email"), def.getColumns());
        System.out.println("✓ @Column(unique=true) constraint: " + def);
    }

    @Test
    @Order(2)
    void tableLevelAnnotationSurfacesAsUniqueConstraintDef() {
        System.out.println("=== @Table(uniqueConstraints) surfaces as UniqueConstraintDef ===");
        List<UniqueConstraintDef> defs =
                de.t14d3.spool.mapping.EntityMetadata.of(UqSingleColumn.class).getUniqueConstraints();
        assertEquals(1, defs.size());
        UniqueConstraintDef def = defs.get(0);
        assertEquals("uq_single_email", def.getName());
        assertEquals(List.of("email"), def.getColumns());
        System.out.println("✓ table-level constraint: " + def);
    }

    @Test
    @Order(3)
    void compositeUniqueConstraintSurfaces() {
        System.out.println("=== Composite @Table(uniqueConstraints) ===");
        List<UniqueConstraintDef> defs =
                de.t14d3.spool.mapping.EntityMetadata.of(UqComposite.class).getUniqueConstraints();
        assertEquals(1, defs.size());
        UniqueConstraintDef def = defs.get(0);
        assertEquals("uq_composite_owner_repo", def.getName());
        assertEquals(List.of("owner_id", "repo_id"), def.getColumns());
        System.out.println("✓ composite constraint: " + def);
    }

    @Test
    @Order(4)
    void migrationDdlEmitsInlineUniqueOnH2() throws SQLException {
        System.out.println("=== CREATE TABLE on H2 includes inline UNIQUE ===");
        em.registerEntities(UqSingleColumn.class, UqColumnFlag.class, UqComposite.class);
        List<String> sql = em.getMigrationManager().generateMigrationSql();
        System.out.println("Generated SQL:");
        sql.forEach(s -> System.out.println("  " + s));

        // H2 uppercases unquoted identifiers, so the expected SQL uses
        // uppercase column names. The SqlGenerator wraps names with the
        // dialect's quoteIdentifier(), which for H2 yields
        // uppercase-quoted identifiers.
        String createSingle = findCreateTable(sql, "uq_single_column");
        assertTrue(createSingle.contains("CONSTRAINT \"UQ_SINGLE_EMAIL\" UNIQUE (\"EMAIL\""),
                "uq_single_column should embed the named unique constraint inline, got: " + createSingle);

        String createColumnFlag = findCreateTable(sql, "uq_column_flag");
        assertTrue(createColumnFlag.contains("CONSTRAINT \"UNIQ_UQ_COLUMN_FLAG_EMAIL\" UNIQUE (\"EMAIL\""),
                "uq_column_flag should embed the auto-named single-column constraint inline, got: " + createColumnFlag);

        String createComposite = findCreateTable(sql, "uq_composite");
        assertTrue(createComposite.contains("CONSTRAINT \"UQ_COMPOSITE_OWNER_REPO\" UNIQUE (\"OWNER_ID\", \"REPO_ID\""),
                "uq_composite should embed the composite constraint inline, got: " + createComposite);
    }

    @Test
    @Order(5)
    void migrateAndValidateSchema() throws SQLException {
        System.out.println("=== updateSchema + validateSchema converge ===");
        em.registerEntities(UqSingleColumn.class, UqColumnFlag.class, UqComposite.class);
        int applied = em.updateSchema();
        System.out.println("Applied " + applied + " statements");
        if (!em.validateSchema()) {
            System.out.println("REPORT:\n" + em.getSchemaReport());
            System.out.println("DIFF:\n" + em.getMigrationManager().getSchemaDiff());
        }
        assertTrue(em.validateSchema(), "schema should be in sync after updateSchema");
        assertEquals(0, em.getMigrationManager().generateMigrationSql().size(),
                "no further migrations expected after converge");
    }

    @Test
    @Order(6)
    void introspectorReadsUniqueConstraintsFromH2() throws SQLException {
        System.out.println("=== SchemaIntrospector picks up unique indexes from H2 ===");
        em.registerEntities(UqComposite.class);
        em.updateSchema();
        TableDefinition table = em.getMigrationManager().getIntrospector()
                .getTableDefinition("uq_composite");
        assertNotNull(table);
        System.out.println("Introspected table: " + table);
        // H2 renames the inline CONSTRAINT into "<name>_INDEX_A"; the
        // introspector surfaces the actual database-assigned name, so
        // assert on the column set (case-insensitively - H2 uppercases
        // unquoted identifiers by default) rather than the declared name.
        Set<Set<String>> columnSets = table.getUniqueConstraints().stream()
                .map(uc -> uc.getColumns().stream()
                        .map(c -> c.toLowerCase(java.util.Locale.ROOT))
                        .collect(java.util.stream.Collectors.toSet()))
                .collect(java.util.stream.Collectors.toSet());
        Set<String> expected = Set.of("owner_id", "repo_id");
        assertTrue(columnSets.contains(expected),
                "introspected unique constraint should cover the declared columns, got: " + columnSets);
    }

    @Test
    @Order(7)
    void duplicateInsertThrowsTypedException() throws SQLException {
        System.out.println("=== Duplicate insert surfaces as UniqueConstraintViolationException ===");
        em.registerEntities(UqSingleColumn.class);
        em.updateSchema();

        // First row succeeds.
        UqSingleColumn first = new UqSingleColumn("alice@example.com");
        em.persist(first);
        em.flush();

        // Second row with the same email value must fail at the database.
        UqSingleColumn dup = new UqSingleColumn("alice@example.com");
        em.persist(dup);
        // EntityManager.flush() catches the underlying SQLException and
        // surfaces a UniqueConstraintViolationException (a runtime
        // exception) directly. The original SQLException is the cause.
        UniqueConstraintViolationException thrown = assertThrows(
                UniqueConstraintViolationException.class,
                () -> em.flush(),
                "duplicate insert must surface as the typed exception"
        );
        System.out.println("Caught: " + thrown.getClass().getSimpleName()
                + " constraint=" + thrown.getConstraintName()
                + " entity=" + thrown.getEntityName()
                + " sqlState=" + thrown.getSQLState()
                + " cause=" + thrown.getCause().getClass().getSimpleName());
        // H2 renames the constraint (the original "uq_single_email"
        // becomes "UQ_SINGLE_EMAIL_INDEX_E" after materialization) and
        // EntityManager.flush() does not know the entity class, so the
        // entityName is null until an application-level wrapper sets
        // it. The key invariants are: SQLState, constraint name
        // non-null, original cause is a SQLException.
        assertEquals("23505", thrown.getSQLState(),
                "H2 emits SQLState 23505 for unique violations");
        assertNotNull(thrown.getConstraintName(),
                "constraint name should be parsed from the vendor error message");
        assertNotNull(thrown.getCause(),
                "the original SQLException must be retained as the cause");
        assertTrue(thrown.getCause() instanceof java.sql.SQLException,
                "the original cause should be a SQLException");
    }

    @Test
    @Order(9)
    void cancelPendingDropsEnqueuedInsert() throws SQLException {
        System.out.println("=== cancelPending removes enqueued insert so retry can succeed ===");
        em.registerEntities(UqSingleColumn.class);
        em.updateSchema();

        // First commit establishes the row.
        em.persist(new UqSingleColumn("alice@example.com"));
        em.flush();

        // Second persist enqueues a duplicate insert. id is null, so
        // Spool routes to pendingInserts; the unique constraint fires
        // on flush because the email column collides.
        UqSingleColumn retry = new UqSingleColumn("alice@example.com");
        em.persist(retry);
        UniqueConstraintViolationException thrown = assertThrows(
                UniqueConstraintViolationException.class,
                () -> em.flush(),
                "first flush must collide on the unique constraint"
        );
        assertNotNull(thrown);

        // Drop the orphan. Without this, the next flush() would
        // collide again on the same row because the orphan is still
        // enqueued in pendingInserts.
        em.cancelPending(retry);

        // Now the queue is clean; a no-op flush must not throw. If
        // cancelPending had not removed the orphan, the constraint
        // would fire again here.
        em.flush();
    }

    @Test
    @Order(8)
    void diffIsEmptyAfterConverge() throws SQLException {
        System.out.println("=== SchemaDiff is empty after migrate ===");
        em.registerEntities(UqSingleColumn.class, UqColumnFlag.class, UqComposite.class);
        em.updateSchema();
        SchemaDiff diff = em.getMigrationManager().getSchemaDiff();
        assertFalse(diff.hasChanges(),
                "diff must be empty after migrate; got: " + diff);
    }

    private static String findCreateTable(List<String> sql, String tableName) {
        String needle = "\"" + tableName.toUpperCase() + "\"";
        for (String s : sql) {
            if (s.contains("CREATE TABLE") && s.toUpperCase().contains(needle)) {
                return s;
            }
        }
        fail("CREATE TABLE for " + tableName + " not found in: " + sql);
        return "";
    }
}
