package de.t14d3.spool.test;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.migration.ColumnDefinition;
import de.t14d3.spool.migration.SchemaIntrospector;
import de.t14d3.spool.migration.TableDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for explicit column type specification in @Column annotation.
 */
public class ColumnTypeTest {

    private Connection connection;
    private EntityManager em;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
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
    void testExplicitColumnType() {
        System.out.println("=== Testing Explicit Column Type ===");

        // Register the test entity with explicit column type
        em.registerEntities(TestEntity.class);

        // Create schema from entity metadata
        SchemaIntrospector introspector = em.getMigrationManager().getIntrospector();
        TableDefinition tableDef = introspector.buildTableDefinitionFromEntity(TestEntity.class);

        // Check that the description column uses TEXT type instead of VARCHAR
        assertTrue(tableDef.hasColumn("description"), "Should have description column");
        ColumnDefinition descColumn = tableDef.getColumns().get("description");
        assertEquals("TEXT", descColumn.getSqlType(), "Description column should use TEXT type (explicitly specified)");

        // Check that the notes column uses VARCHAR (default inferred type)
        assertTrue(tableDef.hasColumn("notes"), "Should have notes column");
        ColumnDefinition notesColumn = tableDef.getColumns().get("notes");
        assertEquals("VARCHAR", notesColumn.getSqlType(), "Notes column should use VARCHAR type (default inferred)");

        // Check that the name column uses VARCHAR(150) (explicit type with length from annotation)
        assertTrue(tableDef.hasColumn("name"), "Should have name column");
        ColumnDefinition nameColumn = tableDef.getColumns().get("name");
        assertEquals("VARCHAR", nameColumn.getSqlType(), "Name column should use VARCHAR type");
        assertEquals(150, nameColumn.getLength(), "Name column should have length 150");

        // Check that the age column uses INTEGER type (default inferred type)
        assertTrue(tableDef.hasColumn("age"), "Should have age column");
        ColumnDefinition ageColumn = tableDef.getColumns().get("age");
        assertEquals("INTEGER", ageColumn.getSqlType(), "Age column should use INTEGER type (default inferred)");

        System.out.println("✓ Explicit column types work correctly");
        System.out.println("✓ Default type inference still works for columns without type() specification");
    }
}

@Entity
@Table(name = "test_entities")
class TestEntity {
    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "name", length = 150)
    private String name;

    @Column(name = "description", type = "TEXT")
    private String description;

    @Column(name = "notes")
    private String notes;

    @Column(name = "age")
    private Integer age;

    // Constructor, getters, setters omitted for brevity
    public TestEntity() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }

    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }
}
