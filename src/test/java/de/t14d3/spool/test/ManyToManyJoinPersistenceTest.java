package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Course;
import de.t14d3.spool.test.entities.Student;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ManyToManyJoinPersistenceTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        String dbName = "m2m_join_" + UUID.randomUUID().toString().replace("-", "");
        em = EntityManager.create("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");
        assertDoesNotThrow(() -> em.registerEntities(Student.class, Course.class).getMigrationManager().updateSchema());
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void testJoinRowsInsertedAndUpdatedOnRelationshipChange() throws Exception {
        Student student = new Student("Ada");
        Course course1 = new Course("Databases 101");
        Course course2 = new Course("Compilers");

        student.getCourses().add(course1);
        student.getCourses().add(course2);

        em.persist(course1);
        em.persist(course2);
        em.persist(student);
        em.flush();

        assertNotNull(student.getId());
        assertNotNull(course1.getId());
        assertNotNull(course2.getId());

        assertEquals(2, countJoinRowsForStudent(student.getId()));

        // Remove one link and flush without calling persist again.
        student.getCourses().remove(course1);
        em.flush();
        assertEquals(1, countJoinRowsForStudent(student.getId()));

        // Re-add from inverse side, verify it persists.
        course1.getStudents().add(student);
        em.flush();
        assertEquals(2, countJoinRowsForStudent(student.getId()));
    }

    private int countJoinRowsForStudent(Long studentId) throws Exception {
        String sql = "SELECT COUNT(*) AS c FROM students_courses WHERE students_id = ?";
        try (PreparedStatement ps = em.getExecutor().getConnection().prepareStatement(sql)) {
            ps.setLong(1, studentId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt("c");
            }
        }
    }
}

