package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.CourseJoinTable;
import de.t14d3.spool.test.entities.StudentJoinTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ManyToManyJoinTableCustomizationTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        String dbName = "m2m_jt_" + UUID.randomUUID().toString().replace("-", "");
        em = EntityManager.create("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");
        assertDoesNotThrow(() -> em.registerEntities(StudentJoinTable.class, CourseJoinTable.class).getMigrationManager().updateSchema());
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void testCustomJoinTableIsCreatedAndUsedForPersistence() throws Exception {
        StudentJoinTable student = new StudentJoinTable("Linus");
        CourseJoinTable course = new CourseJoinTable("Operating Systems");

        student.getCourses().add(course);

        em.persist(course);
        em.persist(student);
        em.flush();

        assertNotNull(student.getId());
        assertNotNull(course.getId());

        assertEquals(1, countEnrollments(student.getId()));

        student.getCourses().remove(course);
        em.flush();

        assertEquals(0, countEnrollments(student.getId()));
    }

    private int countEnrollments(Long studentId) throws Exception {
        String sql = "SELECT COUNT(*) AS c FROM enrollments WHERE student_ref = ?";
        try (PreparedStatement ps = em.getExecutor().getConnection().prepareStatement(sql)) {
            ps.setLong(1, studentId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt("c");
            }
        }
    }
}
