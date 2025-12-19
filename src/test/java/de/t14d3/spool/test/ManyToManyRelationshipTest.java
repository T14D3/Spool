package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Course;
import de.t14d3.spool.test.entities.Student;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ManyToManyRelationshipTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        String dbName = "m2m_rel_" + UUID.randomUUID().toString().replace("-", "");
        em = EntityManager.create("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");
        assertDoesNotThrow(() -> em.registerEntities(Student.class, Course.class).getMigrationManager().updateSchema());
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void testManyToManyBidirectionalSyncOnPersist() {
        Student student = new Student("Ada");
        Course course = new Course("Databases 101");

        // Modify only one side.
        student.getCourses().add(course);
        assertFalse(course.getStudents().contains(student));

        // Persist should prepare relationships and sync both sides.
        em.persist(student);
        assertTrue(course.getStudents().contains(student));

        // Wrapper should keep both sides in sync for subsequent modifications.
        student.getCourses().remove(course);
        assertFalse(course.getStudents().contains(student));
    }

    @Test
    void testManyToManyBidirectionalSyncFromInverseSide() {
        Student student = new Student("Grace");
        Course course = new Course("Compilers");

        em.persist(course); // installs wrapper on inverse side

        // Modify only inverse side.
        course.getStudents().add(student);
        assertTrue(student.getCourses().contains(course));

        course.getStudents().remove(student);
        assertFalse(student.getCourses().contains(course));
    }
}
