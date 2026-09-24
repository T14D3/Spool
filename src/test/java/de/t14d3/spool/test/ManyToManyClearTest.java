package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Course;
import de.t14d3.spool.test.entities.Student;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression coverage for T1.1: {@code clear()} and {@code rollback()} must
 * drop dirty ManyToMany flags, otherwise a later reload of the same
 * class + id makes the next flush issue a spurious join-table sync.
 */
public class ManyToManyClearTest {
    private EntityManager em;
    private QueryCountingConnection counting;
    private Long studentId;
    private Long extraCourseId;

    @BeforeEach
    void setup() throws Exception {
        String dbName = "m2m_clear_" + UUID.randomUUID().toString().replace("-", "");
        String url = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";

        EntityManager seed = EntityManager.create(url);
        try {
            seed.registerEntities(Student.class, Course.class);
            seed.updateSchema();

            Student student = new Student("Ada");
            Course course = new Course("Databases 101");
            Course extra = new Course("Compilers");
            student.getCourses().add(course);
            seed.persist(course);
            seed.persist(extra);
            seed.persist(student);
            seed.flush();

            studentId = student.getId();
            extraCourseId = extra.getId();
        } finally {
            seed.close();
        }

        counting = new QueryCountingConnection(DriverManager.getConnection(url));
        em = EntityManager.create(counting.getProxy());
        em.registerEntities(Student.class, Course.class);
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void clearDropsDirtyManyToManyFlags() {
        Student student = em.find(Student.class, studentId);
        student.getCourses().add(em.find(Course.class, extraCourseId));
        em.clear();

        Student reloaded = em.find(Student.class, studentId);
        assertEquals(1, reloaded.getCourses().size(), "rollback/clear must not drop committed join rows");

        counting.reset();
        em.flush();
        assertEquals(0, counting.getPreparedStatements(),
                "clear() must drop dirty ManyToMany flags; flush issued join-table SQL");
    }

    @Test
    void rollbackDropsDirtyManyToManyFlags() {
        em.beginTransaction();
        Student student = em.find(Student.class, studentId);
        student.getCourses().add(em.find(Course.class, extraCourseId));
        em.rollback();

        Student reloaded = em.find(Student.class, studentId);
        assertEquals(1, reloaded.getCourses().size(), "rolled-back link must not be visible");

        counting.reset();
        em.flush();
        assertEquals(0, counting.getPreparedStatements(),
                "rollback() must drop dirty ManyToMany flags; flush issued join-table SQL");
    }
}
