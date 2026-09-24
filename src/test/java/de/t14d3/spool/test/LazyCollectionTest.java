package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.Course;
import de.t14d3.spool.test.entities.Post;
import de.t14d3.spool.test.entities.Student;
import de.t14d3.spool.test.entities.Tag;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LazyCollectionTest {

    private static String freshUrl(String prefix) {
        return "jdbc:h2:mem:" + prefix + "_" + UUID.randomUUID().toString().replace("-", "")
                + ";DB_CLOSE_DELAY=-1";
    }

    @Test
    void lazyOneToManyLoadsOnFirstAccess() {
        String url = freshUrl("lazy_o2m");

        Long authorId;
        EntityManager writer = EntityManager.create(url);
        try {
            writer.registerEntities(Author.class, Book.class).updateSchema();
            Author author = new Author("Ursula", "ursula@example.com");
            author.addBook(new Book("The Dispossessed", "isbn-1"));
            author.addBook(new Book("The Left Hand of Darkness", "isbn-2"));
            writer.persist(author);
            writer.flush();
            authorId = author.getId();
        } finally {
            writer.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(Author.class, Book.class).updateSchema();
            Author author = reader.find(Author.class, authorId);
            assertNotNull(author);

            // Trigger the lazy load.
            assertEquals(2, author.getBooks().size());
            assertSame(author.getBooks(), author.getBooks(), "the same lazy wrapper must be reused");

            for (Book book : author.getBooks()) {
                assertNotNull(book.getAuthor());
                assertEquals(authorId, book.getAuthor().getId());
            }

            // Nothing is dirty: flush must not delete or duplicate rows.
            reader.flush();
            assertEquals(2, countRows(reader, "books", "author_id", authorId));
        } finally {
            reader.close();
        }
    }

    @Test
    void lazyManyToManyLoadsOnFirstAccessAndSyncsInverse() {
        String url = freshUrl("lazy_m2m");

        Long studentId;
        EntityManager writer = EntityManager.create(url);
        try {
            writer.registerEntities(Student.class, Course.class).updateSchema();
            Student student = new Student("Ada");
            Course databases = new Course("Databases");
            Course compilers = new Course("Compilers");
            student.getCourses().add(databases);
            student.getCourses().add(compilers);
            writer.persist(databases);
            writer.persist(compilers);
            writer.persist(student);
            writer.flush();
            studentId = student.getId();
        } finally {
            writer.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(Student.class, Course.class).updateSchema();
            Student student = reader.find(Student.class, studentId);
            assertNotNull(student);

            assertEquals(2, student.getCourses().size());
            for (Course course : student.getCourses()) {
                assertTrue(course.getStudents().contains(student),
                        "lazy ManyToMany load must wire the inverse collection");
            }

            // Mutating the lazily loaded collection must mark it dirty and
            // persist the join-row delta.
            Course first = student.getCourses().get(0);
            student.getCourses().remove(first);
            reader.flush();

            assertEquals(1, countRows(reader, "students_courses", "students_id", studentId));
        } finally {
            reader.close();
        }
    }

    @Test
    void lazyManyToManyInverseSideLoads() {
        String url = freshUrl("lazy_m2m_inverse");

        Long courseId;
        EntityManager writer = EntityManager.create(url);
        try {
            writer.registerEntities(Student.class, Course.class).updateSchema();
            Student student = new Student("Grace");
            Course course = new Course("Networks");
            student.getCourses().add(course);
            writer.persist(course);
            writer.persist(student);
            writer.flush();
            courseId = course.getId();
        } finally {
            writer.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(Student.class, Course.class).updateSchema();
            Course course = reader.find(Course.class, courseId);
            assertNotNull(course);

            assertEquals(1, course.getStudents().size());
            assertEquals("Grace", course.getStudents().get(0).getName());
        } finally {
            reader.close();
        }
    }

    @Test
    void unidirectionalLazyManyToManyLoads() {
        String url = freshUrl("lazy_m2m_uni");

        Long postId;
        EntityManager writer = EntityManager.create(url);
        try {
            writer.registerEntities(Post.class, Tag.class).updateSchema();
            Post post = new Post("Hello");
            Tag java = new Tag("java");
            Tag orm = new Tag("orm");
            post.getTags().add(java);
            post.getTags().add(orm);
            writer.persist(java);
            writer.persist(orm);
            writer.persist(post);
            writer.flush();
            postId = post.getId();
        } finally {
            writer.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(Post.class, Tag.class).updateSchema();
            Post post = reader.find(Post.class, postId);
            assertNotNull(post);

            assertEquals(2, post.getTags().size());
            assertEquals("java", post.getTags().get(0).getLabel());
        } finally {
            reader.close();
        }
    }

    private static int countRows(EntityManager em, String table, String column, Object value) {
        String sql = "SELECT COUNT(*) AS c FROM " + table + " WHERE " + column + " = ?";
        try (PreparedStatement ps = em.getExecutor().getConnection().prepareStatement(sql)) {
            ps.setObject(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt("c");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
