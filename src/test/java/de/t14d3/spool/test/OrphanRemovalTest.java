package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.OrphanChild;
import de.t14d3.spool.test.entities.OrphanDepartment;
import de.t14d3.spool.test.entities.OrphanEmployee;
import de.t14d3.spool.test.entities.OrphanParent;
import de.t14d3.spool.test.entities.Passport;
import de.t14d3.spool.test.entities.Person;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T2.3: collection-removal semantics. Default removal leaves child rows
 * untouched (unidirectional associations null the managed FK); with
 * {@code orphanRemoval = true} the removed row is deleted at flush.
 */
public class OrphanRemovalTest {

    @Test
    void mappedByRemovalWithoutOrphanRemovalKeepsChildRow() throws Exception {
        String url = url("no_orphan");
        EntityManager em = EntityManager.create(url);
        Long bookId;
        Long authorId;
        try {
            em.registerEntities(Author.class, Book.class);
            em.updateSchema();
            Author author = new Author("ada", "ada@example.com");
            Book book = new Book("b", "isbn");
            author.addBook(book);
            em.persist(author);
            em.flush();
            bookId = book.getId();
            authorId = author.getId();
            assertTrue(exists(em, "books", bookId));

            author.getBooks().remove(book);
            em.flush();
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(Author.class, Book.class);
            assertTrue(exists(reader, "books", bookId), "without orphanRemoval the child row stays");
            assertEquals(authorId, longValue(reader, "books", "author_id", bookId),
                    "removal from a mappedBy collection leaves the child's own FK untouched");
        } finally {
            reader.close();
        }
    }

    @Test
    void mappedByRemovalWithOrphanRemovalDeletesChildRow() throws Exception {
        String url = url("orphan_mapped");
        EntityManager em = EntityManager.create(url);
        Long removedId;
        Long keptId;
        try {
            em.registerEntities(OrphanParent.class, OrphanChild.class);
            em.updateSchema();
            OrphanParent parent = new OrphanParent("p");
            OrphanChild removed = new OrphanChild("removed");
            OrphanChild kept = new OrphanChild("kept");
            for (OrphanChild child : List.of(removed, kept)) {
                child.setParent(parent);
                parent.getChildren().add(child);
            }
            em.persist(parent);
            em.flush();
            removedId = removed.getId();
            keptId = kept.getId();
            assertTrue(exists(em, "orphan_children", removedId));

            parent.getChildren().remove(removed);
            em.flush();
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(OrphanParent.class, OrphanChild.class);
            assertFalse(exists(reader, "orphan_children", removedId), "orphanRemoval must delete the removed child");
            assertTrue(exists(reader, "orphan_children", keptId), "kept children must survive");
        } finally {
            reader.close();
        }
    }

    @Test
    void unidirectionalRemovalWithOrphanRemovalDeletesChildRow() throws Exception {
        String url = url("orphan_unidirectional");
        EntityManager em = EntityManager.create(url);
        Long removedId;
        try {
            em.registerEntities(OrphanDepartment.class, OrphanEmployee.class);
            em.updateSchema();
            OrphanDepartment department = new OrphanDepartment("d");
            OrphanEmployee removed = new OrphanEmployee("removed");
            OrphanEmployee kept = new OrphanEmployee("kept");
            department.getEmployees().add(removed);
            department.getEmployees().add(kept);
            em.persist(department);
            em.flush();
            removedId = removed.getId();
            assertEquals(department.getId(), longValue(em, "orphan_employees", "department_id", removedId));

            department.getEmployees().remove(removed);
            em.flush();
            assertFalse(exists(em, "orphan_employees", removedId));
        } finally {
            em.close();
        }
    }

    @Test
    void deletingOwnerWithOrphanRemovalDeletesChildren() throws Exception {
        String url = url("orphan_owner_delete");
        EntityManager em = EntityManager.create(url);
        Long childId;
        try {
            em.registerEntities(OrphanParent.class, OrphanChild.class);
            em.updateSchema();
            OrphanParent parent = new OrphanParent("p");
            OrphanChild child = new OrphanChild("c");
            child.setParent(parent);
            parent.getChildren().add(child);
            em.persist(parent);
            em.flush();
            childId = child.getId();

            em.remove(parent);
            em.flush();
            assertFalse(exists(em, "orphan_children", childId),
                    "removing an owner with orphanRemoval must delete its children");
        } finally {
            em.close();
        }
    }

    @Test
    void replacingOwningOneToOneWithOrphanRemovalDeletesOldRow() throws Exception {
        String url = url("orphan_one_to_one");
        EntityManager em = EntityManager.create(url);
        Long oldPersonId;
        try {
            em.registerEntities(Person.class, Passport.class);
            em.updateSchema();
            Person oldPerson = new Person("old");
            Person newPerson = new Person("new");
            Passport passport = new Passport("P-1");
            passport.setPerson(oldPerson);
            oldPerson.setPassport(passport);
            em.persist(passport);
            em.persist(newPerson);
            em.flush();
            oldPersonId = oldPerson.getId();

            passport.setPerson(newPerson);
            newPerson.setPassport(passport);
            em.flush();

            assertFalse(exists(em, "persons", oldPersonId),
                    "replacing an owning OneToOne with orphanRemoval must delete the old row");
            assertEquals(newPerson.getId(), longValue(em, "passports", "person_id", passport.getId()));
        } finally {
            em.close();
        }
    }

    private static String url(String name) {
        return "jdbc:h2:mem:" + name + "_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    }

    private static boolean exists(EntityManager em, String table, Long id) throws Exception {
        try (PreparedStatement ps = em.getExecutor().getConnection()
                .prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1) > 0;
            }
        }
    }

    private static Long longValue(EntityManager em, String table, String column, Long id) throws Exception {
        try (PreparedStatement ps = em.getExecutor().getConnection()
                .prepareStatement("SELECT " + column + " FROM " + table + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                long value = rs.getLong(1);
                return rs.wasNull() ? null : value;
            }
        }
    }
}
