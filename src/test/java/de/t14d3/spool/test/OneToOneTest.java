package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Passport;
import de.t14d3.spool.test.entities.Person;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One-to-one support: the owning side persists the FK column, the inverse side
 * is hydrated by an FK lookup, and the inverse side cascades the owner first
 * so a generated id is available when the owner row is written.
 */
public class OneToOneTest {
    private static final String URL = "jdbc:h2:mem:one_to_one;DB_CLOSE_DELAY=-1";

    private EntityManager openSchema() {
        EntityManager em = EntityManager.create(URL);
        em.getExecutor().execute("DROP TABLE IF EXISTS passports", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS persons", List.of());
        em.registerEntities(Person.class, Passport.class);
        em.updateSchema();
        return em;
    }

    @Test
    void owningSideWritesForeignKeyAndBothSidesHydrate() throws Exception {
        EntityManager em = openSchema();
        Long personId;
        Long passportId;
        try {
            Person person = new Person("ada");
            Passport passport = new Passport("P-1");
            passport.setPerson(person);
            person.setPassport(passport);

            em.persist(passport);
            em.flush();

            personId = person.getId();
            passportId = passport.getId();
            assertNotNull(personId);
            assertNotNull(passportId);
            assertEquals(personId, fkOf(em, passportId));
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(URL);
        try {
            reader.registerEntities(Person.class, Passport.class);

            Passport loaded = reader.find(Passport.class, passportId);
            assertNotNull(loaded);
            assertNotNull(loaded.getPerson(), "owning side must hydrate its reference");
            assertEquals("ada", loaded.getPerson().getName());

            Person person = reader.find(Person.class, personId);
            assertNotNull(person);
            assertNotNull(person.getPassport(), "inverse side must hydrate by FK lookup");
            assertEquals("P-1", person.getPassport().getNumber());
            assertSame(loaded, person.getPassport(), "identity map must keep one instance per row");
        } finally {
            reader.close();
        }
    }

    @Test
    void inverseSideCascadePersistsOwnerBeforeDependentRow() throws Exception {
        EntityManager em = openSchema();
        try {
            Person person = new Person("bob");
            Passport passport = new Passport("P-2");
            passport.setPerson(person);
            person.setPassport(passport);

            em.persist(person);
            em.flush();

            assertNotNull(person.getId());
            assertNotNull(passport.getId());
            assertEquals(person.getId(), fkOf(em, passport.getId()));
        } finally {
            em.close();
        }
    }

    private Long fkOf(EntityManager em, Long passportId) throws Exception {
        String sql = "SELECT person_id FROM passports WHERE id = ?";
        try (PreparedStatement ps = em.getExecutor().getConnection().prepareStatement(sql)) {
            ps.setLong(1, passportId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                long value = rs.getLong(1);
                return rs.wasNull() ? null : value;
            }
        }
    }
}
