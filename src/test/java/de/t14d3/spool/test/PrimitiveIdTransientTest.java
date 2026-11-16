package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.PrimitiveIdCategory;
import de.t14d3.spool.test.entities.PrimitiveIdTicket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the transient-detection contract for entities whose id is a primitive
 * {@code int} with {@code autoIncrement = true}.
 * <p>
 * Such an entity cannot express "no id yet" with {@code null}; the unset
 * default {@code 0} has to carry that meaning. If it does not, a cascading
 * reference is written with a foreign key of {@code 0} instead of the
 * generated key of the referenced row.
 */
public class PrimitiveIdTransientTest {
    private Path databaseFile;
    private EntityManager em;

    @BeforeEach
    void setup() throws Exception {
        databaseFile = Files.createTempFile("spool-primitive-id-", ".db");
        Files.deleteIfExists(databaseFile);
        em = EntityManager.create("jdbc:sqlite:" + databaseFile.toAbsolutePath());
        em.registerEntities(PrimitiveIdCategory.class, PrimitiveIdTicket.class);
        em.updateSchema();
    }

    @AfterEach
    void teardown() throws Exception {
        if (em != null) {
            em.close();
        }
        if (databaseFile != null) {
            Files.deleteIfExists(databaseFile);
        }
    }

    @Test
    void cascadingReferenceGetsTheGeneratedForeignKey() {
        PrimitiveIdCategory category = new PrimitiveIdCategory("default");
        PrimitiveIdTicket ticket = new PrimitiveIdTicket("first");
        ticket.setCategory(category);

        em.persist(ticket);
        em.flush();

        assertTrue(category.getId() > 0, "the referenced row must get a generated id");
        assertTrue(ticket.getId() > 0, "the owning row must get a generated id");

        // The FK must point at the referenced row, not at the unset id 0.
        assertEquals(category.getId(), readForeignKey(ticket.getId()));
    }

    @Test
    void theRelationSurvivesAReload() {
        PrimitiveIdCategory category = new PrimitiveIdCategory("default");
        PrimitiveIdTicket ticket = new PrimitiveIdTicket("first");
        ticket.setCategory(category);

        em.persist(ticket);
        em.flush();

        // A fresh context must see the same relation the writer saw.
        em.clear();
        PrimitiveIdTicket reloaded = em.find(PrimitiveIdTicket.class, ticket.getId());
        assertNotNull(reloaded);
        assertNotNull(reloaded.getCategory(), "the relation must resolve after a reload");
        assertEquals(category.getId(), reloaded.getCategory().getId());
        assertEquals("default", reloaded.getCategory().getName());
    }

    @Test
    void aTransientEntityHasNoUsableId() {
        PrimitiveIdCategory category = new PrimitiveIdCategory("default");

        // A transient entity must not be mistaken for a persisted one: its id
        // is the unset default, so refreshing it must fail as transient.
        assertThrows(IllegalStateException.class, () -> em.refresh(category));
    }

    /**
     * Read the raw {@code category_id} column of a ticket row, bypassing the
     * entity mapping so the stored value is asserted, not the in-memory one.
     */
    private int readForeignKey(int ticketId) {
        List<Object> rows = em.executeProjectionQuery(
                "SELECT category_id FROM primitive_id_tickets WHERE id = ?", List.of(ticketId));
        assertEquals(1, rows.size(), "the ticket row must exist");
        return ((Number) rows.get(0)).intValue();
    }
}
