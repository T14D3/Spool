package de.t14d3.spool.test;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.test.entities.BaseEntity;
import de.t14d3.spool.test.entities.MappedSuperclassOrder;
import de.t14d3.spool.test.entities.MappedSuperclassUser;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T1.3 / T5.1: fields declared in a {@code @MappedSuperclass} are copied into
 * every entity subclass mapping; a superclass that is neither mapped nor
 * field-less is rejected instead of silently dropping its fields.
 */
public class MappedSuperclassTest {
    private static final String URL = "jdbc:h2:mem:mapped_superclass;DB_CLOSE_DELAY=-1";

    @Test
    void baseFieldsAreMappedIntoEachSubclassTable() {
        EntityManager em = EntityManager.create(URL);
        try {
            em.getExecutor().execute("DROP TABLE IF EXISTS mapped_superclass_orders", List.of());
            em.getExecutor().execute("DROP TABLE IF EXISTS mapped_superclass_users", List.of());
            em.registerEntities(MappedSuperclassUser.class, MappedSuperclassOrder.class);
            em.updateSchema();

            LocalDateTime now = LocalDateTime.now().withNano(0);
            MappedSuperclassUser user = new MappedSuperclassUser("ada");
            user.setCreatedAt(now);
            MappedSuperclassOrder order = new MappedSuperclassOrder(new BigDecimal("12.50"));
            order.setCreatedAt(now);

            em.persist(user);
            em.persist(order);
            em.flush();

            assertNotNull(user.getId(), "id declared in @MappedSuperclass must be generated");
            assertNotNull(order.getId());
            assertEquals(0L, user.getVersion(), "version declared in @MappedSuperclass must initialize");

            EntityManager reader = EntityManager.create(URL);
            try {
                reader.registerEntities(MappedSuperclassUser.class, MappedSuperclassOrder.class);
                MappedSuperclassUser loaded = reader.find(MappedSuperclassUser.class, user.getId());
                assertNotNull(loaded);
                assertEquals("ada", loaded.getName());
                assertEquals(now, loaded.getCreatedAt());
                assertEquals(0L, loaded.getVersion());

                MappedSuperclassOrder loadedOrder = reader.find(MappedSuperclassOrder.class, order.getId());
                assertNotNull(loadedOrder);
                assertEquals(0, new BigDecimal("12.50").compareTo(loadedOrder.getTotal()));
                assertEquals(now, loadedOrder.getCreatedAt());
            } finally {
                reader.close();
            }
        } finally {
            em.close();
        }
    }

    @Test
    void idFieldInMappedSuperclassIsRecognized() {
        EntityMetadata metadata = EntityMetadata.of(MappedSuperclassUser.class);
        assertSame(BaseEntity.class, metadata.getIdField().getDeclaringClass());
        assertEquals("id", metadata.getIdColumnName());
        assertEquals("created_at", metadata.getColumnName(EntityMetadata.findField(BaseEntity.class, "createdAt")));
    }

    @Test
    void unmappedSuperclassWithFieldsFailsAtMetadataBuild() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(ExtendsPlainBase.class));
        assertTrue(ex.getMessage().contains("PlainBase"), ex.getMessage());
        assertTrue(ex.getMessage().contains("ignored"), ex.getMessage());
    }

    @Test
    void entitySuperclassFailsAtMetadataBuild() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(ExtendsEntityBase.class));
        assertTrue(ex.getMessage().contains("entity inheritance"), ex.getMessage());
    }

    @Test
    void fieldlessUnmappedSuperclassIsAllowed() {
        assertDoesNotThrow(() -> EntityMetadata.of(ExtendsMarkerBase.class));
    }

    static class PlainBase {
        private String ignored;
    }

    @Entity
    static class ExtendsPlainBase extends PlainBase {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;
    }

    @Entity
    static class EntityBase {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;
    }

    @Entity
    static class ExtendsEntityBase extends EntityBase {
        @Column(name = "name")
        private String name;
    }

    static class MarkerBase {
        static final String CONSTANT = "constant";
    }

    @Entity
    static class ExtendsMarkerBase extends MarkerBase {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;
    }
}
