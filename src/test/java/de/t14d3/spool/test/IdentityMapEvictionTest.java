package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Author;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T6.1: identity-map eviction. Bulk imports must be able to drop managed
 * entities periodically instead of growing without bound.
 */
public class IdentityMapEvictionTest {
    private static final int ROW_COUNT = 2000;

    private EntityManager openSchema() throws Exception {
        String url = "jdbc:h2:mem:eviction_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        EntityManager em = EntityManager.create(url);
        em.registerEntities(Author.class);
        em.updateSchema();

        try (PreparedStatement ps = em.getExecutor().getConnection()
                .prepareStatement("INSERT INTO authors(name, email) VALUES (?, ?)")) {
            for (int i = 0; i < ROW_COUNT; i++) {
                ps.setString(1, "Author " + i);
                ps.setString(2, "author" + i + "@example.com");
                ps.addBatch();
                if (i % 500 == 499) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        return em;
    }

    private List<Long> authorIds(EntityManager em) throws Exception {
        List<Long> ids = new ArrayList<>(ROW_COUNT);
        try (PreparedStatement ps = em.getExecutor().getConnection()
                .prepareStatement("SELECT id FROM authors ORDER BY id");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
        }
        return ids;
    }

    @Test
    void loadingWithoutEvictionGrowsTheIdentityMap() throws Exception {
        EntityManager em = openSchema();
        try {
            List<Long> ids = authorIds(em);
            assertEquals(ROW_COUNT, ids.size());

            for (int i = 0; i < 300; i++) {
                em.find(Author.class, ids.get(i));
            }
            assertTrue(em.managedEntityCount() >= 300,
                    "every loaded row becomes a managed entity; got " + em.managedEntityCount());

            for (int i = 0; i < 300; i++) {
                assertTrue(em.evict(Author.class, ids.get(i)));
                assertFalse(em.isManaged(Author.class, ids.get(i)));
            }
            assertEquals(0, em.managedEntityCount(), "evict must release identity-map entries");
        } finally {
            em.close();
        }
    }

    @Test
    void periodicEvictionKeepsImportFlat() throws Exception {
        EntityManager em = openSchema();
        try {
            List<Long> ids = authorIds(em);
            int maxManaged = 0;
            for (Long id : ids) {
                Author author = em.find(Author.class, id);
                assertTrue(em.evict(author), "evict(entity) must report the removed entry");
                maxManaged = Math.max(maxManaged, em.managedEntityCount());
            }
            assertEquals(0, em.managedEntityCount());
            assertTrue(maxManaged <= 1,
                    "import with per-row eviction must stay flat, peaked at " + maxManaged + " managed entities");
        } finally {
            em.close();
        }
    }
}
