package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.OrderLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompositePrimaryKeyTest {
    private EntityManager em;
    private EntityRepository<OrderLine> repo;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:composite_pk_test;DB_CLOSE_DELAY=-1");
        em.getExecutor().execute("DROP TABLE IF EXISTS order_lines", List.of());
        em.registerEntities(OrderLine.class);
        em.updateSchema();
        repo = new EntityRepository<>(em, OrderLine.class);
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void schemaMarksBothIdColumnsAsPrimaryKey() {
        EntityMetadata metadata = EntityMetadata.of(OrderLine.class);
        assertTrue(metadata.isCompositeId());
        assertEquals(2, metadata.getIdFields().size());
        assertThrows(IllegalStateException.class, metadata::getIdField);

        // Composite PK allows equal first component with distinct second one.
        em.persist(new OrderLine(1L, 1, "A", 1));
        em.persist(new OrderLine(1L, 2, "B", 1));
        em.flush();

        assertEquals(2, repo.count());
    }

    @Test
    void persistAndFindWithMapKey() {
        OrderLine line = new OrderLine(10L, 3, "SKU-1", 5);
        em.persist(line);
        em.flush();

        OrderLine found = em.find(OrderLine.class, Map.of("orderId", 10L, "lineNo", 3));
        assertNotNull(found);
        assertEquals("SKU-1", found.getSku());
        assertEquals(5, found.getQuantity());
    }

    @Test
    void findWithEntityInstanceAndRepository() {
        OrderLine line = new OrderLine(20L, 1, "SKU-2", 2);
        repo.save(line);
        em.flush();

        assertNotNull(em.find(OrderLine.class, line));

        OrderLine byRepo = repo.findById(Map.of("orderId", 20L, "lineNo", 1));
        assertNotNull(byRepo);
        assertEquals("SKU-2", byRepo.getSku());

        assertTrue(repo.existsById(Map.of("orderId", 20L, "lineNo", 1)));
        assertFalse(repo.existsById(Map.of("orderId", 20L, "lineNo", 2)));
    }

    @Test
    void updateUsesAllIdColumnsInWhereClause() {
        OrderLine line = new OrderLine(30L, 1, "OLD", 1);
        repo.save(line);
        em.flush();

        line.setSku("NEW");
        line.setQuantity(9);
        repo.save(line);
        em.flush();

        em.detach(line);
        OrderLine reloaded = em.find(OrderLine.class, Map.of("orderId", 30L, "lineNo", 1));
        assertNotNull(reloaded);
        assertEquals("NEW", reloaded.getSku());
        assertEquals(9, reloaded.getQuantity());
    }

    @Test
    void deleteUsesAllIdColumns() {
        OrderLine first = new OrderLine(40L, 1, "A", 1);
        OrderLine second = new OrderLine(40L, 2, "B", 1);
        repo.save(first);
        repo.save(second);
        em.flush();

        repo.deleteById(Map.of("orderId", 40L, "lineNo", 1));
        em.flush();

        assertNull(em.find(OrderLine.class, Map.of("orderId", 40L, "lineNo", 1)));
        assertNotNull(em.find(OrderLine.class, Map.of("orderId", 40L, "lineNo", 2)));
    }

    @Test
    void refreshUsesCompositeKey() {
        OrderLine line = new OrderLine(50L, 1, "ORIGINAL", 1);
        repo.save(line);
        em.flush();

        line.setSku("CHANGED");
        repo.refresh(line);
        assertEquals("ORIGINAL", line.getSku());
    }

    @Test
    void lockUsesCompositeKey() {
        OrderLine line = new OrderLine(60L, 1, "LOCK", 1);
        repo.save(line);
        em.flush();

        em.transactional(() -> repo.lock(line, de.t14d3.spool.query.LockMode.PESSIMISTIC_WRITE));
    }

    @Test
    void duplicateIdIsRejected() {
        repo.save(new OrderLine(70L, 1, "A", 1));
        em.flush();

        // Clear the identity map so the second persist is routed to INSERT
        // instead of UPDATE for the already-managed row.
        em.clear();
        em.persist(new OrderLine(70L, 1, "B", 1));
        assertThrows(RuntimeException.class, em::flush);
    }

    @Test
    void hqlWorksWithCompositeIdFields() {
        repo.save(new OrderLine(80L, 1, "HQL-A", 1));
        repo.save(new OrderLine(80L, 2, "HQL-B", 1));
        em.flush();

        List<OrderLine> lines = em.createQuery(
                        "SELECT o FROM OrderLine o WHERE o.orderId = :order ORDER BY o.lineNo")
                .setParameter("order", 80L)
                .getResultList();

        assertEquals(2, lines.size());
        assertEquals("HQL-A", lines.get(0).getSku());
        assertEquals("HQL-B", lines.get(1).getSku());
    }
}
