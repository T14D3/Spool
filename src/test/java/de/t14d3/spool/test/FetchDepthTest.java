package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.DepthA;
import de.t14d3.spool.test.entities.DepthB;
import de.t14d3.spool.test.entities.DepthC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T3.2: per-query eager-fetch depth. Level 1 resolves the direct eager
 * references, level 2 their references; deeper levels stay id-only stubs.
 */
public class FetchDepthTest {
    private EntityManager em;
    private QueryCountingConnection counting;

    @BeforeEach
    void setup() throws Exception {
        String url = "jdbc:h2:mem:fetch_depth_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";

        EntityManager seed = EntityManager.create(url);
        try {
            seed.registerEntities(DepthA.class, DepthB.class, DepthC.class);
            seed.updateSchema();
            for (int i = 0; i < 5; i++) {
                DepthC c = new DepthC("c" + i);
                seed.persist(c);
                DepthB b = new DepthB("b" + i);
                b.setC(c);
                seed.persist(b);
                DepthA a = new DepthA("a" + i);
                a.setB(b);
                seed.persist(a);
            }
            seed.flush();
        } finally {
            seed.close();
        }

        counting = new QueryCountingConnection(DriverManager.getConnection(url));
        em = EntityManager.create(counting.getProxy());
        em.registerEntities(DepthA.class, DepthB.class, DepthC.class);
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void depthOneLoadsOnlyDirectReferences() {
        em.clear();
        counting.reset();
        List<DepthA> rows = em.createQuery("SELECT a FROM DepthA a").setFetchDepth(1).getResultList();

        assertEquals(5, rows.size());
        for (DepthA a : rows) {
            assertNotNull(a.getB());
            assertNotNull(a.getB().getName(), "level 1 must be hydrated");
            assertNotNull(a.getB().getC(), "level 2 stays an id-only stub");
            assertNull(a.getB().getC().getName());
        }
        assertTrue(counting.getPreparedStatements() <= 2,
                "depth 1 must use one query per level, got " + counting.getPreparedStatements());
    }

    @Test
    void depthTwoLoadsSecondLevel() {
        em.clear();
        counting.reset();
        List<DepthA> rows = em.createQuery("SELECT a FROM DepthA a").setFetchDepth(2).getResultList();

        for (DepthA a : rows) {
            assertNotNull(a.getB().getC().getName(), "level 2 must be hydrated");
        }
        assertTrue(counting.getPreparedStatements() <= 3,
                "depth 2 must use at most two prefetch waves, got " + counting.getPreparedStatements());
    }

    @Test
    void defaultDepthIsUnlimited() {
        em.clear();
        counting.reset();
        List<DepthA> rows = em.createQuery("SELECT a FROM DepthA a").getResultList();

        for (DepthA a : rows) {
            assertNotNull(a.getB().getC().getName());
        }
        assertTrue(counting.getPreparedStatements() <= 3);
    }

    @Test
    void invalidDepthIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> em.createQuery("SELECT a FROM DepthA a").setFetchDepth(0));
    }
}
