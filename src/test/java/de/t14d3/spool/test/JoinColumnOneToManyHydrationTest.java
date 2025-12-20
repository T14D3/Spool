package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.HomeCustomJoinColumn;
import de.t14d3.spool.test.entities.PlayerCustomJoinColumn;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class JoinColumnOneToManyHydrationTest {

    @Test
    void testOneToManyHydrationUsesCustomJoinColumnName() throws Exception {
        String dbName = "joincol_" + UUID.randomUUID().toString().replace("-", "");
        String jdbcUrl = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";

        EntityManager em = EntityManager.create(jdbcUrl);
        try {
            em.registerEntities(PlayerCustomJoinColumn.class, HomeCustomJoinColumn.class);
            em.getMigrationManager().updateSchema();

            UUID playerId = UUID.randomUUID();
            PlayerCustomJoinColumn player = new PlayerCustomJoinColumn(playerId, "p1");
            player.addHome(new HomeCustomJoinColumn("h1"));
            player.addHome(new HomeCustomJoinColumn("h2"));

            em.persist(player);
            em.flush();

            // Ensure FK column exists with the custom name and rows are linked.
            assertEquals(2, countHomesByPlayerUuid(em, playerId));

            // Fresh EM to ensure hydration comes from DB.
            EntityManager em2 = EntityManager.create(jdbcUrl);
            try {
                em2.registerEntities(PlayerCustomJoinColumn.class, HomeCustomJoinColumn.class);
                em2.getMigrationManager().updateSchema();

                PlayerCustomJoinColumn loaded = em2.find(PlayerCustomJoinColumn.class, playerId);
                assertNotNull(loaded);
                assertNotNull(loaded.getHomes());
                assertEquals(2, loaded.getHomes().size());
                for (HomeCustomJoinColumn h : loaded.getHomes()) {
                    assertNotNull(h.getPlayer());
                    assertEquals(playerId, h.getPlayer().getUuid());
                }
            } finally {
                em2.close();
            }
        } finally {
            em.close();
        }
    }

    private int countHomesByPlayerUuid(EntityManager em, UUID uuid) throws Exception {
        String sql = "SELECT COUNT(*) AS c FROM homes_cjc WHERE player_uuid = ?";
        try (PreparedStatement ps = em.getExecutor().getConnection().prepareStatement(sql)) {
            ps.setObject(1, uuid);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt("c");
            }
        }
    }
}

