package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.HomeCustomJoinColumn;
import de.t14d3.spool.test.entities.PlayerCustomJoinColumn;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class RefreshCascadeCycleTest {
    @Test
    void refreshWithBidirectionalCascadeDoesNotRecurseInfinitely() throws Exception {
        String dbName = "refresh_cycle_" + UUID.randomUUID().toString().replace("-", "");
        String jdbcUrl = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";

        EntityManager em = EntityManager.create(jdbcUrl);
        try {
            em.registerEntities(PlayerCustomJoinColumn.class, HomeCustomJoinColumn.class);
            em.getMigrationManager().updateSchema();

            UUID playerId = UUID.randomUUID();
            PlayerCustomJoinColumn player = new PlayerCustomJoinColumn(playerId, "p1");
            player.addHome(new HomeCustomJoinColumn("h1"));

            em.persist(player);
            em.flush();

            assertDoesNotThrow(() -> em.refresh(player));
            assertNotNull(player.getHomes());
            assertEquals(1, player.getHomes().size());
            assertSame(player, player.getHomes().get(0).getPlayer());
        } finally {
            em.close();
        }
    }
}
