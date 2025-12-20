package de.t14d3.spool.test;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;
import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.migration.ColumnDefinition;
import de.t14d3.spool.migration.SchemaDiff;
import de.t14d3.spool.migration.SchemaIntrospector;
import de.t14d3.spool.migration.SqlGenerator;
import de.t14d3.spool.migration.TableDefinition;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.test.entities.HomeCustomJoinColumn;
import de.t14d3.spool.test.entities.PlayerCustomJoinColumn;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MySqlDialectVarcharLengthTest {

    @Entity
    @Table(name = "spool_test_mysql_keywords")
    public static class MySqlReservedKeywordEntity {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @Column(name = "range")
        private Double range;

        @Column(name = "is_default")
        private Boolean isDefault;
    }

    @Test
    void mysqlDialectAddsLengthForUuidPrimaryKeyAndForeignKeys() throws Exception {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:test_mysql_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1")) {
            SchemaIntrospector introspector = new SchemaIntrospector(connection);

            TableDefinition players = introspector.buildTableDefinitionFromEntity(PlayerCustomJoinColumn.class);
            assertEquals(Integer.valueOf(36), players.getColumn("uuid").getLength());

            TableDefinition homes = introspector.buildTableDefinitionFromEntity(HomeCustomJoinColumn.class);
            assertEquals(Integer.valueOf(36), homes.getColumn("player_uuid").getLength());
        }
    }

    @Test
    void sqlGeneratorMysqlFallsBackToDefaultVarcharLength() {
        SqlGenerator generator = new SqlGenerator(Dialect.MYSQL);

        TableDefinition table = new TableDefinition("t");
        table.addColumn(new ColumnDefinition.Builder()
                .name("id")
                .sqlType("BIGINT")
                .primaryKey(true)
                .autoIncrement(true)
                .build());
        table.addColumn(new ColumnDefinition.Builder()
                .name("v")
                .sqlType("VARCHAR")
                .nullable(false)
                .build());

        String sql = generator.generateSql(SchemaDiff.SchemaChange.createTable(table));
        assertTrue(sql.contains("`v` VARCHAR(255)"));
    }

    @Test
    void sqlGeneratorMysqlQuotesReservedIdentifiers() {
        SqlGenerator generator = new SqlGenerator(Dialect.MYSQL);

        TableDefinition table = new TableDefinition("where");
        table.addColumn(new ColumnDefinition.Builder()
                .name("id")
                .sqlType("BIGINT")
                .primaryKey(true)
                .autoIncrement(true)
                .build());
        table.addColumn(new ColumnDefinition.Builder()
                .name("range")
                .sqlType("DOUBLE")
                .nullable(false)
                .build());
        table.addColumn(new ColumnDefinition.Builder()
                .name("is_default")
                .sqlType("BOOLEAN")
                .nullable(false)
                .build());

        String sql = generator.generateSql(SchemaDiff.SchemaChange.createTable(table));
        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS `where`"));
        assertTrue(sql.contains("`range` DOUBLE"));
        assertTrue(sql.contains("`is_default` BOOLEAN"));
        assertTrue(sql.contains("PRIMARY KEY (`id`)"));
    }

    @Test
    void mysqlLocalhostRootRootRoot_smokeTest() throws Exception {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL class not found: " + e.getMessage());
            Assumptions.abort("MySQL JDBC driver not on test classpath");
            return;
        }

        String url = "jdbc:mysql://localhost:3306/db" +
                "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" +
                "&connectTimeout=1000&socketTimeout=2000";
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "root");

        Connection connection;
        try {
            connection = DriverManager.getConnection(url, props);
        } catch (Exception e) {
            System.out.println("MySQL connection failed: " + e.getMessage());
            Assumptions.abort("MySQL not available at root@localhost/root (root/root)");
            return;
        }

        try (connection) {
            try (Statement stmt = connection.createStatement()) {
                // best-effort cleanup from previous runs
                stmt.execute("DROP TABLE IF EXISTS `spool_test_mysql_keywords`");
                stmt.execute("DROP TABLE IF EXISTS `spool_migrations`");
            } catch (Exception ignored) {
            }

            EntityManager em = EntityManager.create(connection);
            try {
                em.registerEntities(MySqlReservedKeywordEntity.class);
                assertDoesNotThrow(em::updateSchema);
            } finally {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS `spool_test_mysql_keywords`");
                    stmt.execute("DROP TABLE IF EXISTS `spool_migrations`");
                } catch (Exception ignored) {
                }
                em.close(); // closes the shared connection
            }
        }
    }
}
