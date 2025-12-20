package de.t14d3.spool.test;

import de.t14d3.spool.migration.ColumnDefinition;
import de.t14d3.spool.migration.SchemaDiff;
import de.t14d3.spool.migration.SchemaIntrospector;
import de.t14d3.spool.migration.SqlGenerator;
import de.t14d3.spool.migration.TableDefinition;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.test.entities.HomeCustomJoinColumn;
import de.t14d3.spool.test.entities.PlayerCustomJoinColumn;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MySqlDialectVarcharLengthTest {

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
        assertTrue(sql.contains("v VARCHAR(255)"));
    }
}
