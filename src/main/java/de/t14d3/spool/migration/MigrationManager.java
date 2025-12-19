package de.t14d3.spool.migration;

import de.t14d3.spool.query.Dialect;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Manages database migrations including tracking applied migrations,
 * generating new migrations from entity changes, and applying migrations.
 */
public class MigrationManager {
    private static final String MIGRATION_TABLE = "spool_migrations";
    private static final DateTimeFormatter VERSION_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final Connection connection;
    private final SchemaIntrospector introspector;
    private final SqlGenerator sqlGenerator;
    private final Set<Class<?>> entityClasses;

    public MigrationManager(Connection connection) {
        this.connection = connection;
        this.introspector = new SchemaIntrospector(connection);
        this.sqlGenerator = new SqlGenerator(detectDialect());
        this.entityClasses = new LinkedHashSet<>();
    }

    /**
     * Register an entity class for migration management.
     */
    public MigrationManager registerEntity(Class<?> entityClass) {
        entityClasses.add(entityClass);
        return this;
    }

    /**
     * Register multiple entity classes for migration management.
     */
    public MigrationManager registerEntities(Class<?>... entityClasses) {
        Collections.addAll(this.entityClasses, entityClasses);
        return this;
    }

    /**
     * Initialize the migration tracking table.
     */
    public void initialize() throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + MIGRATION_TABLE + " (\n" +
                "  version VARCHAR(20) PRIMARY KEY,\n" +
                "  description VARCHAR(255),\n" +
                "  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
                "  checksum VARCHAR(64)\n" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    /**
     * Get all applied migration versions.
     */
    public Set<String> getAppliedMigrations() throws SQLException {
        Set<String> versions = new LinkedHashSet<>();

        if (!introspector.tableExists(MIGRATION_TABLE)) {
            return versions;
        }

        String sql = "SELECT version FROM " + MIGRATION_TABLE + " ORDER BY version";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                versions.add(rs.getString("version"));
            }
        }
        return versions;
    }

    /**
     * Generate a new migration version string.
     */
    public String generateVersion() {
        return LocalDateTime.now().format(VERSION_FORMAT);
    }

    /**
     * Analyze the current schema and generate migration SQL if needed.
     * Returns the list of SQL statements needed to bring the database up to date.
     */
    public List<String> generateMigrationSql() throws SQLException {
        // Build expected schema from entities
        Map<String, TableDefinition> expectedSchema = new LinkedHashMap<>();
        for (Class<?> entityClass : entityClasses) {
            TableDefinition table = introspector.buildTableDefinitionFromEntity(entityClass);
            expectedSchema.put(table.getName().toLowerCase(), table);
        }
        for (Class<?> entityClass : entityClasses) {
            for (TableDefinition join : introspector.buildJoinTableDefinitionsFromEntity(entityClass)) {
                expectedSchema.putIfAbsent(join.getName().toLowerCase(), join);
            }
        }

        // Get actual schema from database
        Map<String, TableDefinition> actualSchema = new LinkedHashMap<>();
        for (String tableName : expectedSchema.keySet()) {
            TableDefinition table = introspector.getTableDefinition(tableName);
            if (table != null) {
                actualSchema.put(tableName.toLowerCase(), table);
            }
        }

        // Compare and generate diff
        SchemaDiff diff = SchemaDiff.compare(expectedSchema, actualSchema);

        // Generate SQL
        return sqlGenerator.generateSql(diff);
    }

    /**
     * Get the schema diff between entities and database.
     */
    public SchemaDiff getSchemaDiff() throws SQLException {
        Map<String, TableDefinition> expectedSchema = new LinkedHashMap<>();
        for (Class<?> entityClass : entityClasses) {
            TableDefinition table = introspector.buildTableDefinitionFromEntity(entityClass);
            expectedSchema.put(table.getName().toLowerCase(), table);
        }
        for (Class<?> entityClass : entityClasses) {
            for (TableDefinition join : introspector.buildJoinTableDefinitionsFromEntity(entityClass)) {
                expectedSchema.putIfAbsent(join.getName().toLowerCase(), join);
            }
        }

        Map<String, TableDefinition> actualSchema = new LinkedHashMap<>();
        for (String tableName : expectedSchema.keySet()) {
            TableDefinition table = introspector.getTableDefinition(tableName);
            if (table != null) {
                actualSchema.put(tableName.toLowerCase(), table);
            }
        }

        return SchemaDiff.compare(expectedSchema, actualSchema);
    }

    /**
     * Apply all pending migrations automatically.
     * This will generate and execute SQL to bring the database schema in sync with entities.
     *
     * @return The number of SQL statements executed
     */
    public int migrate() throws SQLException {
        initialize();

        List<String> sqlStatements = generateMigrationSql();

        if (sqlStatements.isEmpty()) {
            return 0;
        }

        String version = generateVersion();
        boolean originalAutoCommit = connection.getAutoCommit();

        try {
            connection.setAutoCommit(false);

            // Execute all migration statements
            try (Statement stmt = connection.createStatement()) {
                for (String sql : sqlStatements) {
                    // Skip comment-only statements
                    if (sql.trim().startsWith("--")) {
                        continue;
                    }
                    stmt.execute(sql);
                }
            }

            // Record the migration
            recordMigration(version, "Auto-generated migration", calculateChecksum(sqlStatements));

            connection.commit();
            return sqlStatements.size();

        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException("Migration failed, rolled back: " + e.getMessage(), e);
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    /**
     * Apply migrations with a dry-run option.
     *
     * @param dryRun If true, only returns the SQL without executing
     * @return The list of SQL statements that would be/were executed
     */
    public List<String> migrate(boolean dryRun) throws SQLException {
        List<String> sqlStatements = generateMigrationSql();

        if (!dryRun && !sqlStatements.isEmpty()) {
            migrate();
        }

        return sqlStatements;
    }

    /**
     * Update schema to match entities (alias for migrate).
     * Creates tables and columns as needed.
     */
    public int updateSchema() throws SQLException {
        return migrate();
    }

    /**
     * Validate that the database schema matches the entity definitions.
     *
     * @return true if schema is in sync, false otherwise
     */
    public boolean validateSchema() throws SQLException {
        SchemaDiff diff = getSchemaDiff();
        return !diff.hasChanges();
    }

    /**
     * Get a report of schema differences.
     */
    public String getSchemaReport() throws SQLException {
        SchemaDiff diff = getSchemaDiff();

        if (!diff.hasChanges()) {
            return "Schema is up to date. No changes needed.";
        }

        StringBuilder report = new StringBuilder();
        report.append("Schema changes detected:\n");
        report.append("========================\n\n");

        for (SchemaDiff.SchemaChange change : diff.getChanges()) {
            switch (change.getType()) {
                case CREATE_TABLE:
                    report.append("CREATE TABLE: ").append(change.getTableName()).append("\n");
                    for (ColumnDefinition col : change.getTableDefinition().getColumns().values()) {
                        report.append("  - ").append(col.getName()).append(" (").append(col.getSqlType()).append(")\n");
                    }
                    break;
                case ADD_COLUMN:
                    report.append("ADD COLUMN: ").append(change.getTableName())
                            .append(".").append(change.getColumn().getName())
                            .append(" (").append(change.getColumn().getSqlType()).append(")\n");
                    break;
                case MODIFY_COLUMN:
                    report.append("MODIFY COLUMN: ").append(change.getTableName())
                            .append(".").append(change.getColumn().getName()).append("\n");
                    break;
                case DROP_COLUMN:
                    report.append("DROP COLUMN: ").append(change.getTableName())
                            .append(".").append(change.getColumn().getName()).append("\n");
                    break;
                case DROP_TABLE:
                    report.append("DROP TABLE: ").append(change.getTableName()).append("\n");
                    break;
            }
        }

        report.append("\nGenerated SQL:\n");
        report.append("--------------\n");
        for (String sql : sqlGenerator.generateSql(diff)) {
            report.append(sql).append(";\n\n");
        }

        return report.toString();
    }

    /**
     * Record a migration as applied.
     */
    private void recordMigration(String version, String description, String checksum) throws SQLException {
        String sql = "INSERT INTO " + MIGRATION_TABLE + " (version, description, checksum) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, version);
            stmt.setString(2, description);
            stmt.setString(3, checksum);
            stmt.executeUpdate();
        }
    }

    /**
     * Calculate a simple checksum for migration SQL.
     */
    private String calculateChecksum(List<String> sqlStatements) {
        int hash = 0;
        for (String sql : sqlStatements) {
            hash = 31 * hash + sql.hashCode();
        }
        return Integer.toHexString(hash);
    }

    /**
     * Detect the SQL dialect from the connection.
     */
    private Dialect detectDialect() {
        try {
            String url = connection.getMetaData().getURL();
            return Dialect.detectFromUrl(url);
        } catch (SQLException e) {
            return Dialect.GENERIC;
        }
    }

    /**
     * Get the schema introspector for advanced usage.
     */
    public SchemaIntrospector getIntrospector() {
        return introspector;
    }

    /**
     * Get the SQL generator for advanced usage.
     */
    public SqlGenerator getSqlGenerator() {
        return sqlGenerator;
    }
}
