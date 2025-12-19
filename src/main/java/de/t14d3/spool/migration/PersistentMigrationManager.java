package de.t14d3.spool.migration;

import de.t14d3.spool.query.Dialect;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages persistent migration files similar to Doctrine ORM.
 * Each migration is stored as a separate file with versioning for better
 * version control and team collaboration.
 */
public class PersistentMigrationManager {
    private static final String MIGRATION_TABLE = "spool_migrations";
    private static final DateTimeFormatter VERSION_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final String MIGRATION_DIR = "src/main/resources/migrations";
    private static final String MIGRATION_FILE_SUFFIX = ".sql";
    
    private final Connection connection;
    private final SchemaIntrospector introspector;
    private final SqlGenerator sqlGenerator;
    private final Set<Class<?>> entityClasses;
    private final Path migrationsDirectory;

    public PersistentMigrationManager(Connection connection) {
        this(connection, Paths.get(MIGRATION_DIR));
    }

    public PersistentMigrationManager(Connection connection, Path migrationsDirectory) {
        this.connection = connection;
        this.introspector = new SchemaIntrospector(connection);
        this.sqlGenerator = new SqlGenerator(detectDialect());
        this.entityClasses = new LinkedHashSet<>();
        this.migrationsDirectory = migrationsDirectory;
        
        // Ensure migrations directory exists
        try {
            Files.createDirectories(migrationsDirectory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create migrations directory: " + migrationsDirectory, e);
        }
    }

    /**
     * Register an entity class for migration management.
     */
    public PersistentMigrationManager registerEntity(Class<?> entityClass) {
        entityClasses.add(entityClass);
        return this;
    }

    /**
     * Register multiple entity classes for migration management.
     */
    public PersistentMigrationManager registerEntities(Class<?>... entityClasses) {
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
                "  filename VARCHAR(255),\n" +
                "  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
                "  checksum VARCHAR(64)\n" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    /**
     * Generate a new migration file from current entity schema changes.
     * This creates a persistent migration file that can be version controlled.
     *
     * @param description Description of what this migration does
     * @return The migration file that was created
     */
    public MigrationFile generateMigrationFile(String description) throws SQLException, IOException {
        SchemaDiff diff = getSchemaDiff();
        
        if (!diff.hasChanges()) {
            throw new IllegalStateException("No schema changes detected. No migration needed.");
        }

        String version = generateVersion();
        List<String> sqlStatements = sqlGenerator.generateSql(diff);
        
        MigrationFile migrationFile = new MigrationFile(
            version, 
            description, 
            sqlStatements,
            calculateChecksum(sqlStatements)
        );

        // Write the migration file
        writeMigrationFile(migrationFile);
        
        return migrationFile;
    }

    /**
     * Apply a specific migration file by name.
     */
    public void applyMigration(String filename) throws SQLException, IOException {
        // Initialize migration tracking table if it doesn't exist
        initialize();
        
        MigrationFile migration = loadMigrationFile(filename);
        
        if (isMigrationApplied(migration.getVersion())) {
            System.out.println("Migration " + filename + " already applied, skipping.");
            return;
        }

        boolean originalAutoCommit = connection.getAutoCommit();
        
        try {
            connection.setAutoCommit(false);
            
            // Execute migration SQL
            try (Statement stmt = connection.createStatement()) {
                for (String sql : migration.getSqlStatements()) {
                    // Skip comment-only statements
                    if (sql.trim().startsWith("--")) {
                        continue;
                    }
                    stmt.execute(sql);
                }
            }

            // Record the migration
            recordMigration(migration);
            connection.commit();
            
            System.out.println("Applied migration: " + filename);
            
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException("Migration " + filename + " failed, rolled back: " + e.getMessage(), e);
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    /**
     * Apply all pending migrations (migrations that exist as files but haven't been applied).
     */
    public List<String> applyPendingMigrations() throws SQLException, IOException {
        initialize();
        
        Set<String> appliedMigrations = getAppliedMigrations();
        List<String> migrationFiles = getMigrationFiles();
        List<String> appliedMigrationsList = new ArrayList<>();
        
        for (String filename : migrationFiles) {
            MigrationFile migration = loadMigrationFile(filename);
            
            if (!appliedMigrations.contains(migration.getVersion())) {
                applyMigration(filename);
                appliedMigrationsList.add(filename);
            }
        }
        
        return appliedMigrationsList;
    }

    /**
     * Get all migration files in chronological order.
     */
    public List<String> getMigrationFiles() throws IOException {
        return Files.list(migrationsDirectory)
                .filter(path -> path.toString().endsWith(MIGRATION_FILE_SUFFIX))
                .map(path -> path.getFileName().toString())
                .sorted()
                .collect(Collectors.toList());
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
     * Check if a migration has been applied.
     */
    public boolean isMigrationApplied(String version) throws SQLException {
        if (!introspector.tableExists(MIGRATION_TABLE)) {
            return false;
        }

        String sql = "SELECT COUNT(*) FROM " + MIGRATION_TABLE + " WHERE version = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, version);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    /**
     * Get pending migrations (files that exist but haven't been applied).
     */
    public List<String> getPendingMigrations() throws SQLException, IOException {
        Set<String> appliedMigrations = getAppliedMigrations();
        List<String> migrationFiles = getMigrationFiles();
        List<String> pending = new ArrayList<>();
        
        for (String filename : migrationFiles) {
            MigrationFile migration = loadMigrationFile(filename);
            if (!appliedMigrations.contains(migration.getVersion())) {
                pending.add(filename);
            }
        }
        
        return pending;
    }

    /**
     * Generate a new migration version string.
     */
    public String generateVersion() {
        return LocalDateTime.now().format(VERSION_FORMAT);
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
     * Create a migration file from current entity schema.
     * This is the main entry point for creating persistent migrations.
     *
     * @param description Human-readable description of the migration
     * @return The filename of the created migration
     */
    public String createMigration(String description) throws SQLException, IOException {
        MigrationFile migration = generateMigrationFile(description);
        return migration.getFilename();
    }

    /**
     * Load a migration file from disk.
     */
    public MigrationFile loadMigrationFile(String filename) throws IOException {
        Path filePath = migrationsDirectory.resolve(filename);
        
        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("Migration file not found: " + filename);
        }
        
        String content = Files.readString(filePath);
        return MigrationFile.fromFileContent(filename, content);
    }

    /**
     * Write a migration file to disk.
     */
    private void writeMigrationFile(MigrationFile migration) throws IOException {
        Path filePath = migrationsDirectory.resolve(migration.getFilename());
        String content = migration.toFileContent();
        Files.writeString(filePath, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Record a migration as applied in the database.
     */
    private void recordMigration(MigrationFile migration) throws SQLException {
        String sql = "INSERT INTO " + MIGRATION_TABLE + " (version, description, filename, checksum) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, migration.getVersion());
            stmt.setString(2, migration.getDescription());
            stmt.setString(3, migration.getFilename());
            stmt.setString(4, migration.getChecksum());
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
     * Get migration status report.
     */
    public String getMigrationStatus() throws SQLException, IOException {
        List<String> allMigrations = getMigrationFiles();
        Set<String> appliedMigrations = getAppliedMigrations();
        List<String> pendingMigrations = getPendingMigrations();

        StringBuilder report = new StringBuilder();
        report.append("Migration Status\n");
        report.append("================\n\n");
        report.append("Total migrations: ").append(allMigrations.size()).append("\n");
        report.append("Applied migrations: ").append(appliedMigrations.size()).append("\n");
        report.append("Pending migrations: ").append(pendingMigrations.size()).append("\n\n");

        if (!pendingMigrations.isEmpty()) {
            report.append("Pending migrations:\n");
            report.append("------------------\n");
            for (String migration : pendingMigrations) {
                report.append("  - ").append(migration).append("\n");
            }
        }

        return report.toString();
    }

    /**
     * Get the migrations directory path.
     */
    public Path getMigrationsDirectory() {
        return migrationsDirectory;
    }
}
