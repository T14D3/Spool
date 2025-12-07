package de.t14d3.spool.migration;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

/**
 * Represents a persistent migration file with metadata.
 * Each migration contains version, description, SQL statements, and checksum.
 */
public class MigrationFile {
    private static final DateTimeFormatter VERSION_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final String HEADER_PREFIX = "-- Migration: ";
    private static final String DESCRIPTION_PREFIX = "-- Description: ";
    private static final String VERSION_PREFIX = "-- Version: ";
    private static final String TIMESTAMP_PREFIX = "-- Timestamp: ";
    private static final String CHECKSUM_PREFIX = "-- Checksum: ";
    private static final String SQL_DELIMITER = ";\n";
    
    private final String version;
    private final String description;
    private final List<String> sqlStatements;
    private final String checksum;
    private final LocalDateTime timestamp;
    private final String filename;

    public MigrationFile(String version, String description, List<String> sqlStatements, String checksum) {
        this.version = version;
        this.description = description;
        this.sqlStatements = sqlStatements;
        this.checksum = checksum;
        this.timestamp = LocalDateTime.now();
        this.filename = generateFilename();
    }

    public MigrationFile(String version, String description, List<String> sqlStatements, String checksum, LocalDateTime timestamp) {
        this.version = version;
        this.description = description;
        this.sqlStatements = sqlStatements;
        this.checksum = checksum;
        this.timestamp = timestamp;
        this.filename = generateFilename();
    }

    /**
     * Create a MigrationFile from file content.
     */
    public static MigrationFile fromFileContent(String filename, String content) {
        String[] lines = content.split("\n");
        
        String version = null;
        String description = null;
        String timestampStr = null;
        String checksum = null;
        StringBuilder sqlBuilder = new StringBuilder();
        boolean inSqlSection = false;

        for (String line : lines) {
            String trimmedLine = line.trim();
            
            if (trimmedLine.startsWith(VERSION_PREFIX)) {
                version = trimmedLine.substring(VERSION_PREFIX.length()).trim();
            } else if (trimmedLine.startsWith(DESCRIPTION_PREFIX)) {
                description = trimmedLine.substring(DESCRIPTION_PREFIX.length()).trim();
            } else if (trimmedLine.startsWith(TIMESTAMP_PREFIX)) {
                timestampStr = trimmedLine.substring(TIMESTAMP_PREFIX.length()).trim();
            } else if (trimmedLine.startsWith(CHECKSUM_PREFIX)) {
                checksum = trimmedLine.substring(CHECKSUM_PREFIX.length()).trim();
            } else if (trimmedLine.startsWith("--") || trimmedLine.isEmpty()) {
                // Skip comments and empty lines in metadata section
                continue;
            } else {
                // This is SQL content
                inSqlSection = true;
                if (!sqlBuilder.isEmpty()) {
                    sqlBuilder.append("\n");
                }
                sqlBuilder.append(line);
            }
        }

        if (version == null || description == null) {
            throw new IllegalArgumentException("Invalid migration file format: missing version or description");
        }

        List<String> sqlStatements = parseSqlStatements(sqlBuilder.toString());
        
        LocalDateTime timestamp = timestampStr != null ? 
            LocalDateTime.parse(timestampStr, VERSION_FORMAT) : LocalDateTime.now();

        MigrationFile migration = new MigrationFile(version, description, sqlStatements, checksum, timestamp);
        return migration;
    }

    /**
     * Convert the migration to file content format.
     */
    public String toFileContent() {
        StringBuilder content = new StringBuilder();
        
        // Header
        content.append(HEADER_PREFIX).append(getFilename()).append("\n");
        content.append(DESCRIPTION_PREFIX).append(description).append("\n");
        content.append(VERSION_PREFIX).append(version).append("\n");
        content.append(TIMESTAMP_PREFIX).append(timestamp.format(VERSION_FORMAT)).append("\n");
        content.append(CHECKSUM_PREFIX).append(checksum).append("\n");
        content.append("\n");
        
        // SQL statements
        for (String sql : sqlStatements) {
            content.append(sql);
            if (!sql.trim().endsWith(";")) {
                content.append(";");
            }
            content.append("\n\n");
        }
        
        return content.toString();
    }

    /**
     * Generate the filename for this migration.
     */
    private String generateFilename() {
        return version + "_" + description.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase() + ".sql";
    }

    /**
     * Parse SQL statements from content.
     */
    private static List<String> parseSqlStatements(String sqlContent) {
        if (sqlContent.trim().isEmpty()) {
            return List.of();
        }
        
        // Split by semicolon, but handle statements that contain semicolons in quotes
        String[] parts = sqlContent.split(";");
        List<String> statements = new java.util.ArrayList<>();
        
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("--")) {
                statements.add(trimmed);
            }
        }
        
        return statements;
    }

    /**
     * Get the migration version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Get the migration description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the SQL statements.
     */
    public List<String> getSqlStatements() {
        return sqlStatements;
    }

    /**
     * Get the checksum.
     */
    public String getChecksum() {
        return checksum;
    }

    /**
     * Get the timestamp.
     */
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * Get the filename.
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Get a summary of the migration.
     */
    public String getSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append("Migration: ").append(filename).append("\n");
        summary.append("Version: ").append(version).append("\n");
        summary.append("Description: ").append(description).append("\n");
        summary.append("Timestamp: ").append(timestamp.format(VERSION_FORMAT)).append("\n");
        summary.append("SQL Statements: ").append(sqlStatements.size()).append("\n");
        
        for (int i = 0; i < sqlStatements.size(); i++) {
            String sql = sqlStatements.get(i);
            summary.append("  ").append(i + 1).append(". ").append(getSqlType(sql)).append("\n");
        }
        
        return summary.toString();
    }

    /**
     * Get a simple description of what the SQL does.
     */
    private String getSqlType(String sql) {
        String upperSql = sql.trim().toUpperCase();
        if (upperSql.startsWith("CREATE TABLE")) {
            return "CREATE TABLE";
        } else if (upperSql.startsWith("ALTER TABLE")) {
            if (upperSql.contains("ADD")) {
                return "ADD COLUMN";
            } else if (upperSql.contains("MODIFY") || upperSql.contains("ALTER")) {
                return "MODIFY COLUMN";
            } else if (upperSql.contains("DROP")) {
                return "DROP COLUMN";
            }
            return "ALTER TABLE";
        } else if (upperSql.startsWith("DROP TABLE")) {
            return "DROP TABLE";
        } else if (upperSql.startsWith("CREATE INDEX")) {
            return "CREATE INDEX";
        } else if (upperSql.startsWith("DROP INDEX")) {
            return "DROP INDEX";
        }
        return "SQL Statement";
    }

    @Override
    public String toString() {
        return "MigrationFile{" +
                "version='" + version + '\'' +
                ", description='" + description + '\'' +
                ", filename='" + filename + '\'' +
                ", sqlStatements=" + sqlStatements.size() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationFile that = (MigrationFile) o;
        return Objects.equals(version, that.version) && 
               Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, filename);
    }
}
