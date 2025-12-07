package de.t14d3.spool;

import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.migration.MigrationManager;
import de.t14d3.spool.migration.SchemaIntrospector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * Spool ORM CLI - Command-line interface for schema generation, migration management,
 * and entity validation.
 * 
 * Usage:
 *   java -cp ... de.t14d3.spool.Main [command] [options]
 * 
 * Commands:
 *   validate <entity-class>...     - Validate entity annotations and configuration
 *   schema <entity-class>...       - Generate schema SQL for entities
 *   migrate [options] <entity-class>...  - Generate and apply migrations
 *   inspect [options]              - Inspect database schema
 *   help                          - Show this help message
 * 
 * Options:
 *   --db-url <url>                - Database URL (default: jdbc:h2:mem:test)
 *   --dry-run                     - Show SQL without executing (for migrate command)
 *   --package <package>           - Package to scan for entities
 *   --verbose                     - Verbose output
 */
public class Main {
    private static final String DEFAULT_DB_URL = "jdbc:h2:mem:test";
    
    public static void main(String[] args) {
        if (args.length == 0) {
            printHelp();
            System.exit(1);
        }
        
        String command = args[0].toLowerCase();
        
        try {
            switch (command) {
                case "help":
                case "--help":
                case "-h":
                    printHelp();
                    break;
                case "validate":
                    handleValidate(Arrays.copyOfRange(args, 1, args.length));
                    break;
                case "schema":
                    handleSchema(Arrays.copyOfRange(args, 1, args.length));
                    break;
                case "migrate":
                    handleMigrate(Arrays.copyOfRange(args, 1, args.length));
                    break;
                case "inspect":
                    handleInspect(Arrays.copyOfRange(args, 1, args.length));
                    break;
                default:
                    System.err.println("Unknown command: " + command);
                    System.out.println();
                    printHelp();
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            if (isVerbose(args)) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }
    
    private static void printHelp() {
        System.out.println("Spool ORM CLI - Command-line interface for schema and migration management");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -cp ... de.t14d3.spool.Main [command] [options]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  validate <entity-class>...     - Validate entity annotations and configuration");
        System.out.println("  schema <entity-class>...       - Generate schema SQL for entities");
        System.out.println("  migrate [options] <entity-class>...  - Generate and apply migrations");
        System.out.println("  inspect [options]              - Inspect database schema");
        System.out.println("  help                           - Show this help message");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --db-url <url>                 - Database URL (default: " + DEFAULT_DB_URL + ")");
        System.out.println("  --dry-run                      - Show SQL without executing (for migrate command)");
        System.out.println("  --package <package>            - Package to scan for entities");
        System.out.println("  --verbose                      - Verbose output");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -cp ... de.t14d3.spool.Main validate com.example.User com.example.Post");
        System.out.println("  java -cp ... de.t14d3.spool.Main schema --db-url jdbc:h2:mem:test com.example.User");
        System.out.println("  java -cp ... de.t14d3.spool.Main migrate --dry-run com.example.User com.example.Post");
        System.out.println("  java -cp ... de.t14d3.spool.Main inspect --db-url jdbc:h2:mem:test");
    }
    
    private static void handleValidate(String[] args) {
        if (args.length == 0) {
            System.err.println("Error: No entity classes specified for validation");
            System.err.println("Usage: validate <entity-class>...");
            System.exit(1);
        }
        
        List<String> entityClasses = new ArrayList<>();
        String dbUrl = DEFAULT_DB_URL;
        boolean verbose = false;
        
        // Parse arguments
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--")) {
                switch (args[i]) {
                    case "--db-url":
                        if (i + 1 >= args.length) {
                            System.err.println("Error: --db-url requires a value");
                            System.exit(1);
                        }
                        dbUrl = args[i + 1];
                        i += 2;
                        break;
                    case "--verbose":
                        verbose = true;
                        i++;
                        break;
                    default:
                        System.err.println("Unknown option: " + args[i]);
                        System.exit(1);
                }
            } else {
                entityClasses.add(args[i]);
                i++;
            }
        }
        
        System.out.println("Validating " + entityClasses.size() + " entity class(es)...");
        System.out.println("Database URL: " + dbUrl);
        System.out.println();
        
        int validCount = 0;
        int invalidCount = 0;
        
        for (String className : entityClasses) {
            try {
                Class<?> entityClass = Class.forName(className);
                EntityMetadata metadata = EntityMetadata.of(entityClass);
                
                System.out.println("✓ " + className);
                System.out.println("  Table: " + metadata.getTableName());
                System.out.println("  ID Field: " + metadata.getIdField().getName());
                System.out.println("  Fields: " + metadata.getFields().size());
                
                // Additional validation checks
                List<String> warnings = validateEntity(entityClass, metadata);
                if (!warnings.isEmpty()) {
                    System.out.println("  Warnings:");
                    for (String warning : warnings) {
                        System.out.println("    - " + warning);
                    }
                }
                
                if (verbose) {
                    System.out.println("  Column mappings:");
                    for (var field : metadata.getFields()) {
                        String columnName = metadata.getColumnName(field);
                        System.out.println("    " + field.getName() + " -> " + columnName + " (" + field.getType().getSimpleName() + ")");
                    }
                }
                
                System.out.println();
                validCount++;
                
            } catch (ClassNotFoundException e) {
                System.err.println("✗ " + className + " - Class not found");
                if (verbose) {
                    e.printStackTrace();
                }
                invalidCount++;
            } catch (IllegalArgumentException e) {
                System.err.println("✗ " + className + " - " + e.getMessage());
                if (verbose) {
                    e.printStackTrace();
                }
                invalidCount++;
            } catch (Exception e) {
                System.err.println("✗ " + className + " - Unexpected error: " + e.getMessage());
                if (verbose) {
                    e.printStackTrace();
                }
                invalidCount++;
            }
        }
        
        System.out.println("Validation Summary:");
        System.out.println("  Valid: " + validCount);
        System.out.println("  Invalid: " + invalidCount);
        System.out.println("  Total: " + (validCount + invalidCount));
        
        if (invalidCount > 0) {
            System.exit(1);
        }
    }
    
    private static void handleSchema(String[] args) {
        if (args.length == 0) {
            System.err.println("Error: No entity classes specified for schema generation");
            System.err.println("Usage: schema <entity-class>...");
            System.exit(1);
        }
        
        List<String> entityClasses = new ArrayList<>();
        String dbUrl = DEFAULT_DB_URL;
        boolean verbose = false;
        
        // Parse arguments
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--")) {
                switch (args[i]) {
                    case "--db-url":
                        if (i + 1 >= args.length) {
                            System.err.println("Error: --db-url requires a value");
                            System.exit(1);
                        }
                        dbUrl = args[i + 1];
                        i += 2;
                        break;
                    case "--verbose":
                        verbose = true;
                        i++;
                        break;
                    default:
                        System.err.println("Unknown option: " + args[i]);
                        System.exit(1);
                }
            } else {
                entityClasses.add(args[i]);
                i++;
            }
        }
        
        System.out.println("Generating schema SQL for " + entityClasses.size() + " entity class(es)...");
        System.out.println("Database URL: " + dbUrl);
        System.out.println();
        
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            MigrationManager migrationManager = new MigrationManager(conn);
            
            // Register all entity classes
            for (String className : entityClasses) {
                try {
                    Class<?> entityClass = Class.forName(className);
                    migrationManager.registerEntity(entityClass);
                    System.out.println("✓ Registered: " + className);
                } catch (ClassNotFoundException e) {
                    System.err.println("✗ Class not found: " + className);
                    System.exit(1);
                } catch (Exception e) {
                    System.err.println("✗ Error registering " + className + ": " + e.getMessage());
                    System.exit(1);
                }
            }
            
            System.out.println();
            System.out.println("Generated SQL:");
            System.out.println("==============");
            
            List<String> sqlStatements = migrationManager.generateMigrationSql();
            
            if (sqlStatements.isEmpty()) {
                System.out.println("-- No schema changes needed - entities match current database schema");
            } else {
                for (String sql : sqlStatements) {
                    if (!sql.trim().startsWith("--")) {
                        System.out.println(sql + ";");
                    } else {
                        System.out.println(sql);
                    }
                }
            }
            
            if (verbose) {
                System.out.println();
                System.out.println("Schema Report:");
                System.out.println("==============");
                System.out.println(migrationManager.getSchemaReport());
            }
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }
    
    private static void handleMigrate(String[] args) {
        List<String> entityClasses = new ArrayList<>();
        String dbUrl = DEFAULT_DB_URL;
        boolean dryRun = false;
        boolean verbose = false;
        
        // Parse arguments
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--")) {
                switch (args[i]) {
                    case "--db-url":
                        if (i + 1 >= args.length) {
                            System.err.println("Error: --db-url requires a value");
                            System.exit(1);
                        }
                        dbUrl = args[i + 1];
                        i += 2;
                        break;
                    case "--dry-run":
                        dryRun = true;
                        i++;
                        break;
                    case "--verbose":
                        verbose = true;
                        i++;
                        break;
                    default:
                        System.err.println("Unknown option: " + args[i]);
                        System.exit(1);
                }
            } else {
                entityClasses.add(args[i]);
                i++;
            }
        }
        
        if (entityClasses.isEmpty()) {
            System.err.println("Error: No entity classes specified for migration");
            System.err.println("Usage: migrate [options] <entity-class>...");
            System.exit(1);
        }
        
        System.out.println("Migration Manager");
        System.out.println("=================");
        System.out.println("Database URL: " + dbUrl);
        System.out.println("Entity classes: " + String.join(", ", entityClasses));
        System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be applied)" : "LIVE RUN"));
        System.out.println();
        
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            MigrationManager migrationManager = new MigrationManager(conn);
            
            // Register all entity classes
            for (String className : entityClasses) {
                try {
                    Class<?> entityClass = Class.forName(className);
                    migrationManager.registerEntity(entityClass);
                    System.out.println("✓ Registered: " + className);
                } catch (ClassNotFoundException e) {
                    System.err.println("✗ Class not found: " + className);
                    System.exit(1);
                } catch (Exception e) {
                    System.err.println("✗ Error registering " + className + ": " + e.getMessage());
                    System.exit(1);
                }
            }
            
            System.out.println();
            
            // Check current status
            Set<String> appliedMigrations = migrationManager.getAppliedMigrations();
            System.out.println("Applied migrations: " + appliedMigrations.size());
            
            if (!appliedMigrations.isEmpty() && verbose) {
                System.out.println("Migration versions:");
                for (String version : appliedMigrations) {
                    System.out.println("  - " + version);
                }
            }
            
            System.out.println();
            
            // Generate migration SQL
            List<String> sqlStatements = migrationManager.generateMigrationSql();
            
            if (sqlStatements.isEmpty()) {
                System.out.println("✓ Schema is up to date - no migrations needed");
                return;
            }
            
            System.out.println("Pending changes: " + sqlStatements.size() + " SQL statement(s)");
            System.out.println();
            
            if (dryRun) {
                System.out.println("Migration SQL (DRY RUN):");
                System.out.println("========================");
                for (String sql : sqlStatements) {
                    if (!sql.trim().startsWith("--")) {
                        System.out.println(sql + ";");
                    } else {
                        System.out.println(sql);
                    }
                }
                
                System.out.println();
                System.out.println("Schema Report:");
                System.out.println("==============");
                System.out.println(migrationManager.getSchemaReport());
                
            } else {
                System.out.println("Applying migrations...");
                System.out.println();
                
                try {
                    int executedStatements = migrationManager.migrate();
                    System.out.println("✓ Migration completed successfully");
                    System.out.println("  Executed " + executedStatements + " SQL statement(s)");
                    System.out.println("  Database schema is now synchronized with entities");
                } catch (SQLException e) {
                    System.err.println("✗ Migration failed: " + e.getMessage());
                    if (verbose) {
                        e.printStackTrace();
                    }
                    System.exit(1);
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }
    
    private static void handleInspect(String[] args) {
        String dbUrl = DEFAULT_DB_URL;
        boolean verbose = false;
        
        // Parse arguments
        int i = 0;
        while (i < args.length) {
            if (args[i].startsWith("--")) {
                switch (args[i]) {
                    case "--db-url":
                        if (i + 1 >= args.length) {
                            System.err.println("Error: --db-url requires a value");
                            System.exit(1);
                        }
                        dbUrl = args[i + 1];
                        i += 2;
                        break;
                    case "--verbose":
                        verbose = true;
                        i++;
                        break;
                    default:
                        System.err.println("Unknown option: " + args[i]);
                        System.exit(1);
                }
            } else {
                System.err.println("Unknown argument: " + args[i]);
                System.exit(1);
            }
        }
        
        System.out.println("Database Schema Inspector");
        System.out.println("========================");
        System.out.println("Database URL: " + dbUrl);
        System.out.println();
        
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            SchemaIntrospector introspector = new SchemaIntrospector(conn);
            
            // Get all table names
            Set<String> tableNames = introspector.getTableNames();
            
            if (tableNames.isEmpty()) {
                System.out.println("No user tables found in database");
                return;
            }
            
            System.out.println("Found " + tableNames.size() + " table(s):");
            
            for (String tableName : tableNames) {
                System.out.println();
                System.out.println("Table: " + tableName);
                System.out.println("-------" + "-".repeat(tableName.length()));
                
                try {
                    var tableDef = introspector.getTableDefinition(tableName);
                    if (tableDef == null) {
                        System.out.println("  (Could not read table definition)");
                        continue;
                    }
                    
                    var columns = tableDef.getColumns();
                    System.out.println("  Columns: " + columns.size());
                    
                    for (var column : columns.values()) {
                        StringBuilder colInfo = new StringBuilder();
                        colInfo.append("    ").append(column.getName())
                               .append(" (").append(column.getSqlType());
                        
                        if (column.getLength() != null) {
                            colInfo.append("(").append(column.getLength()).append(")");
                        }
                        
                        colInfo.append(")");
                        
                        if (column.isPrimaryKey()) {
                            colInfo.append(" [PRIMARY KEY]");
                        }
                        
                        if (!column.isNullable()) {
                            colInfo.append(" [NOT NULL]");
                        }
                        
                        if (column.isAutoIncrement()) {
                            colInfo.append(" [AUTO_INCREMENT]");
                        }
                        
                        if (column.getDefaultValue() != null) {
                            colInfo.append(" [DEFAULT: ").append(column.getDefaultValue()).append("]");
                        }
                        
                        System.out.println(colInfo.toString());
                    }
                    
                    if (verbose) {
                        System.out.println();
                        System.out.println("  Full Definition:");
                        System.out.println("  " + tableDef.toString().replace("\n", "\n  "));
                    }
                    
                } catch (SQLException e) {
                    System.out.println("  (Error reading table: " + e.getMessage() + ")");
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }
    
    private static List<String> validateEntity(Class<?> entityClass, EntityMetadata metadata) {
        List<String> warnings = new ArrayList<>();
        
        // Check for reasonable table name
        String tableName = metadata.getTableName();
        if (tableName.equals(entityClass.getSimpleName())) {
            warnings.add("Table name is same as class name - consider using @Table annotation for custom naming");
        }
        
        // Check for ID field type
        Class<?> idType = metadata.getIdField().getType();
        if (!(idType == Long.class || idType == long.class || 
              idType == Integer.class || idType == int.class)) {
            warnings.add("ID field type " + idType.getSimpleName() + " - consider using Long or Integer for better compatibility");
        }
        
        // Check for reasonable field count
        int fieldCount = metadata.getFields().size();
        if (fieldCount == 0) {
            warnings.add("Entity has no mapped fields - check your @Column and relationship annotations");
        } else if (fieldCount > 50) {
            warnings.add("Entity has " + fieldCount + " fields - consider breaking into smaller entities");
        }
        
        return warnings;
    }
    private static boolean isVerbose(String[] args) {
        for (String arg : args) {
            if ("--verbose".equals(arg)) {
                return true;
            }
        }
        return false;
    }
}
