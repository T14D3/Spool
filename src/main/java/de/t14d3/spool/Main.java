package de.t14d3.spool;

import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.mapping.EntityScanner;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Command‐line interface for the ORM library.
 *
 * Usage:
 *   java -jar spool.jar validate        # scans entities, prints count
 *   java -jar spool.jar generate-schema # scans and prints simple DDL
 */
public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            usage();
            System.exit(1);
        }

        String cmd = args[0].toLowerCase();

        Properties cfg = loadProperties("/spool.properties");

        EntityScanner.scan();

        switch (cmd) {
            case "validate":
                doValidate();
                break;
            case "generate-schema":
                doGenerateSchema();
                break;
            default:
                usage();
                System.exit(1);
        }
    }

    private static void usage() {
        System.out.println("de.t14d3.spool CLI");
        System.out.println("  validate        Scans @Entity classes and reports count");
        System.out.println("  generate-schema Scans @Entity classes and emits simple CREATE TABLE statements");
    }

    private static Properties loadProperties(String resource) {
        Properties cfg = new Properties();
        try (InputStream in = Main.class.getResourceAsStream(resource)) {
            if (in != null) {
                cfg.load(in);
            }
        } catch (Exception e) {
            System.err.println("Warning: could not load " + resource + ": " + e.getMessage());
        }
        return cfg;
    }

    private static void doValidate() {
        // gather all loaded metadata entries
        Set<Class<?>> entities = EntityMetadata.loadedClasses();
        System.out.printf("Found %d @Entity classes:%n", entities.size());
        entities.forEach(c -> System.out.println("  - " + c.getName()));
    }

    private static void doGenerateSchema() {
        Set<Class<?>> entities = EntityMetadata.loadedClasses();
        for (Class<?> cls : entities) {
            EntityMetadata md = EntityMetadata.of(cls);
            String cols =
                    md.getIdColumn() + " BIGINT PRIMARY KEY, " +
                            md.getColumns().stream()
                                    .map(c -> c + " VARCHAR(255)")
                                    .collect(Collectors.joining(", "));
            String ddl = String.format("CREATE TABLE %s (%s);", md.getTableName(), cols);
            System.out.println(ddl);
        }
    }
}
