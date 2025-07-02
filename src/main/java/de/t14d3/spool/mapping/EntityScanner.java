package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.Entity;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.Set;

public class EntityScanner {
    /**
     * Scans the classpath for all @Entity types under base package and preloads metadata.
     * Requires 'org.reflections:reflections' on the classpath.
     */
    public static void scan() {
        Reflections reflections = new Reflections(
                new ConfigurationBuilder()
                        .setUrls(ClasspathHelper.forPackage("de.t14d3.spool"))
                        .setScanners(Scanners.TypesAnnotated)
        );
        Set<Class<?>> entities = reflections.getTypesAnnotatedWith(Entity.class);
        for (Class<?> cls : entities) {
            EntityMetadata.of(cls);
        }
    }
}
