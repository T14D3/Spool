package de.t14d3.spool.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T4.1: the opt-in annotation processor validates mappings at compile time and
 * emits a GraalVM reflect-config.json. It never generates code or changes
 * runtime behavior.
 */
public class AnnotationProcessorTest {

    @TempDir
    Path outputDir;

    @Test
    void entityCorpusCompilesAndEmitsReflectConfig() throws Exception {
        Path entitiesDir = Path.of("src", "test", "java", "de", "t14d3", "spool", "test", "entities");
        assertTrue(Files.isDirectory(entitiesDir), "entity source directory must exist");
        List<Path> sources;
        try (var stream = Files.list(entitiesDir)) {
            sources = stream.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList());
        }
        assertFalse(sources.isEmpty());

        Result result = compileFiles(sources);
        assertTrue(result.success, () -> "entity corpus must pass validation:\n" + result.report());

        Path reflectConfig = outputDir.resolve(
                "META-INF/native-image/de.t14d3/spool/reflect-config.json");
        assertTrue(Files.exists(reflectConfig), "processor must emit GraalVM reflect-config.json");
        String json = Files.readString(reflectConfig);
        assertTrue(json.contains("\"de.t14d3.spool.test.entities.Author\""), json);
        assertTrue(json.contains("\"allDeclaredConstructors\": true"), json);
    }

    @Test
    void missingIdFailsCompilation() {
        Result result = compileSource("fixtures.NoId", """
                package fixtures;
                import de.t14d3.spool.annotations.Entity;
                import de.t14d3.spool.annotations.Column;

                @Entity
                public class NoId {
                    @Column(name = "name") private String name;
                }
                """);
        assertFalse(result.success);
        assertTrue(result.report().contains("must have a field annotated with @Id"), result.report());
    }

    @Test
    void invalidMappedByFailsCompilation() {
        Result result = compileSource("fixtures.BadMappedBy", """
                package fixtures;
                import de.t14d3.spool.annotations.Entity;
                import de.t14d3.spool.annotations.Id;
                import de.t14d3.spool.annotations.Column;
                import de.t14d3.spool.annotations.OneToMany;
                import java.util.ArrayList;
                import java.util.List;

                @Entity
                public class BadMappedBy {
                    @Id(autoIncrement = true) @Column(name = "id") private Long id;

                    @OneToMany(targetEntity = Target.class, mappedBy = "missing")
                    private List<Target> children = new ArrayList<>();
                }

                @Entity
                class Target {
                    @Id(autoIncrement = true) @Column(name = "id") private Long id;
                }
                """);
        assertFalse(result.success);
        assertTrue(result.report().contains("has no such field"), result.report());
    }

    @Test
    void blankMappedByWithoutJoinColumnFailsCompilation() {
        Result result = compileSource("fixtures.BlankMappedBy", """
                package fixtures;
                import de.t14d3.spool.annotations.Entity;
                import de.t14d3.spool.annotations.Id;
                import de.t14d3.spool.annotations.Column;
                import de.t14d3.spool.annotations.OneToMany;
                import java.util.ArrayList;
                import java.util.List;

                @Entity
                public class BlankMappedBy {
                    @Id(autoIncrement = true) @Column(name = "id") private Long id;

                    @OneToMany(targetEntity = Item.class, mappedBy = "")
                    private List<Item> items = new ArrayList<>();
                }

                @Entity
                class Item {
                    @Id(autoIncrement = true) @Column(name = "id") private Long id;
                }
                """);
        assertFalse(result.success);
        assertTrue(result.report().contains("@JoinColumn"), result.report());
    }

    @Test
    void entityInheritanceFailsCompilation() {
        Result result = compileSource("fixtures.SubEntity", """
                package fixtures;
                import de.t14d3.spool.annotations.Entity;
                import de.t14d3.spool.annotations.Id;
                import de.t14d3.spool.annotations.Column;

                @Entity
                public class SubEntity extends BaseEntity {
                    @Column(name = "name") private String name;
                }

                @Entity
                class BaseEntity {
                    @Id(autoIncrement = true) @Column(name = "id") private Long id;
                }
                """);
        assertFalse(result.success);
        assertTrue(result.report().contains("entity inheritance is not supported"), result.report());
    }

    // ------------------------------------------------------------------

    private record Result(boolean success, DiagnosticCollector<JavaFileObject> diagnostics) {
        String report() {
            return diagnostics.getDiagnostics().stream()
                    .map(diagnostic -> diagnostic.getKind() + ": " + diagnostic.getMessage(Locale.ROOT))
                    .collect(Collectors.joining("\n"));
        }
    }

    private Result compileFiles(List<Path> sources) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager =
                     compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromPaths(sources);
            return run(compiler, fileManager, diagnostics, units);
        }
    }

    private Result compileSource(String className, String source) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        String binaryName = className.replace('.', '/');
        JavaFileObject unit = new SimpleJavaFileObject(
                URI.create("string:///" + binaryName + ".java"), JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };
        try (StandardJavaFileManager fileManager =
                     compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            return run(compiler, fileManager, diagnostics, List.of(unit));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Result run(JavaCompiler compiler, StandardJavaFileManager fileManager,
                       DiagnosticCollector<JavaFileObject> diagnostics,
                       Iterable<? extends JavaFileObject> units) {
        List<String> options = List.of(
                "-classpath", System.getProperty("java.class.path"),
                "-d", outputDir.toString(),
                "-processor", "de.t14d3.spool.processor.SpoolValidator");
        Boolean success = compiler.getTask(null, fileManager, diagnostics, options, null, units).call();
        return new Result(Boolean.TRUE.equals(success), diagnostics);
    }
}
