package de.t14d3.spool.test;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.OneToOne;
import de.t14d3.spool.mapping.EntityMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the explicitness contract for relationship declarations: a mapping
 * Spool cannot honor fails when the metadata is built, naming the offending
 * field, instead of being silently dropped at query time.
 */
public class MappingValidationTest {

    @Test
    void blankMappedByOneToManyFailsAtMetadataBuild() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(BlankMappedByOwner.class));
        assertTrue(ex.getMessage().contains("BlankMappedByOwner"), ex.getMessage());
        assertTrue(ex.getMessage().contains("children"), ex.getMessage());
        assertTrue(ex.getMessage().contains("mappedBy"), ex.getMessage());
    }

    @Test
    void nonCollectionOneToManyFieldFailsAtMetadataBuild() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(NonCollectionOneToMany.class));
        assertTrue(ex.getMessage().contains("child"), ex.getMessage());
        assertTrue(ex.getMessage().contains("Collection"), ex.getMessage());
    }

    @Test
    void oneToManyMappedByMustExistOnTarget() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(MissingMappedByOwner.class));
        assertTrue(ex.getMessage().contains("children"), ex.getMessage());
        assertTrue(ex.getMessage().contains("owner"), ex.getMessage());
    }

    @Test
    void oneToManyMappedByMustReferenceManyToOne() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(NonManyToOneMappedByOwner.class));
        assertTrue(ex.getMessage().contains("children"), ex.getMessage());
        assertTrue(ex.getMessage().contains("@ManyToOne"), ex.getMessage());
    }

    @Test
    void rawManyToManyCollectionFailsAtMetadataBuild() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(RawManyToManyOwner.class));
        assertTrue(ex.getMessage().contains("tags"), ex.getMessage());
        assertTrue(ex.getMessage().contains("element type"), ex.getMessage());
    }

    @Test
    void manyToManyMappedByMustExistOnTarget() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(MissingManyToManyMappedByOwner.class));
        assertTrue(ex.getMessage().contains("tags"), ex.getMessage());
    }

    @Test
    void oneToOneFailsAtMetadataBuild() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EntityMetadata.of(OneToOneOwner.class));
        assertTrue(ex.getMessage().contains("target"), ex.getMessage());
        assertTrue(ex.getMessage().contains("@OneToOne"), ex.getMessage());
    }

    @Entity
    static class Target {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;
    }

    @Entity
    static class BlankMappedByOwner {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @OneToMany(targetEntity = Target.class, mappedBy = "")
        private List<Target> children = new ArrayList<>();
    }

    @Entity
    static class NonCollectionOneToMany {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @OneToMany(targetEntity = Target.class, mappedBy = "owner")
        private String child;
    }

    @Entity
    static class MissingMappedByOwner {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @OneToMany(targetEntity = Target.class, mappedBy = "owner")
        private List<Target> children = new ArrayList<>();
    }

    @Entity
    static class PlainMappedByTarget {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @Column(name = "owner")
        private String owner;
    }

    @Entity
    static class NonManyToOneMappedByOwner {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @OneToMany(targetEntity = PlainMappedByTarget.class, mappedBy = "owner")
        private List<PlainMappedByTarget> children = new ArrayList<>();
    }

    @Entity
    static class RawManyToManyOwner {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @SuppressWarnings("rawtypes")
        @ManyToMany
        private List tags = new ArrayList();
    }

    @Entity
    static class MissingManyToManyMappedByOwner {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @ManyToMany(mappedBy = "missing")
        private List<Target> tags = new ArrayList<>();
    }

    @Entity
    static class OneToOneOwner {
        @Id(autoIncrement = true)
        @Column(name = "id")
        private Long id;

        @OneToOne
        private Target target;
    }
}
