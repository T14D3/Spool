package de.t14d3.spool.core;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.mapping.RelationshipMapping;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Manages entity relationships and cascading operations.
 */
@SuppressWarnings("ClassCanBeRecord")
public class RelationshipManager {

    private final EntityManager entityManager;

    public RelationshipManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    /**
     * Handles cascade persist operations for an entity.
     */
    public void cascadePersist(Object entity, EntityMetadata metadata) {
        List<RelationshipMapping> relationships = getRelationships(metadata);

        for (RelationshipMapping relationship : relationships) {
            if (relationship.isCascadable(CascadeType.PERSIST) ||
                    relationship.isCascadable(CascadeType.ALL)) {

                Object relatedEntities = metadata.getFieldValue(entity, relationship.field());
                persistRelatedEntities(relatedEntities, relationship);
            }
        }
    }

    /**
     * Handles cascade remove operations for an entity.
     */
    public void cascadeRemove(Object entity, EntityMetadata metadata) {
        List<RelationshipMapping> relationships = getRelationships(metadata);

        for (RelationshipMapping relationship : relationships) {
            if (relationship.isCascadable(CascadeType.REMOVE) ||
                    relationship.isCascadable(CascadeType.ALL)) {

                Object relatedEntities = metadata.getFieldValue(entity, relationship.field());
                removeRelatedEntities(relatedEntities, relationship);
            }
        }
    }

    /**
     * Processes related entities for cascade persist.
     */
    private void persistRelatedEntities(Object relatedEntities, RelationshipMapping relationship) {
        if (relatedEntities == null) {
            return;
        }

        switch (relationship.relationshipType()) {
            case ONE_TO_ONE, MANY_TO_ONE:
                entityManager.persistCascade(relatedEntities);
                break;

            case ONE_TO_MANY, MANY_TO_MANY:
                if (relatedEntities instanceof Collection<?> collection) {
                    for (Object item : collection) {
                        if (item != null) {
                            entityManager.persistCascade(item);
                        }
                    }
                }
                break;
        }
    }

    /**
     * Handles cascade detach operations for an entity.
     */
    public void cascadeDetach(Object entity, EntityMetadata metadata) {
        List<RelationshipMapping> relationships = getRelationships(metadata);

        for (RelationshipMapping relationship : relationships) {
            if (relationship.isCascadable(CascadeType.DETACH) ||
                    relationship.isCascadable(CascadeType.ALL)) {

                Object relatedEntities = metadata.getFieldValue(entity, relationship.field());
                detachRelatedEntities(relatedEntities, relationship);
            }
        }
    }

    /**
     * Handles cascade refresh operations for an entity.
     */
    public void cascadeRefresh(Object entity, EntityMetadata metadata) {
        List<RelationshipMapping> relationships = getRelationships(metadata);

        for (RelationshipMapping relationship : relationships) {
            if (relationship.isCascadable(CascadeType.REFRESH) ||
                    relationship.isCascadable(CascadeType.ALL)) {

                Object relatedEntities = metadata.getFieldValue(entity, relationship.field());
                refreshRelatedEntities(relatedEntities, relationship);
            }
        }
    }

    /**
     * Handles cascade merge operations for an entity.
     */
    public void cascadeMerge(Object entity, EntityMetadata metadata) {
        List<RelationshipMapping> relationships = getRelationships(metadata);

        for (RelationshipMapping relationship : relationships) {
            if (relationship.isCascadable(CascadeType.MERGE) ||
                    relationship.isCascadable(CascadeType.ALL)) {

                Object relatedEntities = metadata.getFieldValue(entity, relationship.field());
                Object mergedRelatedEntities = mergeRelatedEntities(relatedEntities, relationship);

                // Update the field with merged entities if they changed
                if (mergedRelatedEntities != relatedEntities) {
                    metadata.setFieldValue(entity, relationship.field(), mergedRelatedEntities);
                }
            }
        }
    }

    /**
     * Processes related entities for cascade remove.
     */
    private void removeRelatedEntities(Object relatedEntities, RelationshipMapping relationship) {
        if (relatedEntities == null) {
            return;
        }

        switch (relationship.relationshipType()) {
            case ONE_TO_ONE:
                entityManager.remove(relatedEntities);
                break;

            case ONE_TO_MANY, MANY_TO_MANY:
                if (relatedEntities instanceof Collection) {
                    // Create a copy to avoid concurrent modification
                    List<Object> toRemove = new ArrayList<>((Collection<?>) relatedEntities);
                    for (Object item : toRemove) {
                        if (item != null) {
                            entityManager.remove(item);
                        }
                    }
                }
                break;

            case MANY_TO_ONE:
                // Not cascaded for remove operations by default
                break;

        }
    }

    /**
     * Processes related entities for cascade detach.
     */
    private void detachRelatedEntities(Object relatedEntities, RelationshipMapping relationship) {
        if (relatedEntities == null) {
            return;
        }

        switch (relationship.relationshipType()) {
            case ONE_TO_ONE, MANY_TO_ONE:
                entityManager.detach(relatedEntities);
                break;

            case ONE_TO_MANY, MANY_TO_MANY:
                if (relatedEntities instanceof Collection) {
                    for (Object item : (Collection<?>) relatedEntities) {
                        if (item != null) {
                            entityManager.detach(item);
                        }
                    }
                }
                break;
        }
    }

    /**
     * Processes related entities for cascade refresh.
     */
    private void refreshRelatedEntities(Object relatedEntities, RelationshipMapping relationship) {
        if (relatedEntities == null) {
            return;
        }

        switch (relationship.relationshipType()) {
            case ONE_TO_ONE, MANY_TO_ONE:
                entityManager.refresh(relatedEntities);
                break;

            case ONE_TO_MANY, MANY_TO_MANY:
                if (relatedEntities instanceof Collection) {
                    for (Object item : (Collection<?>) relatedEntities) {
                        if (item != null) {
                            entityManager.refresh(item);
                        }
                    }
                }
                break;
        }
    }

    /**
     * Processes related entities for cascade merge.
     */
    private Object mergeRelatedEntities(Object relatedEntities, RelationshipMapping relationship) {
        if (relatedEntities == null) {
            return null;
        }

        return switch (relationship.relationshipType()) {
            case ONE_TO_ONE, MANY_TO_ONE -> entityManager.merge(relatedEntities);
            case ONE_TO_MANY, MANY_TO_MANY -> {
                if (relatedEntities instanceof Collection<?> originalCollection) {
                    List<Object> mergedList = new ArrayList<>();
                    for (Object item : originalCollection) {
                        if (item != null) {
                            mergedList.add(entityManager.merge(item));
                        }
                    }
                    yield mergedList;
                }
                yield relatedEntities;
            }
        };
    }

    /**
     * Gets all relationships for an entity.
     */
    private List<RelationshipMapping> getRelationships(EntityMetadata metadata) {
        List<RelationshipMapping> relationships = new ArrayList<>();

        for (Field field : metadata.getEntityClass().getDeclaredFields()) {
            RelationshipMapping relationship = createRelationshipMapping(field, metadata);
            if (relationship != null) {
                relationships.add(relationship);
            }
        }

        return relationships;
    }

    /**
     * Creates a relationship mapping from annotated fields.
     */
    private RelationshipMapping createRelationshipMapping(Field field, EntityMetadata metadata) {
        field.setAccessible(true);

        // OneToMany
        if (field.isAnnotationPresent(OneToMany.class)) {
            OneToMany annotation = field.getAnnotation(OneToMany.class);
            return new RelationshipMapping(
                    field,
                    annotation.targetEntity(),
                    RelationshipMapping.RelationshipType.ONE_TO_MANY,
                    annotation.mappedBy(),
                    annotation.fetch(),
                    annotation.cascade()
            );
        }

        // ManyToOne
        if (field.isAnnotationPresent(de.t14d3.spool.annotations.ManyToOne.class)) {
            var annotation = field.getAnnotation(de.t14d3.spool.annotations.ManyToOne.class);
            // For ManyToOne, we infer the target entity from the field type
            return new RelationshipMapping(
                    field,
                    field.getType(),
                    RelationshipMapping.RelationshipType.MANY_TO_ONE,
                    "", // No mappedBy for owning side
                    annotation.fetch(),
                    annotation.cascade()
            );
        }

        // OneToOne
        if (field.isAnnotationPresent(de.t14d3.spool.annotations.OneToOne.class)) {
            var annotation = field.getAnnotation(de.t14d3.spool.annotations.OneToOne.class);
            return new RelationshipMapping(
                    field,
                    field.getType(),
                    RelationshipMapping.RelationshipType.ONE_TO_ONE,
                    annotation.mappedBy(),
                    annotation.fetch(),
                    annotation.cascade()
            );
        }

        // ManyToMany
        if (field.isAnnotationPresent(de.t14d3.spool.annotations.ManyToMany.class)) {
            var annotation = field.getAnnotation(de.t14d3.spool.annotations.ManyToMany.class);
            return new RelationshipMapping(
                    field,
                    field.getType(),
                    RelationshipMapping.RelationshipType.MANY_TO_MANY,
                    annotation.mappedBy(),
                    annotation.fetch(),
                    annotation.cascade()
            );
        }

        return null;
    }
}
