package de.t14d3.spool.core;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.FetchType;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.mapping.EntityId;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.mapping.RelationshipMapping;
import de.t14d3.spool.mapping.TypeMapper;
import de.t14d3.spool.query.Query;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages entity relationships and cascading operations.
 */
@SuppressWarnings("ClassCanBeRecord")
public class RelationshipManager {

    private final EntityManager entityManager;
    private final Map<Class<?>, List<RelationshipMapping>> relationshipCache;
    private final ThreadLocal<Integer> bidirectionalSyncDepth;

    public RelationshipManager(EntityManager entityManager) {
        this.entityManager = entityManager;
        this.relationshipCache = new ConcurrentHashMap<>();
        this.bidirectionalSyncDepth = ThreadLocal.withInitial(() -> 0);
    }

    /**
     * Handles cascade persist operations for an entity.
     */
    public void cascadePersist(Object entity, EntityMetadata metadata) {
        prepareRelationships(entity, metadata);
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
        prepareRelationships(entity, metadata);
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
        prepareRelationships(entity, metadata);
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
        prepareRelationships(entity, metadata);
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
        prepareRelationships(entity, metadata);
        List<RelationshipMapping> relationships = getRelationships(metadata);

        for (RelationshipMapping relationship : relationships) {
            if (relationship.isCascadable(CascadeType.MERGE) ||
                    relationship.isCascadable(CascadeType.ALL)) {

                Object relatedEntities = metadata.getFieldValue(entity, relationship.field());
                Object mergedRelatedEntities = mergeRelatedEntities(relatedEntities, relationship);

                // Update the field with merged entities if they changed
                if (mergedRelatedEntities != relatedEntities) {
                    metadata.setFieldValue(entity, relationship.field(), mergedRelatedEntities);
                    // Re-wrap ManyToMany collection after replacement to restore the
                    // bidirectional-sync wrapper (ManyToManySet/List/Collection) that
                    // was removed when mergeRelatedEntities returned a plain collection.
                    if (relationship.relationshipType() == RelationshipMapping.RelationshipType.MANY_TO_MANY) {
                        Field invField = resolveManyToManyInverseField(
                            metadata.getEntityClass(),
                            relationship.field(),
                            relationship.targetEntity(),
                            relationship.mappedBy());
                        if (invField != null) {
                            wrapManyToManyCollection(entity, relationship.field(), invField);
                        }
                    }
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

            case ONE_TO_MANY:
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

            case MANY_TO_MANY:
                // Do NOT cascade delete related entities - ManyToMany entities are shared
                // across multiple owners. Only join-table rows are deleted at flush time
                // by deleteManyToManyJoinRows().
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
                    // Preserve the original collection type (Set vs List) to avoid
                    // IllegalArgumentException when field.set() receives an ArrayList
                    // for a Set-typed field.
                    Collection<Object> mergedCollection = (originalCollection instanceof Set)
                        ? new LinkedHashSet<>()
                        : new ArrayList<>();
                    for (Object item : originalCollection) {
                        if (item != null) {
                            mergedCollection.add(entityManager.merge(item));
                        }
                    }
                    yield mergedCollection;
                }
                yield relatedEntities;
            }
        };
    }

    /**
     * Gets all relationships for an entity.
     */
    private List<RelationshipMapping> getRelationships(EntityMetadata metadata) {
        return relationshipCache.computeIfAbsent(metadata.getEntityClass(), cls -> {
            List<RelationshipMapping> relationships = new ArrayList<>();
            for (Field field : metadata.getHierarchyFields()) {
                RelationshipMapping relationship = createRelationshipMapping(field, metadata);
                if (relationship != null) {
                    relationships.add(relationship);
                }
            }
            return List.copyOf(relationships);
        });
    }

    List<RelationshipMapping> getRelationshipMappings(EntityMetadata metadata) {
        return getRelationships(metadata);
    }

    /**
     * Creates a relationship mapping from annotated fields.
     */
    private RelationshipMapping createRelationshipMapping(Field field, EntityMetadata metadata) {
        field.setAccessible(true);

        // OneToMany
        if (field.isAnnotationPresent(OneToMany.class)) {
            OneToMany annotation = field.getAnnotation(OneToMany.class);
            de.t14d3.spool.annotations.JoinColumn joinColumn =
                    field.getAnnotation(de.t14d3.spool.annotations.JoinColumn.class);
            String fkColumn = (annotation.mappedBy() == null || annotation.mappedBy().isBlank())
                    && joinColumn != null ? joinColumn.name() : "";
            return new RelationshipMapping(
                    field,
                    annotation.targetEntity(),
                    RelationshipMapping.RelationshipType.ONE_TO_MANY,
                    annotation.mappedBy(),
                    annotation.fetch(),
                    annotation.cascade(),
                    fkColumn,
                    annotationOrderBy(field)
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
                    EntityMetadata.collectionElementType(field),
                    RelationshipMapping.RelationshipType.MANY_TO_MANY,
                    annotation.mappedBy(),
                    annotation.fetch(),
                    annotation.cascade(),
                    "",
                    annotationOrderBy(field)
            );
        }

        return null;
    }

    private static String annotationOrderBy(Field field) {
        de.t14d3.spool.annotations.OrderBy orderBy = field.getAnnotation(de.t14d3.spool.annotations.OrderBy.class);
        return orderBy != null ? orderBy.value() : "";
    }

    /**
     * Prepare relationship fields (currently focused on ManyToMany) to keep object graphs consistent.
     *
     * This wraps ManyToMany collections so that add/remove operations update the inverse side.
     * It's safe to call multiple times.
     */
    public void prepareRelationships(Object entity, EntityMetadata metadata) {
        if (entity == null) {
            return;
        }

        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.relationshipType() != RelationshipMapping.RelationshipType.MANY_TO_MANY) {
                continue;
            }

            Field ownerField = relationship.field();
            Class<?> targetEntity = relationship.targetEntity();
            if (targetEntity == Object.class) {
                // Cannot reliably manage ManyToMany without knowing the element type.
                continue;
            }

            Field inverseField = resolveManyToManyInverseField(metadata.getEntityClass(), ownerField, targetEntity, relationship.mappedBy());
            if (inverseField == null) {
                // Unidirectional ManyToMany; nothing to sync.
                continue;
            }

            Object fieldValue = getFieldValue(entity, ownerField);
            if (fieldValue instanceof LazyCollection lazy && !lazy.isLoaded()) {
                // Do not force a lazy load just to synchronize inverse links;
                // the loader does that once the collection is materialized.
                continue;
            }

            wrapManyToManyCollection(entity, ownerField, inverseField);

            Object current = getFieldValue(entity, ownerField);
            if (!(current instanceof Collection<?> collection)) {
                continue;
            }

            for (Object related : collection) {
                if (related == null) continue;
                wrapManyToManyCollection(related, inverseField, ownerField);
                ensureInverseContains(entity, related, inverseField);
            }
        }
    }

    /**
     * Eagerly hydrate single-reference relationships (ManyToOne/OneToOne) when configured with fetch=EAGER.
     *
     * This intentionally does not attempt to make single refs lazy; if fetch=LAZY the reference remains
     * a stub (id-only) created by row mapping or user code.
     */
    public void hydrateEagerSingleRefs(Object entity, EntityMetadata metadata) {
        if (entity == null) {
            return;
        }

        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.relationshipType() != RelationshipMapping.RelationshipType.MANY_TO_ONE
                    && relationship.relationshipType() != RelationshipMapping.RelationshipType.ONE_TO_ONE) {
                continue;
            }
            if (relationship.fetch() != FetchType.EAGER) {
                continue;
            }

            Object related = getFieldValue(entity, relationship.field());
            if (related == null) {
                continue;
            }

            EntityMetadata relatedMeta = EntityMetadata.of(related.getClass());
            EntityId relatedId = relatedMeta.getEntityId(related);
            if (!relatedId.isComplete()) {
                continue;
            }

            Object managed = entityManager.find(related.getClass(), relatedId);
            if (managed != null && managed != related) {
                setFieldValue(entity, relationship.field(), managed);
            }
        }
    }

    /**
     * Eagerly hydrate inverse-side {@code @OneToOne} references (the side with
     * {@code mappedBy} set): no FK column lives on this table, so the owner row
     * is looked up by its FK to this entity.
     */
    public void hydrateEagerInverseOneToOne(Object entity, EntityMetadata metadata) {
        hydrateInverseOneToOne(entity, metadata, true);
    }

    /**
     * Load inverse-side {@code @OneToOne} references regardless of fetch type.
     * Used by the explicit fetch API ({@code em.initialize(...)}).
     */
    public void initializeInverseOneToOne(Object entity, EntityMetadata metadata) {
        hydrateInverseOneToOne(entity, metadata, false);
    }

    private void hydrateInverseOneToOne(Object entity, EntityMetadata metadata, boolean eagerOnly) {
        if (entity == null) {
            return;
        }
        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.relationshipType() != RelationshipMapping.RelationshipType.ONE_TO_ONE) {
                continue;
            }
            if (relationship.isOwningSide()) {
                continue;
            }
            if (eagerOnly && relationship.fetch() != FetchType.EAGER) {
                continue;
            }
            if (metadata.isCompositeId()) {
                throw new UnsupportedOperationException(
                        "Inverse @OneToOne with a composite-id owner is not supported: "
                                + metadata.getEntityClass().getName());
            }
            Object loaded = loadInverseOneToOne(entity, metadata, relationship);
            if (loaded != null) {
                setFieldValue(entity, relationship.field(), loaded);
            }
        }
    }

    private Object loadInverseOneToOne(Object entity, EntityMetadata metadata, RelationshipMapping relationship) {
        EntityMetadata targetMeta = EntityMetadata.of(relationship.targetEntity());
        Field ownerField = EntityMetadata.findField(relationship.targetEntity(), relationship.mappedBy());
        if (ownerField == null || !ownerField.isAnnotationPresent(de.t14d3.spool.annotations.OneToOne.class)) {
            throw new IllegalStateException("Inverse @OneToOne mappedBy field not found: "
                    + relationship.targetEntity().getName() + "." + relationship.mappedBy());
        }
        String fkColumn = targetMeta.getColumnName(ownerField);
        if (fkColumn == null || fkColumn.isBlank()) {
            return null;
        }
        Object id = metadata.getIdValue(entity);
        if (id == null) {
            return null;
        }

        Query q = Query.select(entityManager.getDialect(), "*")
                .from(targetMeta.getTableName())
                .where(entityManager.getDialect().quoteIdentifier(fkColumn) + " = ?", id)
                .build();
        List<?> rows = entityManager.executeSelectQuery(q, relationship.targetEntity());
        return rows.isEmpty() ? null : rows.get(0);
    }

    // ========================================================================
    // Explicit fetch: batched collection initialization
    // ========================================================================

    /**
     * Batch-load collection associations for a list of parents. Issues one pair
     * query plus one entity load per association (chunked at 500 ids), instead
     * of one query per parent. When {@code onlyField} is non-null, only that
     * association is initialized.
     * <p>
     * Parents already loaded through this EntityManager have lazy wrappers;
     * wrappers already marked loaded are skipped. Other parents get a fresh
     * loaded wrapper holding the fetched items.
     */
    public void initializeCollections(List<?> entities, String onlyField) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        Map<Class<?>, List<Object>> byClass = new LinkedHashMap<>();
        for (Object entity : entities) {
            if (entity != null) {
                byClass.computeIfAbsent(entity.getClass(), key -> new ArrayList<>()).add(entity);
            }
        }
        for (Map.Entry<Class<?>, List<Object>> entry : byClass.entrySet()) {
            EntityMetadata metadata = EntityMetadata.of(entry.getKey());
            for (RelationshipMapping relationship : getRelationships(metadata)) {
                if (relationship.relationshipType() != RelationshipMapping.RelationshipType.ONE_TO_MANY
                        && relationship.relationshipType() != RelationshipMapping.RelationshipType.MANY_TO_MANY) {
                    continue;
                }
                if (onlyField != null && !onlyField.equals(relationship.field().getName())) {
                    continue;
                }
                initializeCollectionAssociation(entry.getValue(), metadata, relationship);
            }
        }
    }

    private void initializeCollectionAssociation(
            List<Object> parents,
            EntityMetadata metadata,
            RelationshipMapping relationship) {
        List<Object> pending = new ArrayList<>();
        for (Object parent : parents) {
            Object value = getFieldValue(parent, relationship.field());
            if (value instanceof LazyCollection lazy && lazy.isLoaded()) {
                continue;
            }
            if (metadata.getEntityId(parent).isComplete()) {
                pending.add(parent);
            }
        }
        if (pending.isEmpty()) {
            return;
        }
        if (metadata.isCompositeId()) {
            throw new UnsupportedOperationException(
                    "Collection initialization with a composite-id owner is not supported: "
                            + metadata.getEntityClass().getName());
        }

        Field mappedByField = null;
        Map<Object, List<Object>> grouped;
        if (relationship.relationshipType() == RelationshipMapping.RelationshipType.ONE_TO_MANY) {
            if (!relationship.isOwningSide()) {
                mappedByField = EntityMetadata.findField(relationship.targetEntity(), relationship.mappedBy());
            }
            grouped = loadOneToManyBatch(pending, metadata, relationship);
        } else {
            if (relationship.targetEntity() == Object.class) {
                return;
            }
            grouped = loadManyToManyBatch(pending, metadata, relationship);
        }

        for (Object parent : pending) {
            Object ownerId = metadata.getIdValue(parent);
            List<Object> items = grouped.getOrDefault(ownerId, List.of());
            if (mappedByField != null) {
                for (Object child : items) {
                    setFieldValue(child, mappedByField, parent);
                }
            }
            applyLoadedCollection(parent, relationship, items);
        }

        if (relationship.relationshipType() == RelationshipMapping.RelationshipType.MANY_TO_MANY) {
            Field ownerField = relationship.field();
            Field inverseField = resolveManyToManyInverseField(
                    metadata.getEntityClass(), ownerField, relationship.targetEntity(), relationship.mappedBy());
            if (inverseField != null) {
                for (Object parent : pending) {
                    for (Object related : grouped.getOrDefault(metadata.getIdValue(parent), List.of())) {
                        syncManyToManyInverse(parent, related, inverseField, ownerField);
                    }
                }
            }
        }
    }

    private void applyLoadedCollection(Object parent, RelationshipMapping relationship, List<Object> items) {
        Object value = getFieldValue(parent, relationship.field());
        if (value instanceof LazyCollection lazy) {
            lazy.attachLoader(() -> lazy.replaceContents(items));
            lazy.ensureLoaded();
            return;
        }

        if (relationship.relationshipType() == RelationshipMapping.RelationshipType.MANY_TO_MANY) {
            Field ownerField = relationship.field();
            Field inverseField = resolveManyToManyInverseField(
                    metadataFor(parent).getEntityClass(), ownerField, relationship.targetEntity(), relationship.mappedBy());
            if (inverseField != null) {
                wrapManyToManyCollection(parent, ownerField, inverseField);
                Object wrapped = getFieldValue(parent, ownerField);
                if (wrapped instanceof LazyCollection lazy) {
                    lazy.attachLoader(() -> lazy.replaceContents(items));
                    lazy.ensureLoaded();
                    return;
                }
            }
        }

        LazyCollection wrapper = createLazyCollection(relationship.field(), mutationTracker(parent, relationship));
        if (wrapper != null) {
            wrapper.replaceContents(items);
            setFieldValue(parent, relationship.field(), wrapper);
        }
    }

    /**
     * Resolves an {@code @OrderBy} value to a SQL ORDER BY clause for the given
     * target metadata. {@code tableAlias} prefixes columns when the query joins
     * the target table (M2M pair queries).
     */
    private String orderByClause(RelationshipMapping relationship, EntityMetadata targetMeta, String tableAlias) {
        String orderBy = relationship.orderBy();
        if (orderBy == null || orderBy.isBlank()) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        for (String segment : orderBy.split(",")) {
            String[] tokens = segment.trim().split("\\s+");
            String fieldName = tokens[0];
            boolean descending = tokens.length > 1 && tokens[1].equalsIgnoreCase("DESC");
            Field ordered = EntityMetadata.findField(targetMeta.getEntityClass(), fieldName);
            String column = ordered == null ? null : targetMeta.getColumnName(ordered);
            if (column == null) {
                throw new IllegalStateException("@OrderBy references an unmapped field: "
                        + targetMeta.getEntityClass().getName() + "." + fieldName);
            }
            String prefix = tableAlias == null ? "" : tableAlias + ".";
            parts.add(prefix + column + (descending ? " DESC" : " ASC"));
        }
        return String.join(", ", parts);
    }

    private Set<Object> loadOrderedManyToManyOtherIds(
            EntityManager.ManyToManyJoinTable join,
            EntityMetadata targetMeta,
            Object thisId,
            Class<?> otherIdType,
            String order) {
        Query query = Query.select(entityManager.getDialect(), "j." + join.otherColumn())
                .from(join.joinTable(), "j")
                .join("JOIN " + entityManager.getDialect().quoteIdentifier(targetMeta.getTableName())
                        + " t ON t." + entityManager.getDialect().quoteIdentifier(targetMeta.getIdColumnName())
                        + " = j." + entityManager.getDialect().quoteIdentifier(join.otherColumn()))
                .where("j." + entityManager.getDialect().quoteIdentifier(join.thisColumn()) + " = ?", thisId)
                .orderBy(order)
                .build();
        Set<Object> ids = new LinkedHashSet<>();
        try (PreparedStatement stmt = entityManager.getExecutor().getConnection().prepareStatement(query.getSql())) {
            SqlExecutor.setParameters(stmt, query.getParameters());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ids.add(TypeMapper.convertToJavaType(rs.getObject(1), otherIdType));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load ordered " + join.joinTable() + " rows", e);
        }
        return ids;
    }

    private static EntityMetadata metadataFor(Object entity) {
        return EntityMetadata.of(entity.getClass());
    }

    private Map<Object, List<Object>> loadOneToManyBatch(
            List<Object> parents,
            EntityMetadata metadata,
            RelationshipMapping relationship) {
        Class<?> targetEntity = relationship.targetEntity();
        EntityMetadata targetMeta = EntityMetadata.of(targetEntity);
        String fkColumn;
        if (relationship.isOwningSide()) {
            fkColumn = relationship.joinColumn();
        } else {
            Field mappedByField = EntityMetadata.findField(targetEntity, relationship.mappedBy());
            if (mappedByField == null) {
                throw new IllegalStateException("OneToMany mappedBy field not found: "
                        + targetEntity.getName() + "." + relationship.mappedBy());
            }
            fkColumn = targetMeta.getColumnName(mappedByField);
        }
        if (fkColumn == null || fkColumn.isBlank()) {
            throw new IllegalStateException("OneToMany association has no FK column: "
                    + metadata.getEntityClass().getName() + "." + relationship.field().getName());
        }

        List<Object> ownerIds = new ArrayList<>(parents.size());
        for (Object parent : parents) {
            ownerIds.add(metadata.getIdValue(parent));
        }
        Map<Object, List<Object>> childIdsByOwner = loadPairIds(
                targetMeta.getTableName(),
                null,
                fkColumn,
                targetMeta.getIdColumnName(),
                null,
                orderByClause(relationship, targetMeta, null),
                ownerIds,
                metadata.getIdField().getType(),
                targetMeta.getIdField().getType());
        return loadGroupedTargets(childIdsByOwner, targetEntity, targetMeta);
    }

    private Map<Object, List<Object>> loadManyToManyBatch(
            List<Object> parents,
            EntityMetadata metadata,
            RelationshipMapping relationship) {
        EntityManager.ManyToManyJoinTable join = entityManager.resolveManyToManyJoinTable(metadata, relationship);
        if (join == null) {
            return new LinkedHashMap<>();
        }
        EntityMetadata targetMeta = EntityMetadata.of(relationship.targetEntity());
        List<Object> ownerIds = new ArrayList<>(parents.size());
        for (Object parent : parents) {
            ownerIds.add(metadata.getIdValue(parent));
        }
        String order = orderByClause(relationship, targetMeta, "t");
        String tableAlias = order == null ? null : "j";
        String joinClause = order == null
                ? null
                : "JOIN " + entityManager.getDialect().quoteIdentifier(targetMeta.getTableName())
                        + " t ON t." + entityManager.getDialect().quoteIdentifier(targetMeta.getIdColumnName())
                        + " = j." + entityManager.getDialect().quoteIdentifier(join.otherColumn());
        Map<Object, List<Object>> otherIdsByOwner = loadPairIds(
                join.joinTable(),
                tableAlias,
                join.thisColumn(),
                join.otherColumn(),
                joinClause,
                order,
                ownerIds,
                metadata.getIdField().getType(),
                targetMeta.getIdField().getType());
        return loadGroupedTargets(otherIdsByOwner, relationship.targetEntity(), targetMeta);
    }

    private Map<Object, List<Object>> loadGroupedTargets(
            Map<Object, List<Object>> idsByOwner,
            Class<?> targetEntity,
            EntityMetadata targetMeta) {
        Set<Object> allIds = new LinkedHashSet<>();
        for (List<Object> ids : idsByOwner.values()) {
            allIds.addAll(ids);
        }
        if (allIds.isEmpty()) {
            return new LinkedHashMap<>();
        }
        List<?> targets = entityManager.findManagedByIds(targetEntity, allIds);
        Map<Object, Object> byId = new LinkedHashMap<>();
        for (Object target : targets) {
            EntityId id = targetMeta.getEntityId(target);
            if (id.isComplete()) {
                byId.put(id.singleValue(), target);
            }
        }
        Map<Object, List<Object>> grouped = new LinkedHashMap<>();
        for (Map.Entry<Object, List<Object>> entry : idsByOwner.entrySet()) {
            List<Object> resolved = new ArrayList<>();
            for (Object id : entry.getValue()) {
                Object target = byId.get(id);
                if (target != null) {
                    resolved.add(target);
                }
            }
            grouped.put(entry.getKey(), resolved);
        }
        return grouped;
    }

    /**
     * Loads {@code (fk, id)} pairs from {@code table} where the FK column is in
     * {@code ownerIds}, chunked to keep IN lists small. An optional table alias,
     * JOIN clause and ORDER BY support M2M pair queries ordered by target
     * columns.
     */
    private Map<Object, List<Object>> loadPairIds(
            String table,
            String tableAlias,
            String fkColumn,
            String idColumn,
            String joinClause,
            String orderBy,
            List<Object> ownerIds,
            Class<?> fkType,
            Class<?> idType) {
        Map<Object, List<Object>> grouped = new LinkedHashMap<>();
        String columnPrefix = tableAlias == null ? "" : tableAlias + ".";
        for (int i = 0; i < ownerIds.size(); i += 500) {
            List<Object> chunk = ownerIds.subList(i, Math.min(i + 500, ownerIds.size()));
            String placeholders = String.join(", ", Collections.nCopies(chunk.size(), "?"));
            Query.SelectBuilder builder = tableAlias == null
                    ? Query.select(entityManager.getDialect(), fkColumn, idColumn)
                            .from(table)
                    : Query.select(entityManager.getDialect(), tableAlias + "." + fkColumn, tableAlias + "." + idColumn)
                            .from(table, tableAlias);
            if (joinClause != null) {
                builder.join(joinClause);
            }
            builder.where(columnPrefix + entityManager.getDialect().quoteIdentifier(fkColumn)
                    + " IN (" + placeholders + ")", chunk.toArray());
            if (orderBy != null) {
                builder.orderBy(orderBy);
            }
            Query query = builder.build();
            try (PreparedStatement stmt = entityManager.getExecutor().getConnection().prepareStatement(query.getSql())) {
                SqlExecutor.setParameters(stmt, query.getParameters());
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Object fk = TypeMapper.convertToJavaType(rs.getObject(1), fkType);
                        Object id = TypeMapper.convertToJavaType(rs.getObject(2), idType);
                        grouped.computeIfAbsent(fk, key -> new ArrayList<>()).add(id);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Failed to batch-load relationship rows from " + table + "." + fkColumn, e);
            }
        }
        return grouped;
    }

    /**
     * Wave-based batch hydration of EAGER collection associations for a raw
     * result set. Loaded collections are marked loaded, so the per-entity
     * hydration pass skips them; children join the next wave so their own
     * EAGER collections are batched too.
     */
    public void prefetchEagerCollections(List<?> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Object> wave = new ArrayDeque<>();
        for (Object row : rows) {
            if (row != null) {
                wave.add(row);
            }
        }

        while (!wave.isEmpty()) {
            Map<Class<?>, List<Object>> byClass = new LinkedHashMap<>();
            while (!wave.isEmpty()) {
                Object entity = wave.poll();
                if (entity == null || !visited.add(entity)) {
                    continue;
                }
                EntityMetadata metadata = EntityMetadata.of(entity.getClass());
                if (!metadata.getEntityId(entity).isComplete()) {
                    continue;
                }
                byClass.computeIfAbsent(entity.getClass(), key -> new ArrayList<>()).add(entity);
            }
            if (byClass.isEmpty()) {
                break;
            }

            List<Object> nextWave = new ArrayList<>();
            for (Map.Entry<Class<?>, List<Object>> entry : byClass.entrySet()) {
                EntityMetadata metadata = EntityMetadata.of(entry.getKey());
                List<Object> parents = entry.getValue();
                for (RelationshipMapping relationship : getRelationships(metadata)) {
                    if (relationship.fetch() != FetchType.EAGER) {
                        continue;
                    }
                    if (relationship.relationshipType() == RelationshipMapping.RelationshipType.ONE_TO_MANY) {
                        if (metadata.isCompositeId()) {
                            throw new UnsupportedOperationException(
                                    "Eager collection hydration with a composite-id owner is not supported: "
                                            + metadata.getEntityClass().getName());
                        }
                        Field mappedByField = relationship.isOwningSide()
                                ? null
                                : EntityMetadata.findField(relationship.targetEntity(), relationship.mappedBy());
                        Map<Object, List<Object>> grouped = loadOneToManyBatch(parents, metadata, relationship);
                        for (Object parent : parents) {
                            List<Object> items = grouped.getOrDefault(metadata.getIdValue(parent), List.of());
                            if (mappedByField != null) {
                                for (Object child : items) {
                                    setFieldValue(child, mappedByField, parent);
                                }
                            }
                            applyLoadedCollection(parent, relationship, items);
                            nextWave.addAll(items);
                        }
                    } else if (relationship.relationshipType() == RelationshipMapping.RelationshipType.MANY_TO_MANY) {
                        if (relationship.targetEntity() == Object.class) {
                            continue;
                        }
                        Map<Object, List<Object>> grouped = loadManyToManyBatch(parents, metadata, relationship);
                        Field ownerField = relationship.field();
                        Field inverseField = resolveManyToManyInverseField(
                                metadata.getEntityClass(), ownerField, relationship.targetEntity(), relationship.mappedBy());
                        for (Object parent : parents) {
                            List<Object> items = grouped.getOrDefault(metadata.getIdValue(parent), List.of());
                            applyLoadedCollection(parent, relationship, items);
                            if (inverseField != null) {
                                for (Object related : items) {
                                    syncManyToManyInverse(parent, related, inverseField, ownerField);
                                }
                            }
                            nextWave.addAll(items);
                        }
                    }
                }
            }
            wave.addAll(nextWave);
        }
    }

    /**
     * Eagerly hydrate collection relationships (currently OneToMany) when configured with fetch=EAGER.
     * <p>
     * This runs a SELECT on the target table using the {@code mappedBy} foreign-key column and
     * populates the owner's collection. It also sets the back-reference on the child objects.
     */
    public void hydrateEagerCollections(Object entity, EntityMetadata metadata) {
        if (entity == null) {
            return;
        }

        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.relationshipType() != RelationshipMapping.RelationshipType.ONE_TO_MANY) {
                continue;
            }
            if (relationship.fetch() != FetchType.EAGER) {
                continue;
            }

            List<Object> children = loadOneToManyEntities(entity, metadata, relationship);
            if (relationship.isOwningSide()) {
                // Unidirectional FK-on-target: mutate through a tracking
                // wrapper so flush() can write the FK delta.
                LazyCollection wrapper = createLazyCollection(
                        relationship.field(), mutationTracker(entity, relationship));
                if (wrapper != null) {
                    wrapper.replaceContents(children);
                    setFieldValue(entity, relationship.field(), wrapper);
                    continue;
                }
            }
            Collection<Object> ownerCollection = getOrCreateCollection(entity, relationship.field());
            ownerCollection.clear();
            ownerCollection.addAll(children);
        }
    }

    private Runnable mutationTracker(Object entity, RelationshipMapping relationship) {
        boolean needsTracking = relationship.isOwningSide() || relationship.isOrphanRemoval();
        return needsTracking
                ? () -> entityManager.markOneToManyDirty(entity, relationship.field())
                : null;
    }

    /**
     * Load the children of a OneToMany association and wire their back-references
     * to {@code entity}. Shared by eager hydration and lazy loading.
     */
    private List<Object> loadOneToManyEntities(Object entity, EntityMetadata metadata, RelationshipMapping relationship) {
        if (metadata.isCompositeId()) {
            throw new UnsupportedOperationException(
                    "OneToMany with a composite-id owner is not supported: " + metadata.getEntityClass().getName());
        }
        Object ownerId = metadata.getIdValue(entity);
        if (ownerId == null) {
            return new ArrayList<>();
        }

        Class<?> targetEntity = relationship.targetEntity();
        EntityMetadata targetMeta = EntityMetadata.of(targetEntity);

        Field mappedByField = null;
        String fkColumn;
        if (relationship.isOwningSide()) {
            // Unidirectional @OneToMany: the FK column lives on the target
            // table and is owned by this association (no field on the child).
            fkColumn = relationship.joinColumn();
            if (fkColumn == null || fkColumn.isBlank()) {
                throw new IllegalStateException("Unidirectional OneToMany without a join column: "
                        + metadata.getEntityClass().getName() + "." + relationship.field().getName());
            }
        } else {
            mappedByField = EntityMetadata.findField(targetEntity, relationship.mappedBy());
            if (mappedByField == null) {
                throw new IllegalStateException("OneToMany mappedBy field not found: " + targetEntity.getName() + "." + relationship.mappedBy());
            }
            fkColumn = targetMeta.getColumnName(mappedByField);
            if (fkColumn == null || fkColumn.isBlank()) {
                throw new IllegalStateException("OneToMany mappedBy field is not column-mapped: " + targetEntity.getName() + "." + relationship.mappedBy());
            }
        }

        Query.SelectBuilder builder = Query.select(entityManager.getDialect(), "*")
                .from(targetMeta.getTableName())
                .where(entityManager.getDialect().quoteIdentifier(fkColumn) + " = ?", ownerId);
        String orderBy = orderByClause(relationship, targetMeta, null);
        if (orderBy != null) {
            builder.orderBy(orderBy);
        }
        Query q = builder.build();

        @SuppressWarnings("unchecked")
        List<Object> children = (List<Object>) (List<?>) entityManager.executeSelectQuery(q, targetEntity);

        if (mappedByField != null) {
            for (Object child : children) {
                if (child == null) {
                    continue;
                }
                setFieldValue(child, mappedByField, entity);
            }
        }
        return children;
    }

    /**
     * Eagerly hydrate ManyToMany relationships when configured with fetch=EAGER.
     * <p>
     * To avoid large inverse graphs and recursive hydration loops, this only hydrates the owning side.
     * Inverse collections are synchronized in-memory for the loaded entities.
     */
    public void hydrateEagerManyToMany(Object entity, EntityMetadata metadata) {
        if (entity == null) {
            return;
        }
        if (!metadata.getEntityId(entity).isComplete()) {
            return;
        }

        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.relationshipType() != RelationshipMapping.RelationshipType.MANY_TO_MANY) {
                continue;
            }
            if (relationship.fetch() != FetchType.EAGER) {
                continue;
            }

            List<Object> relatedEntities = loadManyToManyEntities(entity, metadata, relationship);
            Field ownerField = relationship.field();
            Collection<Object> ownerCollection = getOrCreateCollection(entity, ownerField);
            withoutBidirectionalSync(() -> {
                ownerCollection.clear();
                ownerCollection.addAll(relatedEntities);
            });

            Field inverseField = resolveManyToManyInverseField(
                metadata.getEntityClass(),
                ownerField,
                relationship.targetEntity(),
                relationship.mappedBy()
            );
            if (inverseField == null) {
                continue;
            }

            for (Object related : relatedEntities) {
                if (related == null) {
                    continue;
                }
                syncManyToManyInverse(entity, related, inverseField, ownerField);
            }
        }
    }

    /**
     * Wire the inverse side of a loaded ManyToMany link when the inverse
     * collection is already materialized. Unloaded lazy inverse collections
     * are left alone: their own loader reads the same join rows, so forcing
     * them here would recurse (or deadlock) for bidirectional associations.
     */
    private void syncManyToManyInverse(Object owner, Object related, Field inverseField, Field ownerField) {
        if (inverseField == null) {
            return;
        }
        Object inverse = getFieldValue(related, inverseField);
        if (inverse instanceof LazyCollection lazy && !lazy.isLoaded()) {
            return;
        }
        wrapManyToManyCollection(related, inverseField, ownerField);
        ensureInverseContains(owner, related, inverseField);
    }

    /**
     * Load the entities referenced by a ManyToMany association. Shared by eager
     * hydration and lazy loading.
     */
    private List<Object> loadManyToManyEntities(Object entity, EntityMetadata metadata, RelationshipMapping relationship) {
        if (metadata.isCompositeId()) {
            throw new UnsupportedOperationException(
                    "ManyToMany with a composite-id entity is not supported: " + metadata.getEntityClass().getName());
        }
        Object thisId = metadata.getIdValue(entity);
        if (thisId == null || relationship.targetEntity() == Object.class) {
            return new ArrayList<>();
        }
        EntityManager.ManyToManyJoinTable join = entityManager.resolveManyToManyJoinTable(metadata, relationship);
        if (join == null) {
            return new ArrayList<>();
        }

        EntityMetadata otherMeta = EntityMetadata.of(relationship.targetEntity());
        Class<?> otherIdType = otherMeta.getIdField().getType();
        String order = orderByClause(relationship, otherMeta, "t");
        Set<Object> otherIds = order == null
                ? entityManager.loadManyToManyOtherIds(join, thisId, otherIdType)
                : loadOrderedManyToManyOtherIds(join, otherMeta, thisId, otherIdType, order);

        List<Object> relatedEntities = new ArrayList<>();
        for (Object otherId : otherIds) {
            Object related = entityManager.find(relationship.targetEntity(), otherId);
            if (related != null) {
                relatedEntities.add(related);
            }
        }
        return relatedEntities;
    }

    // ========================================================================
    // Lazy collections
    // ========================================================================

    /**
     * Install lazy collection wrappers for every LAZY OneToMany/ManyToMany
     * association on a freshly loaded entity. Called by the EntityManager
     * after eager hydration. The wrappers load their contents on first access;
     * no bytecode instrumentation or proxy library is involved.
     */
    public void installLazyCollections(Object entity, EntityMetadata metadata) {
        if (entity == null || !metadata.getEntityId(entity).isComplete()) {
            return;
        }

        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.fetch() != FetchType.LAZY) {
                continue;
            }
            switch (relationship.relationshipType()) {
                case ONE_TO_MANY -> installLazyOneToMany(entity, metadata, relationship);
                case MANY_TO_MANY -> installLazyManyToMany(entity, metadata, relationship);
                default -> {
                    // Single-reference LAZY associations intentionally remain
                    // id-only stubs; intercepting field access would require a
                    // proxy library.
                }
            }
        }
    }

    private void installLazyOneToMany(Object entity, EntityMetadata metadata, RelationshipMapping relationship) {
        if (relationship.isOwningSide()
                && (relationship.joinColumn() == null || relationship.joinColumn().isBlank())) {
            throw new IllegalStateException("Unidirectional OneToMany without a join column: "
                    + metadata.getEntityClass().getName() + "." + relationship.field().getName());
        }
        Object current = getFieldValue(entity, relationship.field());
        if (current instanceof LazyCollection lazy && !lazy.isLoaded()) {
            return;
        }

        LazyCollection wrapper = createLazyCollection(relationship.field(), mutationTracker(entity, relationship));
        if (wrapper == null) {
            return;
        }
        wrapper.attachLoader(() -> wrapper.replaceContents(loadOneToManyEntities(entity, metadata, relationship)));
        setFieldValue(entity, relationship.field(), wrapper);
    }

    private void installLazyManyToMany(Object entity, EntityMetadata metadata, RelationshipMapping relationship) {
        if (relationship.targetEntity() == Object.class) {
            return;
        }

        Field ownerField = relationship.field();
        Object current = getFieldValue(entity, ownerField);
        if (current instanceof LazyCollection lazy && !lazy.isLoaded()) {
            return;
        }

        Field inverseField = resolveManyToManyInverseField(
                metadata.getEntityClass(), ownerField, relationship.targetEntity(), relationship.mappedBy());
        if (inverseField != null && !(current instanceof BidirectionalManagedCollection)) {
            wrapManyToManyCollection(entity, ownerField, inverseField);
            current = getFieldValue(entity, ownerField);
        }

        LazyCollection wrapper;
        if (current instanceof LazyCollection lazy) {
            wrapper = lazy;
        } else {
            LazyCollection created = createLazyCollection(ownerField, null);
            if (created == null) {
                return;
            }
            if (current instanceof Collection<?> existing) {
                created.replaceContents(new ArrayList<>(existing));
            }
            setFieldValue(entity, ownerField, created);
            wrapper = created;
        }

        wrapper.attachLoader(() ->
                loadManyToManyInto(wrapper, entity, metadata, relationship, ownerField, inverseField));
    }

    private void loadManyToManyInto(
            LazyCollection target,
            Object entity,
            EntityMetadata metadata,
            RelationshipMapping relationship,
            Field ownerField,
            Field inverseField) {
        List<Object> relatedEntities = loadManyToManyEntities(entity, metadata, relationship);
        target.replaceContents(relatedEntities);

        if (inverseField == null) {
            return;
        }
        for (Object related : relatedEntities) {
            if (related == null) {
                continue;
            }
            syncManyToManyInverse(entity, related, inverseField, ownerField);
        }
    }

    private static LazyCollection createLazyCollection(Field field, Runnable mutationTracker) {
        Class<?> declared = field.getType();
        if (List.class.isAssignableFrom(declared)) {
            return new LazyOneToManyList(mutationTracker);
        }
        if (Set.class.isAssignableFrom(declared)) {
            return new LazyOneToManySet(mutationTracker);
        }
        if (Collection.class.isAssignableFrom(declared)) {
            return new LazyOneToManyCollection(mutationTracker);
        }
        return null;
    }

    /**
     * Remove ManyToMany links from both sides (used when deleting an entity).  
     */
    public void unlinkManyToMany(Object entity, EntityMetadata metadata) {      
        if (entity == null) {
            return;
        }

        for (RelationshipMapping relationship : getRelationships(metadata)) {
            if (relationship.relationshipType() != RelationshipMapping.RelationshipType.MANY_TO_MANY) {
                continue;
            }

            Field ownerField = relationship.field();
            Class<?> targetEntity = relationship.targetEntity();
            if (targetEntity == Object.class) {
                continue;
            }

            Field inverseField = resolveManyToManyInverseField(metadata.getEntityClass(), ownerField, targetEntity, relationship.mappedBy());
            if (inverseField == null) {
                continue;
            }

            Object current = getFieldValue(entity, ownerField);
            if (current instanceof LazyCollection lazy && !lazy.isLoaded()) {
                // The rows are removed on the database side by
                // deleteManyToManyJoinRows; there is no need to materialize
                // the collection just to unlink in-memory references.
                continue;
            }
            if (!(current instanceof Collection<?> collection)) {
                continue;
            }

            // Create a copy so we can modify collections safely.
            List<Object> relatedItems = new ArrayList<>();
            for (Object r : collection) {
                if (r != null) {
                    relatedItems.add(r);
                }
            }

            for (Object related : relatedItems) {
                removeInverseLink(entity, related, inverseField);
            }

            withoutBidirectionalSync(collection::clear);
        }
    }

    private void wrapManyToManyCollection(Object owner, Field ownerField, Field inverseField) {
        Object current = getFieldValue(owner, ownerField);
        if (current instanceof BidirectionalManagedCollection) {
            return;
        }

        Collection<Object> base = getOrCreateCollection(owner, ownerField);
        Class<?> declared = ownerField.getType();

        Object wrapped;
        if (List.class.isAssignableFrom(declared)) {
            @SuppressWarnings("unchecked")
            List<Object> list = (base instanceof List<?>) ? (List<Object>) base : new ArrayList<>(base);
            wrapped = new ManyToManyList(owner, ownerField, inverseField, list, this);
        } else if (Set.class.isAssignableFrom(declared)) {
            @SuppressWarnings("unchecked")
            Set<Object> set = (base instanceof Set<?>) ? (Set<Object>) base : new LinkedHashSet<>(base);
            wrapped = new ManyToManySet(owner, ownerField, inverseField, set, this);
        } else if (Collection.class.isAssignableFrom(declared)) {
            wrapped = new ManyToManyCollection(owner, ownerField, inverseField, base, this);
        } else {
            // Not a collection; can't manage.
            return;
        }

        setFieldValue(owner, ownerField, wrapped);
    }

    private void ensureInverseContains(Object owner, Object related, Field inverseField) {
        Object inverse = getFieldValue(related, inverseField);
        if (inverse == null) {
            inverse = getOrCreateCollection(related, inverseField);
        }
        if (!(inverse instanceof Collection<?> inverseCollection)) {
            return;
        }
        if (inverseCollection.contains(owner)) {
            return;
        }
        withoutBidirectionalSync(() -> ((Collection<Object>) inverseCollection).add(owner));
    }

    private void removeInverseLink(Object owner, Object related, Field inverseField) {
        Object inverse = getFieldValue(related, inverseField);
        if (!(inverse instanceof Collection<?> inverseCollection)) {
            return;
        }
        if (!inverseCollection.contains(owner)) {
            return;
        }
        withoutBidirectionalSync(() -> ((Collection<Object>) inverseCollection).remove(owner));
    }

    private void withoutBidirectionalSync(Runnable work) {
        bidirectionalSyncDepth.set(bidirectionalSyncDepth.get() + 1);
        try {
            work.run();
        } finally {
            bidirectionalSyncDepth.set(Math.max(0, bidirectionalSyncDepth.get() - 1));
        }
    }

    private boolean isBidirectionalSyncSuppressed() {
        return bidirectionalSyncDepth.get() > 0;
    }

    private static Object getFieldValue(Object target, Field field) {
        try {
            field.setAccessible(true);
            return field.get(target);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access relationship field " + field.getName(), e);
        }
    }

    private static void setFieldValue(Object target, Field field, Object value) {
        try {
            field.setAccessible(true);
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot set relationship field " + field.getName(), e);
        }
    }

    private static Collection<Object> getOrCreateCollection(Object owner, Field field) {
        Object current = getFieldValue(owner, field);
        if (current instanceof Collection<?> c) {
            @SuppressWarnings("unchecked")
            Collection<Object> out = (Collection<Object>) c;
            return out;
        }

        if (current != null) {
            throw new IllegalStateException("Field " + field.getDeclaringClass().getName() + "." + field.getName() + " is not a Collection");
        }

        Collection<Object> created;
        Class<?> declared = field.getType();
        if (List.class.isAssignableFrom(declared)) {
            created = new ArrayList<>();
        } else if (Set.class.isAssignableFrom(declared)) {
            created = new LinkedHashSet<>();
        } else if (Collection.class.isAssignableFrom(declared)) {
            created = new ArrayList<>();
        } else {
            throw new IllegalStateException("Field " + field.getDeclaringClass().getName() + "." + field.getName() + " must be a Collection for ManyToMany");
        }

        setFieldValue(owner, field, created);
        return created;
    }

    private static Field resolveManyToManyInverseField(Class<?> ownerEntity, Field ownerField, Class<?> targetEntity, String mappedBy) {
        if (mappedBy != null && !mappedBy.isBlank()) {
            // This side is inverse; mappedBy points to the owning field on the target entity.
            return EntityMetadata.findField(targetEntity, mappedBy);
        }

        // Owning side; find inverse field on target that maps back to this field name.
        String ownerFieldName = ownerField.getName();
        for (Class<?> c = targetEntity; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (!f.isAnnotationPresent(de.t14d3.spool.annotations.ManyToMany.class)) {
                    continue;
                }
                var ann = f.getAnnotation(de.t14d3.spool.annotations.ManyToMany.class);
                if (!ownerFieldName.equals(ann.mappedBy())) {
                    continue;
                }
                if (!Collection.class.isAssignableFrom(f.getType())) {
                    continue;
                }
                f.setAccessible(true);
                return f;
            }
        }

        // No bidirectional mapping found.
        return null;
    }

    /**
     * Collection that defers loading its contents until first access.
     */
    private interface LazyCollection {
        boolean isLoaded();

        void attachLoader(Runnable loader);

        void ensureLoaded();

        /**
         * Replace the backing contents without triggering load/mutation logic.
         * Only called from the loader itself.
         */
        void replaceContents(Collection<Object> items);

        /**
         * True when mutations of this wrapper mark the owning association dirty.
         */
        default boolean hasMutationTracker() {
            return false;
        }
    }

    /**
     * True when {@code value} is a lazy wrapper that records collection
     * mutations. A replaced (plain) collection is untracked and must be diffed
     * at flush time.
     */
    static boolean isMutationTracked(Object value) {
        return value instanceof LazyCollection lazy && lazy.hasMutationTracker();
    }

    /**
     * True when {@code value} is a materialized collection: a lazy wrapper that
     * has loaded, or a plain collection. Never triggers a load.
     */
    static boolean isCollectionLoaded(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof LazyCollection lazy) {
            return lazy.isLoaded();
        }
        return value instanceof Collection<?>;
    }

    /**
     * Load-once state shared by all lazy collection wrappers.
     */
    private static final class LazyState {
        private Runnable loader;
        private boolean loaded = true;
        private Thread loadingThread;

        synchronized boolean isLoaded() {
            return loaded;
        }

        synchronized void attach(Runnable loader) {
            this.loader = loader;
            this.loaded = false;
            this.loadingThread = null;
        }

        void ensure() {
            Runnable run;
            synchronized (this) {
                while (loadingThread != null) {
                    if (loadingThread == Thread.currentThread()) {
                        throw new IllegalStateException(
                                "Recursive lazy collection load detected; a load must not trigger itself");
                    }
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while loading lazy collection", e);
                    }
                }
                if (loaded) {
                    return;
                }
                loadingThread = Thread.currentThread();
                run = loader;
                loader = null;
            }
            try {
                if (run != null) {
                    run.run();
                }
            } finally {
                synchronized (this) {
                    loaded = true;
                    loadingThread = null;
                    notifyAll();
                }
            }
        }
    }

    private static final class LazyOneToManyList extends AbstractList<Object> implements LazyCollection {
        private final LazyState state = new LazyState();
        private final List<Object> delegate = new ArrayList<>();
        private final Runnable mutationTracker;

        private LazyOneToManyList(Runnable mutationTracker) {
            this.mutationTracker = mutationTracker;
        }

        private void mutated() {
            if (mutationTracker != null) {
                mutationTracker.run();
            }
        }

        @Override
        public boolean hasMutationTracker() {
            return mutationTracker != null;
        }

        @Override
        public boolean isLoaded() {
            return state.isLoaded();
        }

        @Override
        public void attachLoader(Runnable loader) {
            state.attach(loader);
        }

        @Override
        public void ensureLoaded() {
            state.ensure();
        }

        @Override
        public void replaceContents(Collection<Object> items) {
            delegate.clear();
            delegate.addAll(items);
        }

        @Override
        public Object get(int index) {
            ensureLoaded();
            return delegate.get(index);
        }

        @Override
        public int size() {
            ensureLoaded();
            return delegate.size();
        }

        @Override
        public Object set(int index, Object element) {
            ensureLoaded();
            Object previous = delegate.set(index, element);
            if (!Objects.equals(previous, element)) {
                mutated();
            }
            return previous;
        }

        @Override
        public void add(int index, Object element) {
            ensureLoaded();
            delegate.add(index, element);
            mutated();
        }

        @Override
        public Object remove(int index) {
            ensureLoaded();
            Object removed = delegate.remove(index);
            mutated();
            return removed;
        }

        @Override
        public void clear() {
            ensureLoaded();
            if (!delegate.isEmpty()) {
                delegate.clear();
                mutated();
            }
        }
    }

    private static final class LazyOneToManySet extends AbstractSet<Object> implements LazyCollection {
        private final LazyState state = new LazyState();
        private final Set<Object> delegate = new LinkedHashSet<>();
        private final Runnable mutationTracker;

        private LazyOneToManySet(Runnable mutationTracker) {
            this.mutationTracker = mutationTracker;
        }

        private void mutated() {
            if (mutationTracker != null) {
                mutationTracker.run();
            }
        }

        @Override
        public boolean hasMutationTracker() {
            return mutationTracker != null;
        }

        @Override
        public boolean isLoaded() {
            return state.isLoaded();
        }

        @Override
        public void attachLoader(Runnable loader) {
            state.attach(loader);
        }

        @Override
        public void ensureLoaded() {
            state.ensure();
        }

        @Override
        public void replaceContents(Collection<Object> items) {
            delegate.clear();
            delegate.addAll(items);
        }

        @Override
        public Iterator<Object> iterator() {
            ensureLoaded();
            Iterator<Object> it = delegate.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    it.remove();
                    mutated();
                }
            };
        }

        @Override
        public int size() {
            ensureLoaded();
            return delegate.size();
        }

        @Override
        public boolean add(Object o) {
            ensureLoaded();
            boolean added = delegate.add(o);
            if (added) {
                mutated();
            }
            return added;
        }

        @Override
        public boolean remove(Object o) {
            ensureLoaded();
            boolean removed = delegate.remove(o);
            if (removed) {
                mutated();
            }
            return removed;
        }

        @Override
        public void clear() {
            ensureLoaded();
            if (!delegate.isEmpty()) {
                delegate.clear();
                mutated();
            }
        }
    }

    private static final class LazyOneToManyCollection extends AbstractCollection<Object> implements LazyCollection {
        private final LazyState state = new LazyState();
        private final Collection<Object> delegate = new ArrayList<>();
        private final Runnable mutationTracker;

        private LazyOneToManyCollection(Runnable mutationTracker) {
            this.mutationTracker = mutationTracker;
        }

        private void mutated() {
            if (mutationTracker != null) {
                mutationTracker.run();
            }
        }

        @Override
        public boolean hasMutationTracker() {
            return mutationTracker != null;
        }

        @Override
        public boolean isLoaded() {
            return state.isLoaded();
        }

        @Override
        public void attachLoader(Runnable loader) {
            state.attach(loader);
        }

        @Override
        public void ensureLoaded() {
            state.ensure();
        }

        @Override
        public void replaceContents(Collection<Object> items) {
            delegate.clear();
            delegate.addAll(items);
        }

        @Override
        public Iterator<Object> iterator() {
            ensureLoaded();
            Iterator<Object> it = delegate.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    it.remove();
                    mutated();
                }
            };
        }

        @Override
        public int size() {
            ensureLoaded();
            return delegate.size();
        }

        @Override
        public boolean add(Object o) {
            ensureLoaded();
            boolean added = delegate.add(o);
            if (added) {
                mutated();
            }
            return added;
        }

        @Override
        public boolean remove(Object o) {
            ensureLoaded();
            boolean removed = delegate.remove(o);
            if (removed) {
                mutated();
            }
            return removed;
        }

        @Override
        public void clear() {
            ensureLoaded();
            if (!delegate.isEmpty()) {
                delegate.clear();
                mutated();
            }
        }
    }

    private interface BidirectionalManagedCollection {
        // marker
    }

    private static final class ManyToManyCollection extends AbstractCollection<Object> implements BidirectionalManagedCollection, LazyCollection {
        private final Object owner;
        private final Field ownerField;
        private final Field inverseField;
        private final Collection<Object> delegate;
        private final RelationshipManager manager;
        private final LazyState state = new LazyState();

        private ManyToManyCollection(Object owner, Field ownerField, Field inverseField, Collection<Object> delegate, RelationshipManager manager) {
            this.owner = owner;
            this.ownerField = ownerField;
            this.inverseField = inverseField;
            this.delegate = delegate;
            this.manager = manager;
        }

        @Override
        public boolean isLoaded() {
            return state.isLoaded();
        }

        @Override
        public void attachLoader(Runnable loader) {
            state.attach(loader);
        }

        @Override
        public void ensureLoaded() {
            state.ensure();
        }

        @Override
        public void replaceContents(Collection<Object> items) {
            delegate.clear();
            delegate.addAll(items);
        }

        @Override
        public Iterator<Object> iterator() {
            ensureLoaded();
            Iterator<Object> it = delegate.iterator();
            return new Iterator<>() {
                private Object last;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    last = it.next();
                    return last;
                }

                @Override
                public void remove() {
                    it.remove();
                    if (manager.isBidirectionalSyncSuppressed() || last == null) {
                        return;
                    }
                    manager.removeInverseLink(owner, last, inverseField);
                }
            };
        }

        @Override
        public int size() {
            ensureLoaded();
            return delegate.size();
        }

        @Override
        public boolean add(Object o) {
            ensureLoaded();
            boolean added = delegate.add(o);
            if (!added || manager.isBidirectionalSyncSuppressed() || o == null) {
                return added;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.wrapManyToManyCollection(o, inverseField, ownerField);
            manager.ensureInverseContains(owner, o, inverseField);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            ensureLoaded();
            boolean removed = delegate.remove(o);
            if (!removed || manager.isBidirectionalSyncSuppressed() || o == null) {
                return removed;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.removeInverseLink(owner, o, inverseField);
            return true;
        }

        @Override
        public void clear() {
            ensureLoaded();
            if (manager.isBidirectionalSyncSuppressed()) {
                delegate.clear();
                return;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            List<Object> items = new ArrayList<>(delegate);
            delegate.clear();
            for (Object item : items) {
                if (item != null) {
                    manager.removeInverseLink(owner, item, inverseField);
                }
            }
        }
    }

    private static final class ManyToManyList extends AbstractList<Object> implements BidirectionalManagedCollection, LazyCollection {
        private final Object owner;
        private final Field ownerField;
        private final Field inverseField;
        private final List<Object> delegate;
        private final RelationshipManager manager;
        private final LazyState state = new LazyState();

        private ManyToManyList(Object owner, Field ownerField, Field inverseField, List<Object> delegate, RelationshipManager manager) {
            this.owner = owner;
            this.ownerField = ownerField;
            this.inverseField = inverseField;
            this.delegate = delegate;
            this.manager = manager;
        }

        @Override
        public boolean isLoaded() {
            return state.isLoaded();
        }

        @Override
        public void attachLoader(Runnable loader) {
            state.attach(loader);
        }

        @Override
        public void ensureLoaded() {
            state.ensure();
        }

        @Override
        public void replaceContents(Collection<Object> items) {
            delegate.clear();
            delegate.addAll(items);
        }

        @Override
        public Object get(int index) {
            ensureLoaded();
            return delegate.get(index);
        }

        @Override
        public int size() {
            ensureLoaded();
            return delegate.size();
        }

        @Override
        public Object set(int index, Object element) {
            ensureLoaded();
            Object old = delegate.set(index, element);
            if (manager.isBidirectionalSyncSuppressed()) {
                return old;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            if (old != null) {
                manager.removeInverseLink(owner, old, inverseField);
            }
            if (element != null) {
                manager.wrapManyToManyCollection(element, inverseField, ownerField);
                manager.ensureInverseContains(owner, element, inverseField);
            }
            return old;
        }

        @Override
        public void add(int index, Object element) {
            ensureLoaded();
            delegate.add(index, element);
            if (manager.isBidirectionalSyncSuppressed() || element == null) {
                return;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.wrapManyToManyCollection(element, inverseField, ownerField);
            manager.ensureInverseContains(owner, element, inverseField);
        }

        @Override
        public boolean add(Object element) {
            ensureLoaded();
            boolean added = delegate.add(element);
            if (!added || manager.isBidirectionalSyncSuppressed() || element == null) {
                return added;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.wrapManyToManyCollection(element, inverseField, ownerField);
            manager.ensureInverseContains(owner, element, inverseField);
            return true;
        }

        @Override
        public Object remove(int index) {
            ensureLoaded();
            Object removed = delegate.remove(index);
            if (manager.isBidirectionalSyncSuppressed() || removed == null) {
                return removed;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.removeInverseLink(owner, removed, inverseField);
            return removed;
        }

        @Override
        public boolean remove(Object o) {
            ensureLoaded();
            boolean removed = delegate.remove(o);
            if (!removed || manager.isBidirectionalSyncSuppressed() || o == null) {
                return removed;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.removeInverseLink(owner, o, inverseField);
            return true;
        }

        @Override
        public void clear() {
            ensureLoaded();
            if (manager.isBidirectionalSyncSuppressed()) {
                delegate.clear();
                return;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            List<Object> items = new ArrayList<>(delegate);
            delegate.clear();
            for (Object item : items) {
                if (item != null) {
                    manager.removeInverseLink(owner, item, inverseField);
                }
            }
        }
    }

    private static final class ManyToManySet extends AbstractSet<Object> implements BidirectionalManagedCollection, LazyCollection {
        private final Object owner;
        private final Field ownerField;
        private final Field inverseField;
        private final Set<Object> delegate;
        private final RelationshipManager manager;
        private final LazyState state = new LazyState();

        private ManyToManySet(Object owner, Field ownerField, Field inverseField, Set<Object> delegate, RelationshipManager manager) {
            this.owner = owner;
            this.ownerField = ownerField;
            this.inverseField = inverseField;
            this.delegate = delegate;
            this.manager = manager;
        }

        @Override
        public boolean isLoaded() {
            return state.isLoaded();
        }

        @Override
        public void attachLoader(Runnable loader) {
            state.attach(loader);
        }

        @Override
        public void ensureLoaded() {
            state.ensure();
        }

        @Override
        public void replaceContents(Collection<Object> items) {
            delegate.clear();
            delegate.addAll(items);
        }

        @Override
        public Iterator<Object> iterator() {
            ensureLoaded();
            Iterator<Object> it = delegate.iterator();
            return new Iterator<>() {
                private Object last;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    last = it.next();
                    return last;
                }

                @Override
                public void remove() {
                    it.remove();
                    if (manager.isBidirectionalSyncSuppressed() || last == null) {
                        return;
                    }
                    manager.removeInverseLink(owner, last, inverseField);
                }
            };
        }

        @Override
        public int size() {
            ensureLoaded();
            return delegate.size();
        }

        @Override
        public boolean add(Object o) {
            ensureLoaded();
            boolean added = delegate.add(o);
            if (!added || manager.isBidirectionalSyncSuppressed() || o == null) {
                return added;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.wrapManyToManyCollection(o, inverseField, ownerField);
            manager.ensureInverseContains(owner, o, inverseField);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            ensureLoaded();
            boolean removed = delegate.remove(o);
            if (!removed || manager.isBidirectionalSyncSuppressed() || o == null) {
                return removed;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            manager.removeInverseLink(owner, o, inverseField);
            return true;
        }

        @Override
        public void clear() {
            ensureLoaded();
            if (manager.isBidirectionalSyncSuppressed()) {
                delegate.clear();
                return;
            }
            manager.entityManager.markManyToManyDirty(owner, ownerField);
            List<Object> items = new ArrayList<>(delegate);
            delegate.clear();
            for (Object item : items) {
                if (item != null) {
                    manager.removeInverseLink(owner, item, inverseField);
                }
            }
        }
    }
}
