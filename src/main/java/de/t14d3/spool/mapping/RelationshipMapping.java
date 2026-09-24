package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.FetchType;

import java.lang.reflect.Field;

/**
 * Represents a relationship mapping between entities.
 *
 * @param joinColumn FK column on the target table for a unidirectional
 *                   {@code @OneToMany} (blank otherwise); the owning
 *                   {@code @ManyToOne}/{@code @OneToOne} FK column is tracked
 *                   by {@link EntityMetadata}
 */
public record RelationshipMapping(Field field, Class<?> targetEntity, RelationshipType relationshipType, String mappedBy, FetchType fetch, CascadeType[] cascadeTypes, String joinColumn, String orderBy) {
    public RelationshipMapping(Field field, Class<?> targetEntity, RelationshipType relationshipType, String mappedBy, FetchType fetch, CascadeType[] cascadeTypes) {
        this(field, targetEntity, relationshipType, mappedBy, fetch, cascadeTypes, "", "");
    }

    public RelationshipMapping(Field field, Class<?> targetEntity, RelationshipType relationshipType, String mappedBy, FetchType fetch, CascadeType[] cascadeTypes, String joinColumn) {
        this(field, targetEntity, relationshipType, mappedBy, fetch, cascadeTypes, joinColumn, "");
    }

    public RelationshipMapping(Field field, Class<?> targetEntity, RelationshipType relationshipType, String mappedBy, FetchType fetch, CascadeType[] cascadeTypes, String joinColumn, String orderBy) {
        this.field = field;
        this.targetEntity = targetEntity;
        this.relationshipType = relationshipType;
        this.mappedBy = mappedBy;
        this.fetch = fetch != null ? fetch : FetchType.LAZY;
        this.cascadeTypes = cascadeTypes != null ? cascadeTypes : new CascadeType[0];
        this.joinColumn = joinColumn != null ? joinColumn : "";
        this.orderBy = orderBy != null ? orderBy : "";
    }

    public boolean isCascadable(CascadeType cascadeType) {
        for (CascadeType type : cascadeTypes) {
            if (type == cascadeType) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when this side owns the association storage: for collections, the
     * owning side that defines the join table / foreign key; for
     * {@code @OneToOne}, the side that carries the FK column.
     */
    public boolean isOwningSide() {
        return mappedBy == null || mappedBy.isBlank();
    }

    public enum RelationshipType {
        ONE_TO_ONE, MANY_TO_ONE, ONE_TO_MANY, MANY_TO_MANY
    }
}
