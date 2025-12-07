package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.CascadeType;

import java.lang.reflect.Field;

/**
 * Represents a relationship mapping between entities.
 */
public record RelationshipMapping(Field field, Class<?> targetEntity, RelationshipType relationshipType, String mappedBy, boolean fetch, CascadeType[] cascadeTypes) {
    public RelationshipMapping(Field field, Class<?> targetEntity, RelationshipType relationshipType, String mappedBy, boolean fetch, CascadeType[] cascadeTypes) {
        this.field = field;
        this.targetEntity = targetEntity;
        this.relationshipType = relationshipType;
        this.mappedBy = mappedBy;
        this.fetch = fetch;
        this.cascadeTypes = cascadeTypes != null ? cascadeTypes : new CascadeType[0];
    }

    public boolean isCascadable(CascadeType cascadeType) {
        for (CascadeType type : cascadeTypes) {
            if (type == cascadeType) {
                return true;
            }
        }
        return false;
    }

    public enum RelationshipType {
        ONE_TO_ONE, MANY_TO_ONE, ONE_TO_MANY, MANY_TO_MANY
    }
}