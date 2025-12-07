package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.Table;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds metadata information about an entity class using reflection.
 */
public class EntityMetadata {
    private static final Map<Class<?>, EntityMetadata> METADATA_CACHE = new ConcurrentHashMap<>();

    private final Class<?> entityClass;
    private final String tableName;
    private final Field idField;
    private final boolean autoIncrement;
    private final List<Field> fields;
    private final Map<String, Field> columnToField;
    private final Map<Field, String> fieldToColumn;

    private EntityMetadata(Class<?> entityClass) {
        this.entityClass = entityClass;
        
        // Validate entity annotation
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Class " + entityClass.getName() + " is not annotated with @Entity");
        }

        // Extract table name
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        this.tableName = (tableAnnotation != null) ? tableAnnotation.name() : entityClass.getSimpleName().toLowerCase();

        // Scan fields
        this.fields = new ArrayList<>();
        this.columnToField = new HashMap<>();
        this.fieldToColumn = new HashMap<>();
        Field foundIdField = null;
        boolean foundAutoIncrement = false;

        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);

            if (field.isAnnotationPresent(Id.class)) {
                foundIdField = field;
                Id idAnnotation = field.getAnnotation(Id.class);
                foundAutoIncrement = idAnnotation.autoIncrement();
                
                // Add ID field to fields list - it has a column
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    String columnName = column.name();
                    fields.add(field);
                    columnToField.put(columnName, field);
                    fieldToColumn.put(field, columnName);
                } else {
                    // Use field name as column name for ID
                    String columnName = field.getName();
                    fields.add(field);
                    columnToField.put(columnName, field);
                    fieldToColumn.put(field, columnName);
                }
            }

            // Handle column-mapped fields (regular columns and foreign keys)
            if (field.isAnnotationPresent(Column.class) && !field.isAnnotationPresent(Id.class)) {
                fields.add(field);
                Column column = field.getAnnotation(Column.class);
                String columnName = column.name();
                columnToField.put(columnName, field);
                fieldToColumn.put(field, columnName);
            } else if (field.isAnnotationPresent(de.t14d3.spool.annotations.ManyToOne.class)) {
                // Treat @ManyToOne fields as foreign key columns
                fields.add(field);
                // Convention: {fieldName}_id
                String columnName = field.getName() + "_id";
                columnToField.put(columnName, field);
                fieldToColumn.put(field, columnName);
            } else if (field.isAnnotationPresent(OneToMany.class)) {
                // @OneToMany fields don't have direct database columns but are part of relationships
                // We don't add them to the fields list as they're not direct columns
                // but we should track them for relationship management
            }
        }

        if (foundIdField == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() + " must have a field annotated with @Id");
        }

        this.idField = foundIdField;
        this.autoIncrement = foundAutoIncrement;
    }

    /**
     * Get or create metadata for the given entity class.
     */
    public static EntityMetadata of(Class<?> entityClass) {
        return METADATA_CACHE.computeIfAbsent(entityClass, EntityMetadata::new);
    }

    public Class<?> getEntityClass() {
        return entityClass;
    }

    public String getTableName() {
        return tableName;
    }

    public Field getIdField() {
        return idField;
    }

    public String getIdColumnName() {
        return fieldToColumn.get(idField);
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public List<Field> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public String getColumnName(Field field) {
        return fieldToColumn.get(field);
    }

    public Field getField(String columnName) {
        return columnToField.get(columnName);
    }

    /**
     * Gets the ID value from an entity instance.
     */
    public Object getIdValue(Object entity) {
        try {
            return idField.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access ID field", e);
        }
    }

    /**
     * Sets the ID value on an entity instance.
     */
    public void setIdValue(Object entity, Object value) {
        try {
            idField.set(entity, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot set ID field", e);
        }
    }

    /**
     * Gets the value of a field from an entity instance.
     */
    public Object getFieldValue(Object entity, Field field) {
        try {
            return field.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access field " + field.getName(), e);
        }
    }

    /**
     * Sets the value of a field on an entity instance.
     */
    public void setFieldValue(Object entity, Field field, Object value) {
        try {
            field.set(entity, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot set field " + field.getName(), e);
        }
    }

    /**
     * Creates a new instance of the entity.
     */
    public Object newInstance() {
        // Try to find parameterless constructor
        Constructor<?> constructor = null;
        for (Constructor<?> c : entityClass.getDeclaredConstructors()) {
            if (c.getParameterCount() == 0) {
                constructor = c;
                break;
            }
        }

        if (constructor == null) {
            throw new RuntimeException("Cannot find parameterless constructor for entity " + entityClass.getName());
        }

        try { return constructor.newInstance(); }
        catch (Exception e) { throw new RuntimeException("Cannot instantiate entity " + entityClass.getName(), e); }
    }
}
