package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;
import de.t14d3.spool.exceptions.OrmException;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EntityMetadata {
    private static final Map<Class<?>, EntityMetadata> CACHE = new ConcurrentHashMap<>();

    private final String tableName;
    private final String idColumn;
    private final Field idField;
    private final String idType; // Add field to store ID type
    private boolean autoIncrement; // New field to store auto-increment flag
    private final List<String> columns;
    private final List<String> columnTypes;
    private final List<Field> fields;

    private EntityMetadata(Class<?> cls) {
        if (!cls.isAnnotationPresent(Entity.class)) {
            throw new OrmException("Class not marked @Entity: " + cls);
        }

        Table tbl = cls.getAnnotation(Table.class);
        this.tableName = (tbl != null) ? tbl.name() : cls.getSimpleName().toLowerCase();

        Field idFieldTemp = null;
        String idColumnTemp = null;
        String idTypeTemp = null;
        List<String> cols = new ArrayList<>();
        List<String> types = new ArrayList<>();
        List<Field> flds = new ArrayList<>();

        for (Field f : cls.getDeclaredFields()) {
            f.setAccessible(true);
            if (f.isAnnotationPresent(Id.class)) {
                if (idFieldTemp != null) {
                    throw new OrmException("Multiple @Id fields in class: " + cls);
                }
                idFieldTemp = f;
                Id idAnnotation = f.getAnnotation(Id.class);
                idTypeTemp = idAnnotation.type();
                this.autoIncrement = idAnnotation.autoIncrement(); // Check for auto-increment
                Column col = f.getAnnotation(Column.class);
                idColumnTemp = (col != null && !col.name().isEmpty()) ? col.name() : f.getName();
            } else if (f.isAnnotationPresent(Column.class)) {
                Column col = f.getAnnotation(Column.class);
                cols.add(!col.name().isEmpty() ? col.name() : f.getName());
                types.add(!col.type().isEmpty() ? col.type() : "VARCHAR(255)"); // Default type
                flds.add(f);
            }
        }

        if (idFieldTemp == null) {
            throw new OrmException("No @Id field in class: " + cls);
        }

        this.idField = idFieldTemp;
        this.idColumn = idColumnTemp;
        this.idType = idTypeTemp; // Store the ID type
        this.columns = Collections.unmodifiableList(cols);
        this.columnTypes = Collections.unmodifiableList(types);
        this.fields = Collections.unmodifiableList(flds);
    }

    public static Set<Class<?>> loadedClasses() {
        return Collections.unmodifiableSet(CACHE.keySet());
    }

    public static EntityMetadata of(Class<?> cls) {
        return CACHE.computeIfAbsent(cls, EntityMetadata::new);
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isAutoIncrement() { return autoIncrement; } // New getter for auto-increment

    public String getIdColumn() {
        return idColumn;
    }

    public Field getIdField() {
        return idField;
    }

    public String getIdType() {
        return idType;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Object idValue(Object entity) {
        try {
            return idField.get(entity);
        } catch (Exception e) {
            throw new OrmException(e);
        }
    }

    public List<Object> values(Object entity) {
        List<Object> vals = new ArrayList<>();
        for (Field f : fields) {
            try {
                vals.add(f.get(entity));
            } catch (Exception e) {
                throw new OrmException(e);
            }
        }
        return vals;
    }
}
