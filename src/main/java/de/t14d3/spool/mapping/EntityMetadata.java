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
    private final List<String> columns;
    private final List<Field> fields;

    private EntityMetadata(Class<?> cls) {
        if (!cls.isAnnotationPresent(Entity.class)) {
            throw new OrmException("Class not marked @Entity: " + cls);
        }

        Table tbl = cls.getAnnotation(Table.class);
        this.tableName = (tbl != null) ? tbl.name() : cls.getSimpleName().toLowerCase();

        Field idFieldTemp = null;
        String idColumnTemp = null;
        List<String> cols = new ArrayList<>();
        List<Field> flds = new ArrayList<>();

        for (Field f : cls.getDeclaredFields()) {
            f.setAccessible(true);
            if (f.isAnnotationPresent(Id.class)) {
                if (idFieldTemp != null) {
                    throw new OrmException("Multiple @Id fields in class: " + cls);
                }
                idFieldTemp = f;
                Column col = f.getAnnotation(Column.class);
                idColumnTemp = (col != null && !col.name().isEmpty()) ? col.name() : f.getName();
            } else if (f.isAnnotationPresent(Column.class)) {
                Column col = f.getAnnotation(Column.class);
                cols.add(!col.name().isEmpty() ? col.name() : f.getName());
                flds.add(f);
            }
        }

        if (idFieldTemp == null) {
            throw new OrmException("No @Id field in class: " + cls);
        }

        this.idField = idFieldTemp;
        this.idColumn = idColumnTemp;
        this.columns = Collections.unmodifiableList(cols);
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

    public String getIdColumn() {
        return idColumn;
    }

    public Field getIdField() {
        return idField;
    }

    public List<String> getColumns() {
        return columns;
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
