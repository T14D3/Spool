package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.MappedSuperclass;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.OneToOne;
import de.t14d3.spool.annotations.OrderBy;
import de.t14d3.spool.annotations.Table;
import de.t14d3.spool.annotations.UniqueConstraint;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds metadata information about an entity class using reflection.
 */
public class EntityMetadata {
    private static final Map<Class<?>, EntityMetadata> METADATA_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, Class<?>> ENTITY_NAMES = new ConcurrentHashMap<>();

    private final Class<?> entityClass;
    private final String tableName;
    private final List<Field> idFields;
    private final Field idField;
    private final Field versionField;
    private final boolean autoIncrement;
    private final List<Field> fields;
    private final List<Field> hierarchyFields;
    private final Map<String, Field> columnToField;
    private final Map<String, Field> nameToField;
    private final Map<Field, String> fieldToColumn;
    private final Map<Field, String> fieldToColumnType;
    private final List<UniqueConstraintDef> uniqueConstraints;

    private EntityMetadata(Class<?> entityClass) {
        this.entityClass = entityClass;

        // Validate entity annotation
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Class " + entityClass.getName() + " is not annotated with @Entity");
        }

        // Register resolvable entity names (simple name and FQCN) for HQL.
        ENTITY_NAMES.putIfAbsent(entityClass.getSimpleName(), entityClass);
        ENTITY_NAMES.putIfAbsent(entityClass.getName(), entityClass);

        this.nameToField = new HashMap<>();
        Map<String, Field> hierarchy = new LinkedHashMap<>();
        for (Class<?> mappedClass : collectMappedClasses(entityClass)) {
            for (Field declared : mappedClass.getDeclaredFields()) {
                if (Modifier.isStatic(declared.getModifiers()) || declared.isSynthetic()) {
                    continue;
                }
                declared.setAccessible(true);
                // Subclass declarations shadow superclass fields of the same name.
                hierarchy.put(declared.getName(), declared);
            }
        }
        this.hierarchyFields = List.copyOf(hierarchy.values());
        this.nameToField.putAll(hierarchy);

        // Extract table name
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        this.tableName = (tableAnnotation != null) ? tableAnnotation.name() : entityClass.getSimpleName().toLowerCase();

        // Scan fields
        this.fields = new ArrayList<>();
        this.columnToField = new HashMap<>();
        this.fieldToColumn = new HashMap<>();
        this.fieldToColumnType = new HashMap<>();
        this.uniqueConstraints = new ArrayList<>();
        List<Field> idFields = new ArrayList<>();
        boolean foundAutoIncrement = false;
        Field foundVersionField = null;

        for (Field field : hierarchyFields) {
            validateRelationshipField(field);

            if (field.isAnnotationPresent(de.t14d3.spool.annotations.Version.class)) {
                if (field.isAnnotationPresent(Id.class)) {
                    throw new IllegalArgumentException(
                            "Entity " + entityClass.getName() + " cannot use @Version on an @Id field");
                }
                Class<?> versionType = field.getType();
                if (!(versionType == int.class || versionType == long.class || versionType == short.class
                        || versionType == Integer.class || versionType == Long.class || versionType == Short.class)) {
                    throw new IllegalArgumentException(
                            "Entity " + entityClass.getName() + " has an unsupported @Version type "
                                    + versionType.getName() + "; use int, long, short or their wrappers");
                }
                if (foundVersionField != null) {
                    throw new IllegalArgumentException(
                            "Entity " + entityClass.getName() + " declares more than one @Version field");
                }
                foundVersionField = field;
            }

            if (field.isAnnotationPresent(Id.class)) {
                idFields.add(field);
                Id idAnnotation = field.getAnnotation(Id.class);
                if (idAnnotation.autoIncrement()) {
                    foundAutoIncrement = true;
                }

                // Add ID field to fields list - it has a column
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    String columnName = column.name();
                    String columnType = column.type().trim();
                    if (!columnType.isEmpty()) {
                        fieldToColumnType.put(field, columnType);
                    }
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
                String columnType = column.type().trim();
                if (!columnType.isEmpty()) {
                    fieldToColumnType.put(field, columnType);
                }
                columnToField.put(columnName, field);
                fieldToColumn.put(field, columnName);
            } else if (field.isAnnotationPresent(de.t14d3.spool.annotations.ManyToOne.class)
                    || isOwningToOne(field)) {
                // Treat owning @ManyToOne/@OneToOne fields as foreign key columns
                fields.add(field);
                JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                // Convention: {fieldName}_id (overridable via @JoinColumn(name="..."))
                String columnName = (joinColumn != null && joinColumn.name() != null && !joinColumn.name().isBlank())
                        ? joinColumn.name()
                        : field.getName() + "_id";
                columnToField.put(columnName, field);
                fieldToColumn.put(field, columnName);
            }
        }

        if (idFields.isEmpty()) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() + " must have a field annotated with @Id");
        }
        if (idFields.size() > 1 && foundAutoIncrement) {
            throw new IllegalArgumentException(
                    "Entity " + entityClass.getName() + " has a composite id; generated (autoIncrement) ids are only "
                            + "supported for single-column ids");
        }
        Set<String> idColumns = new HashSet<>();
        for (Field idFieldCandidate : idFields) {
            String column = null;
            if (idFieldCandidate.isAnnotationPresent(Column.class)) {
                column = idFieldCandidate.getAnnotation(Column.class).name();
            } else {
                column = idFieldCandidate.getName();
            }
            if (!idColumns.add(column.toLowerCase())) {
                throw new IllegalArgumentException(
                        "Entity " + entityClass.getName() + " maps more than one @Id field to column '" + column + "'");
            }
        }

        this.idFields = List.copyOf(idFields);
        this.idField = idFields.get(0);
        this.versionField = foundVersionField;
        this.autoIncrement = idFields.size() == 1 && foundAutoIncrement;

        // Collect single-column unique constraints (@Column(unique = true)).
        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            if (column == null || !column.unique()) continue;
            String columnName = fieldToColumn.get(field);
            if (columnName == null) continue;
            String constraintName = deriveSingleColumnUniqueName(tableName, columnName);
            uniqueConstraints.add(new UniqueConstraintDef(constraintName, List.of(columnName)));
        }

        // Collect table-level composite unique constraints (@Table(uniqueConstraints = ...)).
        if (tableAnnotation != null) {
            for (UniqueConstraint uc : tableAnnotation.uniqueConstraints()) {
                String name = uc.name();
                if (name == null || name.isBlank()) {
                    name = deriveCompositeUniqueName(tableName, uc.columns());
                }
                uniqueConstraints.add(new UniqueConstraintDef(name, List.of(uc.columns())));
            }
        }
    }

    /**
     * Derives a stable, deterministic index name for a single-column
     * {@code @Column(unique = true)} entry. The same column always produces
     * the same name, so a re-deploy never tries to re-create the index.
     */
    private static String deriveSingleColumnUniqueName(String tableName, String columnName) {
        return "uniq_" + tableName + "_" + columnName;
    }

    /**
     * Derives a stable, deterministic index name for a composite
     * {@code @Table(uniqueConstraints = ...)} entry that did not specify a
     * name. Columns are sorted to keep the name stable across reorderings.
     */
    private static String deriveCompositeUniqueName(String tableName, String[] columns) {
        String[] sorted = columns.clone();
        java.util.Arrays.sort(sorted);
        return "uniq_" + tableName + "_" + String.join("_", sorted);
    }

    /**
     * Validates one field's relationship annotation. A mapping Spool cannot
     * honor must fail when the metadata is built; it is never dropped
     * silently at query time.
     * <p>
     * This is the runtime half of the explicitness contract and the logic the
     * (future) annotation processor must share so the two cannot disagree.
     */
    private static void validateRelationshipField(Field field) {
        Class<?> declaring = field.getDeclaringClass();

        OneToMany oneToMany = field.getAnnotation(OneToMany.class);
        if (oneToMany != null) {
            if (!Collection.class.isAssignableFrom(field.getType())) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToMany field '"
                        + field.getName() + "' must be a Collection, found " + field.getType().getName());
            }
            String mappedBy = oneToMany.mappedBy();
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
            if (mappedBy == null || mappedBy.isBlank()) {
                if (joinColumn == null || joinColumn.name().isBlank()) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToMany field '"
                            + field.getName() + "' has a blank mappedBy and no @JoinColumn; unidirectional "
                            + "@OneToMany needs @JoinColumn(name=\"...\") to name the FK column on "
                            + oneToMany.targetEntity().getSimpleName());
                }
                validateOrderBy(field, oneToMany.targetEntity());
                return;
            }
            if (joinColumn != null) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToMany field '"
                        + field.getName() + "' declares both mappedBy='" + mappedBy + "' and @JoinColumn; "
                        + "@JoinColumn is only used for unidirectional @OneToMany (blank mappedBy)");
            }
            Field inverse = findField(oneToMany.targetEntity(), mappedBy);
            if (inverse == null) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToMany field '"
                        + field.getName() + "' declares mappedBy='" + mappedBy + "' but "
                        + oneToMany.targetEntity().getName() + " has no such field");
            }
            if (!inverse.isAnnotationPresent(ManyToOne.class)) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToMany field '"
                        + field.getName() + "' declares mappedBy='" + mappedBy + "' but "
                        + oneToMany.targetEntity().getName() + "." + mappedBy
                        + " is not annotated with @ManyToOne");
            }
            validateOrderBy(field, oneToMany.targetEntity());
            return;
        }

        ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
        if (manyToMany != null) {
            if (!Collection.class.isAssignableFrom(field.getType())) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @ManyToMany field '"
                        + field.getName() + "' must be a Collection, found " + field.getType().getName());
            }
            Class<?> target = collectionElementType(field);
            if (target == Object.class) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @ManyToMany field '"
                        + field.getName() + "' must declare its element type (Collection<T>); "
                        + "a raw collection would silently load nothing");
            }
            String mappedBy = manyToMany.mappedBy();
            if (mappedBy != null && !mappedBy.isBlank()) {
                Field inverse = findField(target, mappedBy);
                if (inverse == null) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @ManyToMany field '"
                            + field.getName() + "' declares mappedBy='" + mappedBy + "' but "
                            + target.getName() + " has no such field");
                }
                if (!inverse.isAnnotationPresent(ManyToMany.class)
                        || !Collection.class.isAssignableFrom(inverse.getType())) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @ManyToMany field '"
                            + field.getName() + "' declares mappedBy='" + mappedBy + "' but "
                            + target.getName() + "." + mappedBy + " is not a @ManyToMany collection");
                }
            }
            validateOrderBy(field, target);
            return;
        }

        if (field.isAnnotationPresent(OneToOne.class)) {
            OneToOne oneToOne = field.getAnnotation(OneToOne.class);
            if (oneToOne.mappedBy() != null && !oneToOne.mappedBy().isBlank()) {
                Field owner = findField(field.getType(), oneToOne.mappedBy());
                if (owner == null) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToOne field '"
                            + field.getName() + "' declares mappedBy='" + oneToOne.mappedBy() + "' but "
                            + field.getType().getName() + " has no such field");
                }
                if (!owner.isAnnotationPresent(OneToOne.class)) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToOne field '"
                            + field.getName() + "' declares mappedBy='" + oneToOne.mappedBy() + "' but "
                            + field.getType().getName() + "." + oneToOne.mappedBy()
                            + " is not annotated with @OneToOne");
                }
                OneToOne ownerAnnotation = owner.getAnnotation(OneToOne.class);
                if (ownerAnnotation.mappedBy() != null && !ownerAnnotation.mappedBy().isBlank()) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToOne field '"
                            + field.getName() + "' declares mappedBy='" + oneToOne.mappedBy() + "' but "
                            + field.getType().getName() + "." + oneToOne.mappedBy()
                            + " is itself an inverse side (its mappedBy is set); mappedBy must point at the owning side");
                }
                if (oneToOne.orphanRemoval()) {
                    throw new IllegalArgumentException("Entity " + declaring.getName() + " @OneToOne field '"
                            + field.getName() + "' is the inverse side; orphanRemoval is only supported on the owning side");
                }
            }
            return;
        }

        if (field.isAnnotationPresent(OrderBy.class)) {
            throw new IllegalArgumentException("Entity " + declaring.getName() + " @OrderBy is only valid on "
                    + "@OneToMany/@ManyToMany fields, but '" + field.getName() + "' is not a collection relationship");
        }
    }

    private static void validateOrderBy(Field field, Class<?> targetEntity) {
        OrderBy orderBy = field.getAnnotation(OrderBy.class);
        if (orderBy == null || orderBy.value().isBlank()) {
            return;
        }
        Class<?> declaring = field.getDeclaringClass();
        for (String segment : orderBy.value().split(",")) {
            String[] tokens = segment.trim().split("\\s+");
            if (tokens.length == 0 || tokens.length > 2
                    || (tokens.length == 2
                    && !tokens[1].equalsIgnoreCase("ASC") && !tokens[1].equalsIgnoreCase("DESC"))) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OrderBy on field '"
                        + field.getName() + "' must be 'field [ASC|DESC][, ...]', found '" + segment.trim() + "'");
            }
            Field ordered = findField(targetEntity, tokens[0]);
            if (ordered == null) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OrderBy on field '"
                        + field.getName() + "' references unknown field " + targetEntity.getName() + "." + tokens[0]);
            }
            boolean columnMapped = ordered.isAnnotationPresent(Column.class)
                    || ordered.isAnnotationPresent(Id.class)
                    || ordered.isAnnotationPresent(de.t14d3.spool.annotations.ManyToOne.class)
                    || isOwningToOne(ordered);
            if (!columnMapped) {
                throw new IllegalArgumentException("Entity " + declaring.getName() + " @OrderBy on field '"
                        + field.getName() + "' references " + targetEntity.getName() + "." + tokens[0]
                        + ", which is not a mapped column");
            }
        }
    }

    /**
     * True for single-reference association fields that own a foreign-key
     * column on this entity's table: {@code @ManyToOne} and owning-side
     * {@code @OneToOne} (mappedBy blank).
     */
    public static boolean isOwningToOne(Field field) {
        if (field.isAnnotationPresent(de.t14d3.spool.annotations.ManyToOne.class)) {
            return true;
        }
        OneToOne oneToOne = field.getAnnotation(OneToOne.class);
        return oneToOne != null && (oneToOne.mappedBy() == null || oneToOne.mappedBy().isBlank());
    }

    /**
     * Resolves the element type of a {@code Collection<T>} field, returning
     * {@code Object.class} when the field is not a parameterized collection.
     */
    public static Class<?> collectionElementType(Field field) {
        Type t = field.getGenericType();
        if (!(t instanceof ParameterizedType pt)) {
            return Object.class;
        }
        Type[] args = pt.getActualTypeArguments();
        if (args.length != 1) {
            return Object.class;
        }
        Type arg = args[0];
        if (arg instanceof Class<?> c) {
            return c;
        }
        if (arg instanceof ParameterizedType apt && apt.getRawType() instanceof Class<?> c) {
            return c;
        }
        return Object.class;
    }

    /**
     * Finds a declared field by name on {@code type} or any of its
     * superclasses (base-first lookup, subclass declarations shadow
     * superclass ones for the same name). Returns null when not found.
     */
    public static Field findField(Class<?> type, String name) {
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            try {
                Field field = c.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
                // continue up the hierarchy
            }
        }
        return null;
    }

    /**
     * Builds the mapping class chain: the entity plus its
     * {@code @MappedSuperclass} ancestors, base-first. Superclasses that are
     * neither mapped nor field-less are rejected so their fields cannot be
     * dropped silently.
     */
    private static List<Class<?>> collectMappedClasses(Class<?> entityClass) {
        List<Class<?>> chain = new ArrayList<>();
        for (Class<?> c = entityClass; c != null && c != Object.class; c = c.getSuperclass()) {
            chain.add(c);
        }
        Collections.reverse(chain); // base-first

        List<Class<?>> mapped = new ArrayList<>(chain.size());
        for (Class<?> c : chain) {
            if (c == entityClass || c.isAnnotationPresent(MappedSuperclass.class)) {
                mapped.add(c);
                continue;
            }
            if (c.isAnnotationPresent(Entity.class)) {
                throw new IllegalArgumentException("Entity " + entityClass.getName() + " extends entity "
                        + c.getName() + "; entity inheritance is not supported. Share fields through "
                        + "@MappedSuperclass instead.");
            }
            Field offending = firstPersistentField(c);
            if (offending != null) {
                throw new IllegalArgumentException("Entity " + entityClass.getName() + " extends " + c.getName()
                        + ", which is neither @Entity nor @MappedSuperclass; its field '" + offending.getName()
                        + "' would be ignored. Annotate the superclass with @MappedSuperclass.");
            }
        }
        return List.copyOf(mapped);
    }

    private static Field firstPersistentField(Class<?> type) {
        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                continue;
            }
            return field;
        }
        return null;
    }

    /**
     * Get or create metadata for the given entity class.
     */
    public static EntityMetadata of(Class<?> entityClass) {
        return METADATA_CACHE.computeIfAbsent(entityClass, EntityMetadata::new);
    }

    /**
     * Resolve an entity class by its simple name or fully qualified class name.
     * Entity classes become resolvable as soon as their metadata is built
     * (which happens both lazily and eagerly via
     * {@code EntityManager.registerEntities}). Returns null when the name
     * cannot be resolved.
     */
    public static Class<?> resolveEntityClass(String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        Class<?> registered = ENTITY_NAMES.get(name);
        if (registered != null) {
            return registered;
        }
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    public Class<?> getEntityClass() {
        return entityClass;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * The single {@code @Id} field. Only valid for entities with a
     * single-column id; throws for composite ids.
     */
    public Field getIdField() {
        if (idFields.size() != 1) {
            throw new IllegalStateException(
                    "Entity " + entityClass.getName() + " has a composite id (" + idFields.size()
                            + " @Id fields); getIdField() is only valid for single-column ids");
        }
        return idField;
    }

    /**
     * All {@code @Id} fields, in declaration order.
     */
    public List<Field> getIdFields() {
        return idFields;
    }

    public boolean isCompositeId() {
        return idFields.size() > 1;
    }

    /**
     * True when the given field is one of the entity's {@code @Id} fields.
     */
    public boolean isIdField(Field field) {
        return idFields.contains(field);
    }

    /**
     * The {@code @Version} field, or null when the entity does not use
     * optimistic locking.
     */
    public Field getVersionField() {
        return versionField;
    }

    public boolean hasVersion() {
        return versionField != null;
    }

    /**
     * Single id column name. Only valid for entities with a single-column id.
     */
    public String getIdColumnName() {
        return fieldToColumn.get(getIdField());
    }

    /**
     * All id column names, in {@code @Id} field order.
     */
    public List<String> getIdColumnNames() {
        List<String> columns = new ArrayList<>(idFields.size());
        for (Field field : idFields) {
            columns.add(fieldToColumn.get(field));
        }
        return columns;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public List<Field> getFields() {
        return Collections.unmodifiableList(fields);
    }

    /**
     * All instance fields declared on the entity or any of its
     * {@code @MappedSuperclass} ancestors, base-first. Subclass declarations
     * shadow superclass fields with the same name. This is the field set used
     * for relationship scanning; {@link #getFields()} is the subset that maps
     * to direct table columns.
     */
    public List<Field> getHierarchyFields() {
        return hierarchyFields;
    }

    public String getColumnName(Field field) {
        return fieldToColumn.get(field);
    }

    public Field getField(String columnName) {
        return columnToField.get(columnName);
    }

    /**
     * Get a declared field by its Java name (not column name), searching the
     * entity class and its {@code @MappedSuperclass} ancestors. Returns null
     * when no such field exists.
     */
    public Field getFieldByName(String fieldName) {
        return nameToField.get(fieldName);
    }

    /**
     * Gets the explicit column type for the given field, if specified.
     * Returns null if no explicit type was specified.
     */
    public String getColumnType(Field field) {
        return fieldToColumnType.get(field);
    }

    /**
     * Returns the unique constraints declared on the entity: single-column
     * entries from {@code @Column(unique = true)} and composite entries from
     * {@code @Table(uniqueConstraints = ...)}.
     */
    public List<UniqueConstraintDef> getUniqueConstraints() {
        return Collections.unmodifiableList(uniqueConstraints);
    }

    /**
     * Gets the identity of an entity instance, covering both single-column and
     * composite ids.
     */
    public EntityId getEntityId(Object entity) {
        return EntityId.of(this, entity);
    }

    /**
     * Builds an identity from a user-supplied key (raw value, {@code Map} or
     * entity instance). See {@link EntityId#from(EntityMetadata, Object)}.
     */
    public EntityId getEntityIdFrom(Object key) {
        return EntityId.from(this, key);
    }

    /**
     * All id values from an entity instance, in {@code @Id} field order.
     */
    public List<Object> getIdValues(Object entity) {
        List<Object> values = new ArrayList<>(idFields.size());
        for (Field field : idFields) {
            values.add(getFieldValue(entity, field));
        }
        return values;
    }

    /**
     * Gets the single ID value from an entity instance. Throws for composite
     * ids; use {@link #getEntityId(Object)} instead.
     */
    public Object getIdValue(Object entity) {
        if (idFields.size() != 1) {
            throw new IllegalStateException(
                    "Entity " + entityClass.getName() + " has a composite id; use getEntityId() or getIdValues()");
        }
        return getFieldValue(entity, idField);
    }

    /**
     * Sets the ID value on an entity instance.
     * <p>
     * If the value is null and the ID field is a primitive type, the call is
     * silently skipped to avoid {@link IllegalArgumentException} from
     * {@link java.lang.reflect.Field#set(Object, Object)}. The field retains
     * its JVM default (0, 0L, 0.0, false, etc.).
     */
    public void setIdValue(Object entity, Object value) {
        try {
            if (value == null && idField.getType().isPrimitive()) {
                return;
            }
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
     * <p>
     * If the value is null and the target field is a primitive type, the call
     * is silently skipped to avoid {@link IllegalArgumentException}. The
     * field retains its JVM default.
     */
    public void setFieldValue(Object entity, Field field, Object value) {
        try {
            if (value == null && field.getType().isPrimitive()) {
                return;
            }
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
