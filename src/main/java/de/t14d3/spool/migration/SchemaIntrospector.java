package de.t14d3.spool.migration;



import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinTable;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.mapping.TypeMapper;
import de.t14d3.spool.mapping.UniqueConstraintDef;
import de.t14d3.spool.query.Dialect;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Introspects database schema and entity metadata to build schema definitions.
 * This implementation is defensive and database-agnostic: it prefers DatabaseMetaData,
 * but falls back to vendor-specific queries when necessary (notably for SQLite).
 */
public class SchemaIntrospector {
    /**
     * Matches a trailing, single-number length in a SQL type, e.g. the "(36)" of
     * "VARCHAR(36)". Compound precisions such as "DECIMAL(38, 18)" do not match.
     */
    private static final java.util.regex.Pattern EMBEDDED_TYPE_LENGTH =
            java.util.regex.Pattern.compile("\\s*\\(\\s*(\\d+)\\s*\\)\\s*$");

    private final Connection connection;
    private final Dialect dialect;

    public SchemaIntrospector(Connection connection) {
        this.connection = connection;
        String url = null;
        try {
            DatabaseMetaData md = connection.getMetaData();
            url = md.getURL();
        } catch (SQLException ignored) {
            // If metadata fails, keep url null and default to GENERIC
        }
        this.dialect = Dialect.detectFromUrl(url);
    }

    /**
     * Get all table names from the database (user tables only).
     * Returns names in lowercase for easier, case-insensitive comparisons.
     */
    public Set<String> getTableNames() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        Set<String> tables = new HashSet<>();

        // Try the portable metadata approach first
        try {
            String catalog = catalogForMetadata();
            String schema = resolveSchemaForMetadata();
            // Request only "TABLE" and include "VIEW" as well in some DBs user tables may be views
            try (ResultSet rs = metaData.getTables(catalog, schema, "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String tableSchema = rs.getString("TABLE_SCHEM"); // may be null for sqlite
                    if (tableName == null) continue;

                    // Filter out rows the driver may have returned for other
                    // databases (MySQL ignores a null catalog) and system schemas.
                    if (!matchesCatalog(rs.getString("TABLE_CAT"), catalog)) continue;
                    if (isSystemSchema(tableSchema)) continue;
                    if (isSystemTableName(tableName)) continue;

                    tables.add(tableName.toLowerCase());
                }
            }
            return tables;
        } catch (SQLException e) {
            // Some drivers (older sqlite drivers) can throw on metadata calls with schema param.
            // Fallback to vendor-specific methods (SQLite) or rethrow for unknown drivers.
            if (dialect == Dialect.SQLITE) {
                return getSqliteTableNamesFallback();
            }
            throw e;
        }
    }

    /**
     * Check if a table exists (case-insensitive).
     */
    public boolean tableExists(String tableName) throws SQLException {
        if (tableName == null || tableName.isEmpty()) return false;

        // Try quick metadata lookup first
        DatabaseMetaData metaData = connection.getMetaData();
        String catalog = catalogForMetadata();
        String schema = resolveSchemaForMetadata();

        try {
            // Use getTables with explicit name pattern to be efficient
            try (ResultSet rs = metaData.getTables(catalog, schema, tableName, new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tname = rs.getString("TABLE_NAME");
                    String tschema = rs.getString("TABLE_SCHEM");
                    if (tname != null && tname.equalsIgnoreCase(tableName)
                            && !isSystemSchema(tschema)
                            && matchesCatalog(rs.getString("TABLE_CAT"), catalog)) {
                        return true;
                    }
                }
            }
            // Try case-insensitive check via getTableNames if the above returned nothing
            Set<String> tables = getTableNames();
            return tables.contains(tableName.toLowerCase());
        } catch (SQLException e) {
            if (dialect == Dialect.SQLITE) {
                // SQLite fallback
                return sqliteTableExistsFallback(tableName);
            }
            throw e;
        }
    }

    /**
     * Get the schema definition of a table. Returns null if the table doesn't exist or has no columns.
     */
    public TableDefinition getTableDefinition(String tableName) throws SQLException {
        if (tableName == null || tableName.isEmpty()) return null;

        DatabaseMetaData metaData = connection.getMetaData();
        String catalog = catalogForMetadata();
        String schema = resolveSchemaForMetadata();

        // Try metadata-based approach
        try {
            // First locate the actual table name and schema via getTables
            String actualTable = null;
            String foundSchema = null;
            String foundCatalog = catalog;
            try (ResultSet tablesRs = metaData.getTables(catalog, schema, tableName, new String[]{"TABLE", "VIEW"})) {
                while (tablesRs.next()) {
                    String tname = tablesRs.getString("TABLE_NAME");
                    String tschema = tablesRs.getString("TABLE_SCHEM");
                    String tcatalog = tablesRs.getString("TABLE_CAT");
                    if (tname != null && tname.equalsIgnoreCase(tableName)
                            && !isSystemSchema(tschema)
                            && matchesCatalog(tcatalog, catalog)) {
                        actualTable = tname;
                        foundSchema = tschema;
                        foundCatalog = tcatalog != null ? tcatalog : catalog;
                        break;
                    }
                }
            }

            if (actualTable == null) {
                // maybe uppercase/lowercase variant
                try (ResultSet tablesRs = metaData.getTables(catalog, schema, tableName.toUpperCase(), new String[]{"TABLE", "VIEW"})) {
                    while (tablesRs.next()) {
                        String tname = tablesRs.getString("TABLE_NAME");
                        String tschema = tablesRs.getString("TABLE_SCHEM");
                        String tcatalog = tablesRs.getString("TABLE_CAT");
                        if (tname != null && tname.equalsIgnoreCase(tableName)
                                && !isSystemSchema(tschema)
                                && matchesCatalog(tcatalog, catalog)) {
                            actualTable = tname;
                            foundSchema = tschema;
                            foundCatalog = tcatalog != null ? tcatalog : catalog;
                            break;
                        }
                    }
                }
            }

            if (actualTable == null) {
                // table not found via metadata
                if (dialect == Dialect.SQLITE) {
                    return getTableDefinitionSqliteFallback(tableName);
                }
                return null;
            }

            // gather primary keys
            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet pkRs = metaData.getPrimaryKeys(foundCatalog, foundSchema, actualTable)) {
                while (pkRs.next()) {
                    String col = pkRs.getString("COLUMN_NAME");
                    if (col != null) primaryKeys.add(col.toLowerCase());
                }
            } catch (SQLException ignored) {
                // Some drivers may not return PK info via metadata
            }

            // gather columns
            List<ColumnDefinition> columns = new ArrayList<>();
            try (ResultSet colRs = metaData.getColumns(foundCatalog, foundSchema, actualTable, "%")) {
                while (colRs.next()) {
                    // Filter out system columns if any
                    String tname = colRs.getString("TABLE_NAME");
                    String tschema = colRs.getString("TABLE_SCHEM");
                    if (tname == null || isSystemSchema(tschema)
                            || !matchesCatalog(colRs.getString("TABLE_CAT"), foundCatalog)) continue;
                    columns.add(buildColumnFromResultSet(colRs, primaryKeys));
                }
            }

            if (columns.isEmpty()) {
                // If metadata returned no columns, attempt sqlite fallback or return null
                if (dialect == Dialect.SQLITE) {
                    return getTableDefinitionSqliteFallback(actualTable);
                }
                return null;
            }

            TableDefinition table = new TableDefinition(actualTable);
            for (ColumnDefinition c : columns) table.addColumn(c);
            collectUniqueConstraints(metaData, foundCatalog, foundSchema, actualTable, table);
            return table;

        } catch (SQLException e) {
            if (dialect == Dialect.SQLITE) {
                return getTableDefinitionSqliteFallback(tableName);
            }
            throw e;
        }
    }

    // -----------------------
    // Helper methods / fallbacks
    // -----------------------

    private String resolveSchemaForMetadata() {
        // Choose schema argument for DatabaseMetaData calls depending on dialect and driver behavior.
        // H2: use connection.getSchema() or "PUBLIC" when null.
        // PostgreSQL: leave schema null (the driver resolves the searched schema).
        // SQLite: should pass null to avoid driver errors.
        try {
            if (dialect == Dialect.H2) {
                String s = connection.getSchema();
                if (s == null) return "PUBLIC";
                return s;
            }
        } catch (Throwable ignored) {
            // ignore and fall through
        }
        // For SQLite and generic, use null
        return null;
    }

    /**
     * Catalog argument for {@link DatabaseMetaData} lookups.
     *
     * <p>MySQL and MariaDB treat the database as the JDBC <em>catalog</em>. With a
     * null catalog the driver happily returns tables from <em>every</em> database
     * on the server, so a sibling schema (a test copy, a second Rapunzel install,
     * any other application that happens to use the same table names) makes Spool
     * believe the target database already has the table. The generated "migration"
     * is then a differential ALTER against a table that does not exist in the
     * target schema, and a genuinely empty database can never be initialised.
     * Supplying the catalog pins every lookup to the connected database.</p>
     *
     * @return the catalog to pass to metadata lookups, or null when the dialect
     *         does not separate catalog and schema (or the driver reports none)
     */
    private String catalogForMetadata() {
        if (dialect != Dialect.MYSQL) {
            return null;
        }
        try {
            return connection.getCatalog();
        } catch (SQLException ignored) {
            return null;
        }
    }

    /**
     * Guards against drivers that ignore the catalog argument: a row whose
     * reported catalog differs from the connected one is not part of this schema.
     *
     * @param tableCatalog the {@code TABLE_CAT} of a metadata row (may be null)
     * @param catalog      the catalog the lookup asked for (may be null)
     * @return true when the row belongs to the requested catalog
     */
    private static boolean matchesCatalog(String tableCatalog, String catalog) {
        if (catalog == null || tableCatalog == null) return true;
        return tableCatalog.equalsIgnoreCase(catalog);
    }

    private boolean isSystemSchema(String schema) {
        if (schema == null) return false;
        String s = schema.toUpperCase(Locale.ROOT);
        return s.startsWith("INFORMATION_SCHEMA") || s.startsWith("PG_") || s.equals("SYS") || s.equals("SYSTEM");
    }

    private boolean isSystemTableName(String tableName) {
        if (tableName == null) return true;
        String tn = tableName.toLowerCase(Locale.ROOT);
        // SQLite internal tables start with sqlite_
        if (tn.startsWith("sqlite_")) return true;
        // H2 information schema tables
        if (tn.startsWith("information_schema")) return true;
        return false;
    }

    private Set<String> getSqliteTableNamesFallback() throws SQLException {
        Set<String> result = new HashSet<>();
        String sql = "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name = rs.getString(1);
                if (name != null) result.add(name.toLowerCase());
            }
        }
        return result;
    }

    private boolean sqliteTableExistsFallback(String tableName) throws SQLException {
        String sql = "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private TableDefinition getTableDefinitionSqliteFallback(String tableName) throws SQLException {
        // Check existence
        if (!sqliteTableExistsFallback(tableName)) return null;

        List<ColumnDefinition> columns = new ArrayList<>();

        // Use PRAGMA table_info to get column names, types, notnull, pk, default
        String pragma = "PRAGMA table_info(\"" + tableName.replace("\"", "\"\"") + "\")";
        try (PreparedStatement ps = connection.prepareStatement(pragma);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String colName = rs.getString("name");
                String type = rs.getString("type"); // e.g. "VARCHAR(255)" or "INTEGER"
                int notnull = rs.getInt("notnull"); // 0 or 1
                String dfltValue = rs.getString("dflt_value");
                int pk = rs.getInt("pk"); // 0 or 1 (or >1 for composite)

                ColumnDefinition.Builder b = new ColumnDefinition.Builder()
                        .name(colName)
                        .sqlType(type == null ? "TEXT" : type)
                        .nullable(notnull == 0)
                        .primaryKey(pk != 0)
                        .autoIncrement(false) // SQLite doesn't expose auto-increment in pragma; detect from type if possible
                        .defaultValue(dfltValue);

                int length = parseLengthFromType(type);
                if (length > 0) b.length(length);

                // Rough auto-increment detection: in SQLite an INTEGER PRIMARY KEY is alias for rowid and can be autoinc.
                if (pk != 0 && type != null && type.equalsIgnoreCase("INTEGER")) {
                    b.autoIncrement(true);
                }

                columns.add(b.build());
            }
        }

        if (columns.isEmpty()) return null;
        TableDefinition table = new TableDefinition(tableName);
        for (ColumnDefinition c : columns) table.addColumn(c);
        collectSqliteUniqueConstraints(tableName, table);
        return table;
    }

    /**
     * Reads existing unique indexes for a table from JDBC metadata
     * ({@link DatabaseMetaData#getIndexInfo}) and adds them to
     * {@code table}. The primary key is excluded since it is already
     * tracked via {@code getPrimaryKeys}, and drivers that surface the
     * primary-key index here (H2) would otherwise produce spurious
     * diff entries.
     *
     * <p>The order of columns within each index follows the result set's
     * {@code ORDINAL_POSITION}, which is what
     * {@code CREATE UNIQUE INDEX} would also produce.</p>
     */
    private void collectUniqueConstraints(DatabaseMetaData metaData, String foundCatalog, String foundSchema,
                                          String actualTable, TableDefinition table) throws SQLException {
        // Collect the primary-key columns first so we can skip the
        // primary-key index if the driver surfaces it here.
        java.util.Set<String> pkColumnsLower = new java.util.HashSet<>();
        try (ResultSet pkRs = metaData.getPrimaryKeys(foundCatalog, foundSchema, actualTable)) {
            while (pkRs.next()) {
                String col = pkRs.getString("COLUMN_NAME");
                if (col != null) pkColumnsLower.add(col.toLowerCase());
            }
        } catch (SQLException ignored) {
            // If PK metadata is unavailable, we still proceed; the
            // PK-index filter is best-effort.
        }

        try (ResultSet idxRs = metaData.getIndexInfo(foundCatalog, foundSchema, actualTable, true, false)) {
            java.util.Map<String, java.util.List<String>> indexes = new java.util.LinkedHashMap<>();
            while (idxRs.next()) {
                String idxName = idxRs.getString("INDEX_NAME");
                if (idxName == null) continue; // table-level statistics row
                String colName = idxRs.getString("COLUMN_NAME");
                if (colName == null) continue;
                indexes.computeIfAbsent(idxName, k -> new java.util.ArrayList<>()).add(colName);
            }
            for (var e : indexes.entrySet()) {
                // Skip indexes whose only column is the primary key.
                // H2 reports the primary-key index even with uniqueOnly=true.
                if (isPrimaryKeyIndex(e.getKey(), e.getValue(), pkColumnsLower)) continue;
                table.addUniqueConstraint(new UniqueConstraintDef(e.getKey(), e.getValue()));
            }
        } catch (SQLException ignored) {
            // Some drivers do not support getIndexInfo reliably (older
            // H2 modes, certain MySQL forks). The table can still be
            // introspected for columns; unique constraints just won't be
            // detected and the diff will treat them as new.
        }
    }

    /**
     * Best-effort heuristic for skipping primary-key indexes that some
     * drivers (notably H2) surface through {@code getIndexInfo}. Matches
     * H2's {@code PRIMARY_KEY_<N>} naming convention and single-column
     * indexes over a primary-key column.
     */
    private static boolean isPrimaryKeyIndex(String idxName, java.util.List<String> columns,
                                            java.util.Set<String> pkColumnsLower) {
        String upper = idxName.toUpperCase(java.util.Locale.ROOT);
        if (upper.startsWith("PRIMARY_KEY_") || upper.equals("PRIMARY_KEY")) {
            return true;
        }
        if (columns.size() == 1 && pkColumnsLower.contains(columns.get(0).toLowerCase(java.util.Locale.ROOT))) {
            // Single-column index on a PK column is the implicit
            // primary-key index, even if the driver named it differently
            // (some drivers omit the "PRIMARY_KEY_" prefix).
            return true;
        }
        return false;
    }

    /**
     * Reads unique indexes for a SQLite table via
     * {@code PRAGMA index_list} + {@code PRAGMA index_info}, since
     * {@code DatabaseMetaData.getIndexInfo} is not always reliable for
     * SQLite. The auto-generated primary-key index
     * ({@code sqlite_autoindex_<table>_1}) is excluded.
     */
    private void collectSqliteUniqueConstraints(String tableName, TableDefinition table) throws SQLException {
        String listPragma = "PRAGMA index_list(\"" + tableName.replace("\"", "\"\"") + "\")";
        try (PreparedStatement ps = connection.prepareStatement(listPragma);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String idxName = rs.getString("name");
                Integer isUnique = (Integer) rs.getObject("unique");
                if (idxName == null || isUnique == null || isUnique == 0) continue;
                if (idxName.startsWith("sqlite_autoindex_")) continue;

                String infoPragma = "PRAGMA index_info(\"" + idxName.replace("\"", "\"\"") + "\")";
                java.util.List<String> cols = new java.util.ArrayList<>();
                try (PreparedStatement ips = connection.prepareStatement(infoPragma);
                     ResultSet irs = ips.executeQuery()) {
                    while (irs.next()) {
                        String col = irs.getString("name");
                        if (col != null) cols.add(col);
                    }
                }
                if (!cols.isEmpty()) {
                    table.addUniqueConstraint(new UniqueConstraintDef(idxName, cols));
                }
            }
        }
    }

    // -----------------------
    // Column/Entity helpers
    // -----------------------

    /**
     * Build a ColumnDefinition from a ResultSet row returned by DatabaseMetaData.getColumns(...)
     */
    private ColumnDefinition buildColumnFromResultSet(ResultSet rs, Set<String> primaryKeys) throws SQLException {
        String columnName = rs.getString("COLUMN_NAME");
        String typeName = rs.getString("TYPE_NAME");
        int columnSize = -1;
        try {
            columnSize = rs.getInt("COLUMN_SIZE");
            if (rs.wasNull()) columnSize = -1;
        } catch (SQLException ignored) {
            columnSize = -1;
        }
        boolean nullable = true;
        try {
            int nullableVal = rs.getInt("NULLABLE");
            nullable = (nullableVal == DatabaseMetaData.columnNullable);
        } catch (SQLException ignored) {
            // leave nullable = true
        }
        String defaultValue = null;
        try {
            defaultValue = rs.getString("COLUMN_DEF");
        } catch (SQLException ignored) {}

        boolean isPrimaryKey = columnName != null && primaryKeys.contains(columnName.toLowerCase());
        if (isPrimaryKey) {
            // Drivers are inconsistent about reporting primary-key columns as
            // non-null (SQLite reports nullable for INTEGER PRIMARY KEY);
            // primary keys are never nullable in the schema Spool generates.
            nullable = false;
        }
        boolean isAutoIncrement = false;
        try {
            String ai = rs.getString("IS_AUTOINCREMENT");
            isAutoIncrement = "YES".equalsIgnoreCase(ai);
        } catch (SQLException ignored) {
            // ignore
        }

        ColumnDefinition.Builder builder = new ColumnDefinition.Builder()
                .name(columnName)
                .sqlType(typeName == null ? inferSqlTypeFromSize(columnSize) : typeName)
                .nullable(nullable)
                .primaryKey(isPrimaryKey)
                .autoIncrement(isAutoIncrement)
                .defaultValue(defaultValue);

        if (columnSize > 0) builder.length(columnSize);

        return builder.build();
    }

    /**
     * Build a TableDefinition from entity metadata (what the schema should be).
     */
    public TableDefinition buildTableDefinitionFromEntity(Class<?> entityClass) {
        EntityMetadata metadata = EntityMetadata.of(entityClass);
        TableDefinition table = new TableDefinition(metadata.getTableName());

        for (Field field : metadata.getFields()) {
            String columnName = metadata.getColumnName(field);
            boolean isPrimaryKey = metadata.isIdField(field);
            boolean isAutoIncrement = isPrimaryKey && metadata.isAutoIncrement();

            // Determine SQL type from Java type
            String sqlType = TypeMapper.javaTypeToSqlType(field.getType());     
            Integer length = null;
            boolean nullable = true;
            String referencedTable = null;
            String referencedColumn = null;
            Class<?> lengthSourceType = field.getType();

            // Check for @Column annotation
            if (field.isAnnotationPresent(Column.class)) {
                Column columnAnnotation = field.getAnnotation(Column.class);
                nullable = columnAnnotation.nullable();

                // Use explicit type if specified, otherwise infer from Java type
                String explicitType = columnAnnotation.type().trim();
                if (!explicitType.isEmpty()) {
                    sqlType = explicitType;
                }

                if (field.getType() == String.class || field.getType() == UUID.class) {
                    length = columnAnnotation.length();
                }
            }

            // Primary key never nullable
            if (field.isAnnotationPresent(Id.class)) {
                nullable = false;
            }

            // Handle owning single-reference associations - foreign key columns
            if (EntityMetadata.isOwningToOne(field)) {
                boolean optional = field.isAnnotationPresent(ManyToOne.class)
                        ? field.getAnnotation(ManyToOne.class).optional()
                        : field.getAnnotation(de.t14d3.spool.annotations.OneToOne.class).optional();
                nullable = optional;

                // Determine referenced table and column from field type
                Class<?> targetEntityClass = field.getType();
                if (targetEntityClass != null) {
                    EntityMetadata targetMetadata = EntityMetadata.of(targetEntityClass);
                    if (targetMetadata.isCompositeId()) {
                        throw new UnsupportedOperationException(
                                "@ManyToOne target with a composite id is not supported: " + targetEntityClass.getName());
                    }
                    referencedTable = targetMetadata.getTableName();

                    JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                    if (joinColumn != null && joinColumn.referencedColumnName() != null && !joinColumn.referencedColumnName().isBlank()) {
                        referencedColumn = joinColumn.referencedColumnName();
                        // Best-effort: resolve referenced column Java type to set FK type consistently.
                        Field referencedField = targetMetadata.getField(referencedColumn);
                        if (referencedField != null) {
                            lengthSourceType = referencedField.getType();
                            if ((lengthSourceType == String.class || lengthSourceType == UUID.class)
                                    && referencedField.isAnnotationPresent(Column.class)) {
                                length = referencedField.getAnnotation(Column.class).length();
                            }
                            sqlType = TypeMapper.javaTypeToSqlType(referencedField.getType());
                        } else {
                            // Fallback to id type if we can't resolve the column name.
                            lengthSourceType = targetMetadata.getIdField().getType();
                            if ((lengthSourceType == String.class || lengthSourceType == UUID.class)
                                    && targetMetadata.getIdField().isAnnotationPresent(Column.class)) {
                                length = targetMetadata.getIdField().getAnnotation(Column.class).length();
                            }
                            sqlType = TypeMapper.javaTypeToSqlType(targetMetadata.getIdField().getType());
                        }
                    } else {
                        referencedColumn = targetMetadata.getIdColumnName();
                        lengthSourceType = targetMetadata.getIdField().getType();
                        if ((lengthSourceType == String.class || lengthSourceType == UUID.class)
                                && targetMetadata.getIdField().isAnnotationPresent(Column.class)) {
                            length = targetMetadata.getIdField().getAnnotation(Column.class).length();
                        }
                        sqlType = TypeMapper.javaTypeToSqlType(targetMetadata.getIdField().getType());
                    }
                } else {
                    // Fallback to field name convention
                    referencedTable = field.getType().getSimpleName().toLowerCase();
                    referencedColumn = "id";
                    sqlType = "BIGINT";
                }

            }

            if (dialect == Dialect.MYSQL && sqlType != null && sqlType.equalsIgnoreCase("VARCHAR")) {
                if (lengthSourceType == UUID.class) {
                    // @Column's default length is 255, but UUID-as-string is 36 chars.
                    if (length == null || length == 255) {
                        length = 36;
                    }
                } else if (length == null) {
                    length = defaultVarcharLengthFor(lengthSourceType);
                }
            }

            // A type written with an explicit length ("VARCHAR(36)", "CHAR(8)") carries
            // its own length, and that string is what the DDL generator emits verbatim.
            // @Column length() still holds its 255 default in that case, which made every
            // comparison against the driver-reported length fail: each startup generated
            // a MODIFY COLUMN for every such column, so validateSchema() reported an
            // invalid schema forever. The explicit type is authoritative; a compound
            // precision ("DECIMAL(38, 18)") is not a length and is left alone.
            if (sqlType != null) {
                java.util.regex.Matcher embedded = EMBEDDED_TYPE_LENGTH.matcher(sqlType);
                if (embedded.find()) {
                    try {
                        length = Integer.parseInt(embedded.group(1));
                    } catch (NumberFormatException ignored) {
                        // Keep the declared length.
                    }
                }

                // Only VARCHAR/CHAR compare by length across dialects. For everything
                // else the driver reports a capacity that has nothing to do with the
                // entity (@Column's length() default of 255 vs MariaDB's TEXT = 65535),
                // so keeping it produced a MODIFY COLUMN for every TEXT/BLOB column on
                // every startup and validateSchema() never returned true.
                String upperType = sqlType.toUpperCase(java.util.Locale.ROOT);
                if (!(upperType.startsWith("VARCHAR") || upperType.startsWith("CHAR"))) {
                    length = null;
                }
            }

            ColumnDefinition.Builder builder = new ColumnDefinition.Builder()
                    .name(columnName)
                    .sqlType(sqlType)
                    .nullable(nullable)
                    .primaryKey(isPrimaryKey)
                    .autoIncrement(isAutoIncrement)
                    .length(length);
            
            if (referencedTable != null && referencedColumn != null) {
                builder.referencedTable(referencedTable)
                       .referencedColumn(referencedColumn);
            }
            
            ColumnDefinition column = builder.build();
            table.addColumn(column);
        }

        // Carry the unique constraints declared on the entity (single-column
        // @Column(unique=true) entries and composite @Table(uniqueConstraints)
        // entries) into the TableDefinition. The SQL generator emits them
        // inside CREATE TABLE on MySQL/MariaDB/PostgreSQL/H2; SQLite gets
        // separate CREATE UNIQUE INDEX statements after the table.
        for (de.t14d3.spool.mapping.UniqueConstraintDef uc : metadata.getUniqueConstraints()) {
            table.addUniqueConstraint(uc);
        }

        return table;
    }

    /**
     * Build additional TableDefinitions for ManyToMany join tables.
     *
     * Convention:
     * - join table name: {@code <ownerTable>_<ownerFieldName>}
     * - join columns: {@code <ownerTable>_<ownerIdColumn>} and {@code <inverseTable>_<inverseIdColumn>}
     * - composite primary key across both join columns
     *
     * Only the owning side (mappedBy empty) creates a join table.
     */
    public List<TableDefinition> buildJoinTableDefinitionsFromEntity(Class<?> entityClass) {
        EntityMetadata ownerMetadata = EntityMetadata.of(entityClass);
        List<TableDefinition> joinTables = new ArrayList<>();

        for (Field field : ownerMetadata.getHierarchyFields()) {
            if (!field.isAnnotationPresent(ManyToMany.class)) {
                continue;
            }
            ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
            if (manyToMany.mappedBy() != null && !manyToMany.mappedBy().isBlank()) {
                // Inverse side
                continue;
            }

            Class<?> targetEntity = EntityMetadata.collectionElementType(field);
            if (targetEntity == Object.class) {
                continue;
            }

            EntityMetadata inverseMetadata = EntityMetadata.of(targetEntity);
            if (ownerMetadata.isCompositeId() || inverseMetadata.isCompositeId()) {
                throw new UnsupportedOperationException(
                        "ManyToMany with composite-id entities is not supported (join tables use single-column keys): "
                                + ownerMetadata.getEntityClass().getName() + " <-> " + targetEntity.getName());
            }

            JoinTable joinTable = field.getAnnotation(JoinTable.class);
            String joinTableName = joinTable != null && !joinTable.name().isBlank()
                    ? joinTable.name()
                    : ownerMetadata.getTableName() + "_" + field.getName();
            String ownerJoinColumn = joinTable != null && !joinTable.joinColumn().isBlank()
                    ? joinTable.joinColumn()
                    : ownerMetadata.getTableName() + "_" + ownerMetadata.getIdColumnName();
            String inverseJoinColumn = joinTable != null && !joinTable.inverseJoinColumn().isBlank()
                    ? joinTable.inverseJoinColumn()
                    : inverseMetadata.getTableName() + "_" + inverseMetadata.getIdColumnName();

            String ownerIdSqlType = TypeMapper.javaTypeToSqlType(ownerMetadata.getIdField().getType());
            String inverseIdSqlType = TypeMapper.javaTypeToSqlType(inverseMetadata.getIdField().getType());

            Integer ownerIdLength = null;
            if (dialect == Dialect.MYSQL && ownerIdSqlType != null && ownerIdSqlType.equalsIgnoreCase("VARCHAR")) {
                ownerIdLength = ownerMetadata.getIdField().isAnnotationPresent(Column.class)
                        ? ownerMetadata.getIdField().getAnnotation(Column.class).length()
                        : null;
                if (ownerMetadata.getIdField().getType() == UUID.class) {
                    if (ownerIdLength == null || ownerIdLength == 255) {
                        ownerIdLength = 36;
                    }
                } else if (ownerIdLength == null) {
                    ownerIdLength = defaultVarcharLengthFor(ownerMetadata.getIdField().getType());
                }
            }
            Integer inverseIdLength = null;
            if (dialect == Dialect.MYSQL && inverseIdSqlType != null && inverseIdSqlType.equalsIgnoreCase("VARCHAR")) {
                inverseIdLength = inverseMetadata.getIdField().isAnnotationPresent(Column.class)
                        ? inverseMetadata.getIdField().getAnnotation(Column.class).length()
                        : null;
                if (inverseMetadata.getIdField().getType() == UUID.class) {
                    if (inverseIdLength == null || inverseIdLength == 255) {
                        inverseIdLength = 36;
                    }
                } else if (inverseIdLength == null) {
                    inverseIdLength = defaultVarcharLengthFor(inverseMetadata.getIdField().getType());
                }
            }

            TableDefinition join = new TableDefinition(joinTableName);
            join.addColumn(new ColumnDefinition.Builder()
                    .name(ownerJoinColumn)
                    .sqlType(ownerIdSqlType)
                    .length(ownerIdLength)
                    .nullable(false)
                    .primaryKey(true)
                    .referencedTable(ownerMetadata.getTableName())
                    .referencedColumn(ownerMetadata.getIdColumnName())
                    .build());
            join.addColumn(new ColumnDefinition.Builder()
                    .name(inverseJoinColumn)
                    .sqlType(inverseIdSqlType)
                    .length(inverseIdLength)
                    .nullable(false)
                    .primaryKey(true)
                    .referencedTable(inverseMetadata.getTableName())
                    .referencedColumn(inverseMetadata.getIdColumnName())
                    .build());

            joinTables.add(join);
        }

        return joinTables;
    }

    /**
     * Builds the complete expected schema for a set of registered entities:
     * entity tables, ManyToMany join tables, and the FK columns that
     * unidirectional {@code @OneToMany} associations place on their target
     * table.
     */
    public Map<String, TableDefinition> buildExpectedSchema(Collection<Class<?>> entityClasses) {
        Map<String, TableDefinition> expected = new LinkedHashMap<>();
        for (Class<?> entityClass : orderByForeignKeyDependencies(entityClasses)) {
            TableDefinition table = buildTableDefinitionFromEntity(entityClass);
            expected.put(table.getName().toLowerCase(), table);
        }
        for (Class<?> entityClass : entityClasses) {
            for (TableDefinition join : buildJoinTableDefinitionsFromEntity(entityClass)) {
                expected.putIfAbsent(join.getName().toLowerCase(), join);
            }
        }
        for (Class<?> entityClass : entityClasses) {
            addUnidirectionalOneToManyForeignKeys(entityClass, expected);
        }
        return expected;
    }

    /**
     * Orders entity classes so a table referenced by an inline foreign key is
     * created first. Cycles (mutual or self references) keep their original
     * relative order; databases that require deferred constraints for those
     * are a known limitation.
     */
    private List<Class<?>> orderByForeignKeyDependencies(Collection<Class<?>> entityClasses) {
        Map<Class<?>, Set<Class<?>>> dependencies = new LinkedHashMap<>();
        for (Class<?> entityClass : entityClasses) {
            Set<Class<?>> deps = new LinkedHashSet<>();
            for (Field field : EntityMetadata.of(entityClass).getFields()) {
                if (EntityMetadata.isOwningToOne(field)) {
                    deps.add(field.getType());
                }
            }
            dependencies.put(entityClass, deps);
        }
        // A unidirectional @OneToMany puts its FK on the target table, so the
        // target depends on the declaring entity.
        for (Class<?> entityClass : entityClasses) {
            for (Field field : EntityMetadata.of(entityClass).getHierarchyFields()) {
                de.t14d3.spool.annotations.OneToMany oneToMany =
                        field.getAnnotation(de.t14d3.spool.annotations.OneToMany.class);
                if (oneToMany == null || (oneToMany.mappedBy() != null && !oneToMany.mappedBy().isBlank())) {
                    continue;
                }
                if (field.getAnnotation(JoinColumn.class) == null) {
                    continue;
                }
                Set<Class<?>> targetDeps = dependencies.get(oneToMany.targetEntity());
                if (targetDeps != null) {
                    targetDeps.add(entityClass);
                }
            }
        }

        List<Class<?>> ordered = new ArrayList<>();
        Set<Class<?>> remaining = new LinkedHashSet<>(entityClasses);
        boolean progressed = true;
        while (!remaining.isEmpty() && progressed) {
            progressed = false;
            for (Iterator<Class<?>> it = remaining.iterator(); it.hasNext(); ) {
                Class<?> candidate = it.next();
                Set<Class<?>> pending = new LinkedHashSet<>(dependencies.getOrDefault(candidate, Set.of()));
                pending.retainAll(remaining);
                pending.remove(candidate); // self-references are handled at the DB level
                if (pending.isEmpty()) {
                    ordered.add(candidate);
                    it.remove();
                    progressed = true;
                }
            }
        }
        ordered.addAll(remaining); // cycles: keep declaration order
        return ordered;
    }

    /**
     * Adds the FK column owned by a unidirectional {@code @OneToMany} to the
     * table definition of its target entity.
     */
    private void addUnidirectionalOneToManyForeignKeys(Class<?> ownerClass, Map<String, TableDefinition> expected) {
        EntityMetadata ownerMetadata = EntityMetadata.of(ownerClass);
        for (Field field : ownerMetadata.getHierarchyFields()) {
            de.t14d3.spool.annotations.OneToMany oneToMany =
                    field.getAnnotation(de.t14d3.spool.annotations.OneToMany.class);
            if (oneToMany == null || (oneToMany.mappedBy() != null && !oneToMany.mappedBy().isBlank())) {
                continue;
            }
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
            if (joinColumn == null || joinColumn.name().isBlank()) {
                continue; // rejected at metadata build
            }
            if (ownerMetadata.isCompositeId()) {
                throw new UnsupportedOperationException(
                        "Unidirectional @OneToMany with a composite-id owner is not supported: "
                                + ownerClass.getName() + "." + field.getName());
            }

            EntityMetadata targetMetadata = EntityMetadata.of(oneToMany.targetEntity());
            TableDefinition targetTable = expected.get(targetMetadata.getTableName().toLowerCase());
            if (targetTable == null) {
                throw new IllegalArgumentException("Unidirectional @OneToMany " + ownerClass.getName() + "."
                        + field.getName() + " targets " + targetMetadata.getEntityClass().getName()
                        + ", which is not registered; register the target entity so its FK column can be created");
            }
            if (targetTable.hasColumn(joinColumn.name())) {
                throw new IllegalArgumentException("Unidirectional @OneToMany " + ownerClass.getName() + "."
                        + field.getName() + " needs FK column '" + joinColumn.name() + "' on table '"
                        + targetTable.getName() + "', but that column is already mapped");
            }
            targetTable.addColumn(buildForeignKeyColumn(joinColumn.name(), ownerMetadata, true));
        }
    }

    private ColumnDefinition buildForeignKeyColumn(String columnName, EntityMetadata ownerMetadata, boolean nullable) {
        Class<?> idType = ownerMetadata.getIdField().getType();
        String sqlType = TypeMapper.javaTypeToSqlType(idType);
        Integer length = null;
        if ((idType == String.class || idType == UUID.class)
                && ownerMetadata.getIdField().isAnnotationPresent(Column.class)) {
            length = ownerMetadata.getIdField().getAnnotation(Column.class).length();
        }
        if (dialect == Dialect.MYSQL && sqlType != null && sqlType.equalsIgnoreCase("VARCHAR")) {
            if (idType == UUID.class && (length == null || length == 255)) {
                length = 36;
            } else if (length == null) {
                length = defaultVarcharLengthFor(idType);
            }
        }
        return new ColumnDefinition.Builder()
                .name(columnName)
                .sqlType(sqlType)
                .nullable(nullable)
                .primaryKey(false)
                .autoIncrement(false)
                .length(length)
                .referencedTable(ownerMetadata.getTableName())
                .referencedColumn(ownerMetadata.getIdColumnName())
                .build();
    }

    // -----------------------
    // Type mapping helpers
    // -----------------------

    private Integer defaultVarcharLengthFor(Class<?> javaType) {
        if (javaType == UUID.class) return 36;
        // MySQL requires a length for VARCHAR; 255 is a sensible default for free-form strings.
        if (javaType == String.class) return 255;
        return 255;
    }


    private int parseLengthFromType(String type) {
        if (type == null) return -1;
        int start = type.indexOf('(');
        int end = type.indexOf(')');
        if (start >= 0 && end > start) {
            String inside = type.substring(start + 1, end).trim();
            try {
                return Integer.parseInt(inside.split(",")[0].trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return -1;
    }

    private String inferSqlTypeFromSize(int columnSize) {
        if (columnSize <= 0) return "VARCHAR";
        if (columnSize >= Integer.MAX_VALUE / 2) return "TEXT";
        return "VARCHAR";
    }
}
