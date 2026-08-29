package de.t14d3.spool;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helpers for translating low-level JDBC {@link SQLException}s into
 * Spool-typed exceptions.
 *
 * <p>{@link SQLException} exposes an {@code SQLState} string and a
 * vendor-specific {@code errorCode}, both of which vary between
 * databases. The JDBC spec only guarantees a coarse hierarchy
 * ({@link java.sql.SQLIntegrityConstraintViolationException} covers
 * unique, primary key, foreign key, check, and not null), which is not
 * precise enough to drive application code (e.g. "re-find and update on
 * duplicate key, ignore on foreign-key violation").</p>
 *
 * <p>Callers should funnel database write failures through
 * {@link #throwUniqueOrRethrow(SQLException, String)}, which surfaces
 * a unique-key violation as a {@link UniqueConstraintViolationException}
 * (a {@link RuntimeException}) with the resolved constraint name
 * attached. Other integrity violations keep their stock
 * {@link java.sql.SQLIntegrityConstraintViolationException} type;
 * the vendor-specific inspection is centralized here so individual
 * call sites do not have to carry around error-code tables.</p>
 */
public final class SpoolExceptions {

    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    private static final int MYSQL_DUPLICATE_ENTRY = 1062;

    private static final int SQLITE_CONSTRAINT_UNIQUE = 2067;

    private static final int SQLITE_CONSTRAINT_PRIMARYKEY_OR_UNIQUE = 19;

    private static final Pattern MYSQL_DUPLICATE_KEY_PATTERN =
            Pattern.compile("for key '([^']+)'");

    private static final Pattern QUOTE_CONSTRAINT_NAME_PATTERN =
            Pattern.compile("\"([^\"]+)\"");

    private static final Pattern SQLITE_CONSTRAINT_FAILED_PATTERN =
            Pattern.compile("UNIQUE constraint failed:\\s*([^\\s.]+)\\.([^\\s.]+)");

    private SpoolExceptions() {
    }

    /**
     * Always throws. When {@code sqlException} represents a unique-key
     * violation under any supported dialect, throws a
     * {@link UniqueConstraintViolationException} (a
     * {@link RuntimeException}) whose cause is the original. Otherwise
     * re-throws {@code sqlException} wrapped in a plain
     * {@link RuntimeException}. Designed for the top of a catch block
     * where the throw site itself sits behind a {@code throws
     * RuntimeException}-only clause (for example,
     * {@link de.t14d3.spool.core.EntityManager#flush()}).
     */
    public static void throwUniqueOrRethrow(SQLException sqlException, String entityName) {
        if (sqlException == null) {
            throw new IllegalArgumentException("sqlException must not be null");
        }
        if (isUniqueViolation(sqlException)) {
            throw uniqueViolation(sqlException, entityName);
        }
        throw new RuntimeException(sqlException);
    }

    /**
     * Predicate: does {@code e} represent a unique-key violation under
     * any supported dialect? Walks the chained exception list since
     * some drivers wrap the meaningful cause in {@code getNextException}.
     */
    public static boolean isUniqueViolation(SQLException e) {
        if (e == null) return false;
        SQLException current = e;
        int hops = 0;
        while (current != null && hops++ < 16) {
            if (isUniqueViolationSingle(current)) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

    private static boolean isUniqueViolationSingle(SQLException e) {
        // PostgreSQL, H2: SQLState 23505 is a precise unique violation.
        if (SQLSTATE_UNIQUE_VIOLATION.equals(e.getSQLState())) {
            return true;
        }
        // MySQL, MariaDB: error code 1062 is "Duplicate entry ... for key ...".
        if (e.getErrorCode() == MYSQL_DUPLICATE_ENTRY) {
            return true;
        }
        // SQLite: SQLState 23000 (integrity violation) plus extended
        // error code 19 (CONSTRAINT_PRIMARYKEY / older unique) or 2067
        // (SQLITE_CONSTRAINT_UNIQUE).
        if (e.getErrorCode() == SQLITE_CONSTRAINT_UNIQUE
                || e.getErrorCode() == SQLITE_CONSTRAINT_PRIMARYKEY_OR_UNIQUE) {
            return true;
        }
        return false;
    }

    private static UniqueConstraintViolationException uniqueViolation(
            SQLException sqlException, String entityName) {
        return new UniqueConstraintViolationException(
                sqlException.getMessage(),
                sqlException.getSQLState(),
                sqlException.getErrorCode(),
                entityName,
                extractConstraintName(sqlException),
                sqlException
        );
    }

    private static String extractConstraintName(SQLException e) {
        String message = e.getMessage();
        if (message == null || message.isEmpty()) {
            return null;
        }
        // MySQL/MariaDB: "for key 'table.constraint_name'" - strip the
        // table prefix and keep just the constraint name.
        Matcher mysqlMatcher = MYSQL_DUPLICATE_KEY_PATTERN.matcher(message);
        if (mysqlMatcher.find()) {
            String keyName = mysqlMatcher.group(1);
            int dot = keyName.indexOf('.');
            return dot >= 0 ? keyName.substring(dot + 1) : keyName;
        }
        // SQLite: "UNIQUE constraint failed: table.index_name".
        Matcher sqliteMatcher = SQLITE_CONSTRAINT_FAILED_PATTERN.matcher(message);
        if (sqliteMatcher.find()) {
            return sqliteMatcher.group(2);
        }
        // PostgreSQL/H2: 'violates unique constraint "name"' or
        // "duplicate key value violates unique constraint \"name\"".
        Matcher quoted = QUOTE_CONSTRAINT_NAME_PATTERN.matcher(message);
        if (quoted.find()) {
            return quoted.group(1);
        }
        return null;
    }
}
