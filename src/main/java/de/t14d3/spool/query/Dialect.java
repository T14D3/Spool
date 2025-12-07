package de.t14d3.spool.query;

/**
 * SQL database dialects for proper identifier quoting and type handling.
 */
public enum Dialect {
    GENERIC,
    MYSQL,
    POSTGRESQL,
    SQLITE,
    H2;

    /**
     * Quote an identifier based on the dialect.
     */
    public String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.trim().isEmpty()) {
            return identifier;
        }

        return switch (this) {
            case MYSQL -> "`" + identifier.replace("`", "``") + "`";
            case POSTGRESQL, SQLITE -> "\"" + identifier.replace("\"", "\"\"") + "\"";
            case H2 -> ("\"" + identifier.replace("\"", "\"\"") + "\"").toUpperCase();
            default ->
                // No quoting for generic
                    identifier;
        };
    }

    /**
     * Escape a string literal based on the dialect.
     */
    public String escapeStringLiteral(String literal) {
        if (literal == null) {
            return null;
        }

        // Basic SQL string escaping - replace single quotes with doubled quotes
        return literal.replace("'", "''");
    }

    /**
     * Detect dialect from JDBC URL.
     */
    public static Dialect detectFromUrl(String jdbcUrl) {
        if (jdbcUrl == null) return GENERIC;

        String lowerUrl = jdbcUrl.toLowerCase();
        if (lowerUrl.contains("mysql")) return MYSQL;
        if (lowerUrl.contains("postgresql") || lowerUrl.contains("postgres")) return POSTGRESQL;
        if (lowerUrl.contains("sqlite")) return SQLITE;
        if (lowerUrl.contains("h2")) return H2;

        return GENERIC;
    }
}
