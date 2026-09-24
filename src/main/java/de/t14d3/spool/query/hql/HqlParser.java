package de.t14d3.spool.query.hql;

import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.OneToOne;
import de.t14d3.spool.mapping.EntityMetadata;
import de.t14d3.spool.query.Dialect;
import de.t14d3.spool.query.Query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Recursive-descent parser for the HQL-lite dialect.
 * <p>
 * Supported grammar:
 * <pre>
 *   SELECT (&lt;alias&gt;|*) FROM &lt;Entity&gt; &lt;alias&gt;
 *     [JOIN FETCH &lt;alias&gt;.&lt;path&gt;]...
 *     [WHERE &lt;condition&gt;]
 *     [ORDER BY &lt;path&gt; [ASC|DESC], ...]
 *     [LIMIT &lt;n&gt;]
 *
 *   condition := or
 *   or        := and ('OR' and)*
 *   and       := not ('AND' not)*
 *   not       := 'NOT' not | primary
 *   primary   := '(' or ')' | path predicate
 *   predicate := cmp value | [NOT] LIKE value | [NOT] IN (value, ...) | IS [NOT] NULL
 * </pre>
 * Paths may traverse {@code @ManyToOne} and {@code @OneToOne} relationships by
 * generating an implicit inner join. Collection paths are rejected.
 */
final class HqlParser {

    record Parsed(Class<?> entityClass, Query.SelectBuilder builder, List<HqlQuery.ParamRef> params,
                  boolean countQuery, List<HqlQuery.FetchPath> fetchPaths) {
    }

    private final Dialect dialect;
    private final List<HqlLexer.Token> tokens;
    private final List<HqlQuery.ParamRef> params = new ArrayList<>();
    private final Map<String, String> joinAliases = new LinkedHashMap<>();

    private int index;
    private int joinCounter;
    private Query.SelectBuilder builder;
    private EntityMetadata baseMetadata;
    private String baseAlias;

    HqlParser(Dialect dialect, String hql) {
        this.dialect = dialect;
        this.tokens = new HqlLexer(hql).tokenize();
    }

    Parsed parse() {
        expect(HqlLexer.Kind.SELECT);
        String selectAlias = null;
        String countAlias = null;
        boolean countQuery = false;
        if (!match(HqlLexer.Kind.STAR)) {
            if (peekKind(0) == HqlLexer.Kind.IDENT
                    && current().text().equalsIgnoreCase("count")
                    && peekKind(1) == HqlLexer.Kind.LPAREN) {
                index++;
                expect(HqlLexer.Kind.LPAREN);
                if (!match(HqlLexer.Kind.STAR)) {
                    countAlias = expect(HqlLexer.Kind.IDENT).text();
                }
                expect(HqlLexer.Kind.RPAREN);
                countQuery = true;
            } else {
                selectAlias = expect(HqlLexer.Kind.IDENT).text();
                if (peekKind(0) == HqlLexer.Kind.DOT) {
                    throw new HqlException("Projections are not supported; select the entity alias instead");
                }
            }
        }
        expect(HqlLexer.Kind.FROM);

        String entityName = parseQualifiedName();
        Class<?> entityClass = EntityMetadata.resolveEntityClass(entityName);
        if (entityClass == null) {
            throw new HqlException("Unknown entity '" + entityName + "'");
        }
        baseMetadata = EntityMetadata.of(entityClass);
        baseAlias = expect(HqlLexer.Kind.IDENT).text();
        if (selectAlias != null && !selectAlias.equals(baseAlias)) {
            throw new HqlException(
                    "SELECT alias '" + selectAlias + "' does not match FROM alias '" + baseAlias + "'");
        }
        if (countAlias != null && !countAlias.equals(baseAlias)) {
            throw new HqlException(
                    "COUNT alias '" + countAlias + "' does not match FROM alias '" + baseAlias + "'");
        }

        builder = Query.select(dialect, countQuery ? "COUNT(*)" : "*")
                .from(baseMetadata.getTableName(), baseAlias);

        List<HqlQuery.FetchPath> fetchPaths = new ArrayList<>();
        while (peekKind(0) == HqlLexer.Kind.JOIN
                || peekKind(0) == HqlLexer.Kind.LEFT
                || peekKind(0) == HqlLexer.Kind.INNER) {
            if (peekKind(0) == HqlLexer.Kind.JOIN) {
                index++;
            } else {
                index++;
                expect(HqlLexer.Kind.JOIN);
            }
            expect(HqlLexer.Kind.FETCH);
            String fetchAlias = expect(HqlLexer.Kind.IDENT).text();
            if (!fetchAlias.equals(baseAlias)) {
                throw new HqlException(
                        "JOIN FETCH alias '" + fetchAlias + "' does not match FROM alias '" + baseAlias + "'");
            }
            expect(HqlLexer.Kind.DOT);
            String fieldName = expect(HqlLexer.Kind.IDENT).text();
            Field field = baseMetadata.getFieldByName(fieldName);
            if (field == null) {
                throw new HqlException("Unknown field for JOIN FETCH: " + baseAlias + "." + fieldName);
            }
            if (!field.isAnnotationPresent(ManyToOne.class)
                    && !field.isAnnotationPresent(OneToOne.class)
                    && !field.isAnnotationPresent(OneToMany.class)
                    && !field.isAnnotationPresent(ManyToMany.class)) {
                throw new HqlException("JOIN FETCH target is not a relationship: "
                        + baseAlias + "." + fieldName);
            }
            fetchPaths.add(new HqlQuery.FetchPath(entityClass, fieldName));
        }

        if (match(HqlLexer.Kind.WHERE)) {
            builder.where(parseOr());
        }
        if (countQuery) {
            if (peekKind(0) == HqlLexer.Kind.ORDER || peekKind(0) == HqlLexer.Kind.LIMIT) {
                throw new HqlException("ORDER BY and LIMIT are not supported on COUNT queries");
            }
        } else {
            if (match(HqlLexer.Kind.ORDER)) {
                expect(HqlLexer.Kind.BY);
                parseOrderBy();
            }
            if (match(HqlLexer.Kind.LIMIT)) {
                builder.limit(expect(HqlLexer.Kind.NUMBER).text());
            }
        }
        expect(HqlLexer.Kind.EOF);

        return new Parsed(entityClass, builder, params, countQuery, fetchPaths);
    }

    private String parseQualifiedName() {
        StringBuilder name = new StringBuilder(expect(HqlLexer.Kind.IDENT).text());
        while (match(HqlLexer.Kind.DOT)) {
            name.append('.').append(expect(HqlLexer.Kind.IDENT).text());
        }
        return name.toString();
    }

    // ------------------------------------------------------------------
    // WHERE clause
    // ------------------------------------------------------------------

    private String parseOr() {
        String left = parseAnd();
        while (match(HqlLexer.Kind.OR)) {
            left = "(" + left + " OR " + parseAnd() + ")";
        }
        return left;
    }

    private String parseAnd() {
        String left = parseNot();
        while (match(HqlLexer.Kind.AND)) {
            left = "(" + left + " AND " + parseNot() + ")";
        }
        return left;
    }

    private String parseNot() {
        if (match(HqlLexer.Kind.NOT)) {
            return "(NOT " + parseNot() + ")";
        }
        return parsePredicate();
    }

    private String parsePredicate() {
        if (match(HqlLexer.Kind.LPAREN)) {
            String inner = parseOr();
            expect(HqlLexer.Kind.RPAREN);
            return "(" + inner + ")";
        }

        String path = parsePath(true);

        if (match(HqlLexer.Kind.IS)) {
            boolean negated = match(HqlLexer.Kind.NOT);
            expect(HqlLexer.Kind.NULL);
            return path + (negated ? " IS NOT NULL" : " IS NULL");
        }

        boolean negated = false;
        if (peekKind(0) == HqlLexer.Kind.NOT
                && (peekKind(1) == HqlLexer.Kind.LIKE || peekKind(1) == HqlLexer.Kind.IN)) {
            index++;
            negated = true;
        }

        if (match(HqlLexer.Kind.LIKE)) {
            return path + (negated ? " NOT LIKE " : " LIKE ") + parseValue();
        }

        if (match(HqlLexer.Kind.IN)) {
            expect(HqlLexer.Kind.LPAREN);
            StringBuilder in = new StringBuilder();
            if (peekKind(0) == HqlLexer.Kind.NAMED_PARAM) {
                in.append(markerForParam(true, current().text().substring(1), null));
                index++;
            } else if (peekKind(0) == HqlLexer.Kind.POSITIONAL_PARAM) {
                in.append(markerForParam(true, null, positionalIndex(current().text())));
                index++;
            } else {
                in.append(parseValue());
                while (match(HqlLexer.Kind.COMMA)) {
                    in.append(", ").append(parseValue());
                }
            }
            expect(HqlLexer.Kind.RPAREN);
            return path + (negated ? " NOT IN (" : " IN (") + in + ")";
        }

        if (negated) {
            throw new HqlException("Expected LIKE or IN after NOT", current().position());
        }

        HqlLexer.Kind operator = expectAny(
                HqlLexer.Kind.EQ, HqlLexer.Kind.NE, HqlLexer.Kind.LT,
                HqlLexer.Kind.LE, HqlLexer.Kind.GT, HqlLexer.Kind.GE);
        return path + " " + sqlOperator(operator) + " " + parseValue();
    }

    private String parseValue() {
        HqlLexer.Token token = current();
        switch (token.kind()) {
            case NUMBER -> {
                index++;
                return token.text();
            }
            case STRING -> {
                index++;
                return "'" + dialect.escapeStringLiteral(token.text()) + "'";
            }
            case TRUE -> {
                index++;
                return "TRUE";
            }
            case FALSE -> {
                index++;
                return "FALSE";
            }
            case NULL -> {
                index++;
                return "NULL";
            }
            case NAMED_PARAM -> {
                index++;
                return markerForParam(false, token.text().substring(1), null);
            }
            case POSITIONAL_PARAM -> {
                index++;
                return markerForParam(false, null, positionalIndex(token.text()));
            }
            default -> throw new HqlException(
                    "Expected a value, found '" + token.text() + "'", token.position());
        }
    }

    private String parseOrderBy() {
        StringBuilder order = new StringBuilder();
        do {
            if (order.length() > 0) {
                order.append(", ");
            }
            order.append(parsePath(false));
            if (match(HqlLexer.Kind.ASC)) {
                order.append(" ASC");
            } else if (match(HqlLexer.Kind.DESC)) {
                order.append(" DESC");
            }
        } while (match(HqlLexer.Kind.COMMA));
        builder.orderBy(order.toString());
        return order.toString();
    }

    // ------------------------------------------------------------------
    // Path resolution and implicit joins
    // ------------------------------------------------------------------

    private String parsePath(boolean quoted) {
        HqlLexer.Token first = expect(HqlLexer.Kind.IDENT);
        if (!first.text().equals(baseAlias)) {
            throw new HqlException("Unknown entity alias '" + first.text() + "'", first.position());
        }

        List<HqlLexer.Token> segments = new ArrayList<>();
        while (match(HqlLexer.Kind.DOT)) {
            segments.add(expect(HqlLexer.Kind.IDENT));
        }
        if (segments.isEmpty()) {
            throw new HqlException(
                    "Expected a field path after alias '" + baseAlias + "'", first.position());
        }

        EntityMetadata metadata = baseMetadata;
        String alias = baseAlias;
        for (int i = 0; i < segments.size(); i++) {
            HqlLexer.Token segment = segments.get(i);
            Field field = metadata.getFieldByName(segment.text());
            if (field == null) {
                throw new HqlException(
                        "Unknown field '" + segment.text() + "' on "
                                + metadata.getEntityClass().getSimpleName(),
                        segment.position());
            }

            if (i == segments.size() - 1) {
                String column = metadata.getColumnName(field);
                if (column == null) {
                    throw new HqlException(
                            "Field '" + segment.text() + "' is a relationship; use a nested path such as "
                                    + segment.text() + ".id",
                            segment.position());
                }
                return quoted
                        ? quote(alias) + "." + quote(column)
                        : alias + "." + column;
            }

            if (!field.isAnnotationPresent(ManyToOne.class) && !field.isAnnotationPresent(OneToOne.class)) {
                throw new HqlException(
                        "Cannot traverse '" + segment.text() + "' on "
                                + metadata.getEntityClass().getSimpleName()
                                + ": only @ManyToOne and @OneToOne paths are supported",
                        segment.position());
            }
            alias = joinFor(alias, metadata, field, segment.text());
            metadata = EntityMetadata.of(field.getType());
        }
        throw new IllegalStateException("Unreachable: path resolution must return");
    }

    private String joinFor(String currentAlias, EntityMetadata currentMetadata, Field field, String fieldName) {
        String key = currentAlias + "." + fieldName;
        String existing = joinAliases.get(key);
        if (existing != null) {
            return existing;
        }

        EntityMetadata targetMetadata = EntityMetadata.of(field.getType());
        String joinAlias = "t" + (++joinCounter);

        String on;
        if (field.isAnnotationPresent(ManyToOne.class)) {
            on = quote(currentAlias) + "." + quote(currentMetadata.getColumnName(field))
                    + " = " + quote(joinAlias) + "." + quote(targetMetadata.getIdColumnName());
        } else {
            OneToOne oneToOne = field.getAnnotation(OneToOne.class);
            if (oneToOne.mappedBy() == null || oneToOne.mappedBy().isBlank()) {
                on = quote(currentAlias) + "." + quote(currentMetadata.getColumnName(field))
                        + " = " + quote(joinAlias) + "." + quote(targetMetadata.getIdColumnName());
            } else {
                Field inverse = targetMetadata.getFieldByName(oneToOne.mappedBy());
                if (inverse == null) {
                    throw new HqlException("mappedBy field not found: "
                            + field.getType().getSimpleName() + "." + oneToOne.mappedBy());
                }
                String foreignKey = targetMetadata.getColumnName(inverse);
                if (foreignKey == null) {
                    throw new HqlException("mappedBy field is not a column: "
                            + field.getType().getSimpleName() + "." + oneToOne.mappedBy());
                }
                on = quote(currentAlias) + "." + quote(currentMetadata.getIdColumnName())
                        + " = " + quote(joinAlias) + "." + quote(foreignKey);
            }
        }

        builder.join("JOIN " + quote(targetMetadata.getTableName()) + " " + quote(joinAlias) + " ON " + on);
        joinAliases.put(key, joinAlias);
        return joinAlias;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private String markerForParam(boolean list, String name, Integer position) {
        params.add(new HqlQuery.ParamRef(list, name, position));
        return HqlQuery.marker(params.size() - 1);
    }

    private static int positionalIndex(String tokenText) {
        return Integer.parseInt(tokenText.substring(1));
    }

    private String quote(String identifier) {
        return dialect.quoteIdentifier(identifier);
    }

    private static String sqlOperator(HqlLexer.Kind kind) {
        return switch (kind) {
            case EQ -> "=";
            case NE -> "<>";
            case LT -> "<";
            case LE -> "<=";
            case GT -> ">";
            case GE -> ">=";
            default -> throw new IllegalArgumentException("Not a comparison operator: " + kind);
        };
    }

    private HqlLexer.Token current() {
        return tokens.get(Math.min(index, tokens.size() - 1));
    }

    private HqlLexer.Kind peekKind(int offset) {
        return tokens.get(Math.min(index + offset, tokens.size() - 1)).kind();
    }

    private boolean match(HqlLexer.Kind kind) {
        if (peekKind(0) == kind) {
            index++;
            return true;
        }
        return false;
    }

    private HqlLexer.Token expect(HqlLexer.Kind kind) {
        HqlLexer.Token token = current();
        if (token.kind() != kind) {
            throw new HqlException(
                    "Expected " + kind + " but found '" + token.text() + "'", token.position());
        }
        index++;
        return token;
    }

    private HqlLexer.Kind expectAny(HqlLexer.Kind... kinds) {
        HqlLexer.Kind kind = peekKind(0);
        for (HqlLexer.Kind candidate : kinds) {
            if (kind == candidate) {
                index++;
                return kind;
            }
        }
        throw new HqlException(
                "Expected a comparison operator but found '" + current().text() + "'", current().position());
    }
}
