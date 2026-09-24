package de.t14d3.spool.query.hql;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.query.Query;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A parsed HQL-lite query bound to a specific entity type.
 * <p>
 * Parameter markers are resolved when the query executes, which allows a
 * single query object to be reused with different parameter values. The query
 * object itself is untyped; the result methods are generic so that the common
 * fluent chain infers the entity type at the assignment site:
 * {@code List<User> users = em.createQuery("SELECT u FROM User u").getResultList();}
 */
public final class HqlQuery {

    /**
     * Marker delimiters embedded in the generated SQL. They are replaced with
     * JDBC placeholders immediately before execution, so they never reach the
     * database driver. Control characters cannot occur in the generated SQL
     * otherwise.
     */
    static final char MARKER_START = '\u0001';
    static final char MARKER_END = '\u0002';

    record ParamRef(boolean list, String name, Integer position) {
    }

    /**
     * A {@code JOIN FETCH} target: the association is batch-loaded after the
     * main query runs, so a path costs one extra bounded query (chunked at
     * 500 ids), never one query per row.
     */
    public record FetchPath(Class<?> entityClass, String fieldName) {
    }

    private final EntityManager entityManager;
    private final Class<?> resultClass;
    private final Query.SelectBuilder builder;
    private final List<ParamRef> paramRefs;
    private final List<FetchPath> fetchPaths;
    private final boolean countQuery;
    private final boolean projection;
    private int fetchDepth = -1;
    private final Map<String, Object> namedParameters = new HashMap<>();
    private final Map<Integer, Object> positionalParameters = new HashMap<>();

    HqlQuery(EntityManager entityManager, Class<?> resultClass, Query.SelectBuilder builder,
             List<ParamRef> paramRefs, boolean countQuery, List<FetchPath> fetchPaths, boolean projection) {
        this.entityManager = entityManager;
        this.resultClass = resultClass;
        this.builder = builder;
        this.paramRefs = List.copyOf(paramRefs);
        this.countQuery = countQuery;
        this.fetchPaths = List.copyOf(fetchPaths);
        this.projection = projection;
    }

    public static HqlQuery parse(EntityManager entityManager, String hql) {
        HqlParser.Parsed parsed = new HqlParser(entityManager.getDialect(), hql).parse();
        return new HqlQuery(entityManager, parsed.entityClass(), parsed.builder(), parsed.params(),
                parsed.countQuery(), parsed.fetchPaths(), parsed.projection());
    }

    /**
     * The entity class the parsed FROM clause resolves to.
     */
    public Class<?> getResultClass() {
        return resultClass;
    }

    /**
     * True for {@code SELECT COUNT(...)} statements. Their result list
     * contains a single {@link Long}.
     */
    public boolean isCountQuery() {
        return countQuery;
    }

    static String marker(int index) {
        return MARKER_START + Integer.toString(index) + MARKER_END;
    }

    /**
     * Bind a named parameter ({@code :name}).
     */
    public HqlQuery setParameter(String name, Object value) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Parameter name must not be blank");
        }
        namedParameters.put(name, value);
        return this;
    }

    /**
     * Bind a positional parameter ({@code ?1}, {@code ?2}, ...). Positions are
     * one-based, matching the HQL position.
     */
    public HqlQuery setParameter(int position, Object value) {
        if (position < 1) {
            throw new IllegalArgumentException("Positional parameter positions are one-based");
        }
        positionalParameters.put(position, value);
        return this;
    }

    /**
     * Limit the number of returned rows, overriding (or adding) a LIMIT clause.
     */
    public HqlQuery setMaxResults(int maxResults) {
        if (maxResults < 1) {
            throw new IllegalArgumentException("maxResults must be >= 1");
        }
        builder.limit(Integer.toString(maxResults));
        return this;
    }

    /**
     * Bound eager-fetch recursion for this query: 1 resolves only the direct
     * eager references of the result rows, 2 their references, and so on.
     * Defaults to unlimited. LAZY associations are unaffected; use
     * {@code JOIN FETCH} or {@code em.initialize} to resolve them.
     */
    public HqlQuery setFetchDepth(int fetchDepth) {
        if (fetchDepth < 1) {
            throw new IllegalArgumentException("fetchDepth must be >= 1");
        }
        builder.fetchDepth(fetchDepth);
        this.fetchDepth = fetchDepth;
        return this;
    }

    /**
     * Skip the first {@code firstResult} rows (0-based). Can be combined with
     * {@link #setMaxResults(int)} for pagination.
     */
    public HqlQuery setFirstResult(int firstResult) {
        if (firstResult < 0) {
            throw new IllegalArgumentException("firstResult must be >= 0");
        }
        if (firstResult > 0) {
            builder.offset(Integer.toString(firstResult));
        }
        return this;
    }

    /**
     * Execute the query and return the result list.
     * <p>
     * For {@code SELECT COUNT(...)} statements the list contains exactly one
     * {@link Long}.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getResultList() {
        Query query = builder.build();
        List<Object> parameters = new ArrayList<>();
        String sql = bind(query.getSql(), parameters);
        if (projection) {
            List<Object> rows = entityManager.executeProjectionQuery(sql, parameters);
            if (countQuery) {
                List<T> normalized = new ArrayList<>(rows.size());
                for (Object value : rows) {
                    normalized.add(value instanceof Number number ? (T) Long.valueOf(number.longValue()) : (T) value);
                }
                return normalized;
            }
            return (List<T>) rows;
        }
        List<T> results = (List<T>) entityManager.executeSelectQuery(sql, parameters, resultClass, fetchDepth);
        if (!fetchPaths.isEmpty()) {
            entityManager.applyFetchPaths((List<Object>) (List<?>) results, fetchPaths);
        }
        return results;
    }

    /**
     * Execute the query and return exactly one result.
     *
     * @throws IllegalStateException when the query yields no rows or more than one row
     */
    public <T> T getSingleResult() {
        List<T> results = getResultList();
        if (results.isEmpty()) {
            throw new IllegalStateException("Query returned no results");
        }
        if (results.size() > 1) {
            throw new IllegalStateException("Query returned more than one result: " + results.size());
        }
        return results.get(0);
    }

    private String bind(String sql, List<Object> outParameters) {
        StringBuilder bound = new StringBuilder(sql.length());
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c != MARKER_START) {
                bound.append(c);
                i++;
                continue;
            }
            int end = sql.indexOf(MARKER_END, i + 1);
            if (end < 0) {
                throw new HqlException("Corrupt parameter marker in generated SQL");
            }
            int refIndex;
            try {
                refIndex = Integer.parseInt(sql.substring(i + 1, end));
            } catch (NumberFormatException e) {
                throw new HqlException("Corrupt parameter marker in generated SQL");
            }
            if (refIndex < 0 || refIndex >= paramRefs.size()) {
                throw new HqlException("Unknown parameter marker #" + refIndex);
            }
            appendValue(bound, outParameters, paramRefs.get(refIndex));
            i = end + 1;
        }
        return bound.toString();
    }

    private void appendValue(StringBuilder bound, List<Object> outParameters, ParamRef ref) {
        Object value = resolve(ref);
        if (!ref.list()) {
            bound.append('?');
            outParameters.add(value);
            return;
        }

        List<Object> values = toList(value);
        if (values.isEmpty()) {
            // IN (NULL) never matches, which is the correct semantics for an
            // empty collection.
            bound.append("NULL");
            return;
        }
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                bound.append(", ");
            }
            bound.append('?');
            outParameters.add(values.get(i));
        }
    }

    private Object resolve(ParamRef ref) {
        if (ref.name() != null) {
            if (!namedParameters.containsKey(ref.name())) {
                throw new IllegalArgumentException("Parameter not set: :" + ref.name());
            }
            return namedParameters.get(ref.name());
        }
        if (!positionalParameters.containsKey(ref.position())) {
            throw new IllegalArgumentException("Parameter not set: ?" + ref.position());
        }
        return positionalParameters.get(ref.position());
    }

    private static List<Object> toList(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Collection parameter must not be null");
        }
        List<Object> values = new ArrayList<>();
        if (value instanceof Collection<?> collection) {
            values.addAll(collection);
            return values;
        }
        if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                values.add(item);
            }
            return values;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) {
                values.add(Array.get(value, i));
            }
            return values;
        }
        throw new IllegalArgumentException(
                "Collection parameter must be a Collection, Iterable or array, got " + value.getClass().getName());
    }
}
