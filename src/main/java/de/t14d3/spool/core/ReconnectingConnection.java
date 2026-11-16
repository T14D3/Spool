package de.t14d3.spool.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.util.Objects;

/**
 * A {@link Connection} that heals itself when the underlying database session dies.
 *
 * <p>Spool holds one connection for the lifetime of an {@link EntityManager}. Without
 * this wrapper a database restart, a proxy/firewall dropping an idle session, a
 * {@code wait_timeout} expiry or any network blip kills that session for good: every
 * later statement fails with "connection is closed" until the whole process restarts.
 * In a game server that means inventories stop saving, homes/warps/tickets break and
 * the network outbox stops draining long after the database is healthy again.</p>
 *
 * <p>Recovery strategy, cheapest first:</p>
 * <ol>
 *   <li><b>Idle validation</b> - if the connection has not been used for
 *       {@link #IDLE_VALIDATE_MILLIS}, it is pinged once ({@code isValid}) and reopened
 *       when dead. Active connections pay nothing; a connection that goes stale during
 *       an idle period recovers before the caller notices.</li>
 *   <li><b>Reactive heal</b> - a connection-loss {@link SQLException} marks the session
 *       dead so the next use reopens it.</li>
 *   <li><b>Retry</b> - statement-creating calls are retried once against the fresh
 *       session, so the operation that trips over the dead socket usually still
 *       succeeds.</li>
 * </ol>
 *
 * <p>All methods delegate to the current session; {@code close()} closes it and makes
 * the wrapper permanently closed (no reopening after an explicit close).</p>
 */
final class ReconnectingConnection implements InvocationHandler {

    /** Only validate after this much idle time; keeps active workloads round-trip free. */
    private static final long IDLE_VALIDATE_MILLIS = 5_000L;

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final Connection proxy;

    private Connection delegate;
    private volatile boolean closed;
    private volatile long lastUseNanos = System.nanoTime();

    private ReconnectingConnection(String jdbcUrl, String user, String password, Connection initial) {
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        this.user = user;
        this.password = password;
        this.delegate = Objects.requireNonNull(initial, "initial");
        this.proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                this
        );
    }

    /**
     * Wraps a live connection so it reconnects to {@code jdbcUrl} when its session dies.
     *
     * @param jdbcUrl the URL used to reopen the session
     * @param user    the user to reconnect with, may be null (credentials embedded in the URL)
     * @param password the password to reconnect with, may be null
     * @param initial the connection to use until it dies
     * @return the self-healing connection
     */
    static Connection wrap(String jdbcUrl, String user, String password, Connection initial) {
        ReconnectingConnection handler = new ReconnectingConnection(jdbcUrl, user, password, initial);
        return handler.proxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        switch (name) {
            case "close" -> {
                closed = true;
                closeQuietly(delegate);
                delegate = null;
                return null;
            }
            case "isClosed" -> {
                Connection current = delegate;
                return closed || current == null || current.isClosed();
            }
            case "toString" -> {
                return "ReconnectingConnection[" + jdbcUrl + "]";
            }
            case "hashCode" -> {
                return System.identityHashCode(proxy);
            }
            case "equals" -> {
                return proxy == args[0];
            }
            default -> {
                // fall through to delegation below
            }
        }
        if (closed) {
            throw new SQLException("Connection is closed");
        }

        Connection current = ensureLive();
        try {
            return method.invoke(current, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException sql && isConnectionLost(sql)) {
                // Mark the session dead: the next use reopens it. Retry statement
                // creation immediately so a stale idle session stays invisible.
                delegate = null;
                if (isRetryableOnNewSession(name)) {
                    Connection fresh = ensureLive();
                    try {
                        return method.invoke(fresh, args);
                    } catch (InvocationTargetException retryFailure) {
                        throw retryFailure.getCause();
                    }
                }
            }
            throw cause;
        } finally {
            lastUseNanos = System.nanoTime();
        }
    }

    /**
     * Returns a usable session, reopening the connection when it is closed or (after an
     * idle period) no longer responsive.
     */
    private Connection ensureLive() throws SQLException {
        Connection current = delegate;
        if (current != null && !current.isClosed()) {
            boolean idle = System.nanoTime() - lastUseNanos > IDLE_VALIDATE_MILLIS * 1_000_000L;
            if (!idle || isResponsive(current)) {
                return current;
            }
        }
        return reopen();
    }

    private boolean isResponsive(Connection candidate) {
        try {
            return candidate.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }

    private Connection reopen() throws SQLException {
        closeQuietly(delegate);
        Connection fresh = password == null
                ? DriverManager.getConnection(jdbcUrl)
                : DriverManager.getConnection(jdbcUrl, user, password);
        delegate = fresh;
        lastUseNanos = System.nanoTime();
        return fresh;
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) return;
        try {
            connection.close();
        } catch (SQLException ignored) {
            // Already dead; nothing to release.
        }
    }

    /**
     * Only calls that create a new statement (or read metadata) can be retried safely:
     * they have no side effects, and any work done through the returned statement is
     * unaffected. Commits, rollbacks and closes must never be replayed here.
     */
    private static boolean isRetryableOnNewSession(String methodName) {
        return methodName.startsWith("prepare") || methodName.equals("createStatement")
                || methodName.equals("getMetaData") || methodName.equals("isValid");
    }

    /** True when the failure means "this session is gone", not "this statement is invalid". */
    private static boolean isConnectionLost(SQLException e) {
        if (e instanceof SQLNonTransientConnectionException || e instanceof SQLRecoverableException) {
            return true;
        }
        String state = e.getSQLState();
        if (state != null && state.startsWith("08")) {
            return true;
        }
        String message = e.getMessage();
        if (message == null) return false;
        String lower = message.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("communications link failure")
                || lower.contains("connection is closed")
                || lower.contains("connection has been closed")
                || lower.contains("no operations allowed after connection closed")
                || lower.contains("the last packet successfully received")
                || lower.contains("could not receive data from server")
                || lower.contains("socket closed")
                || lower.contains("broken pipe")
                || lower.contains("connection reset");
    }
}
