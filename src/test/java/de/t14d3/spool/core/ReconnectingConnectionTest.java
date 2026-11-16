package de.t14d3.spool.core;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for {@link ReconnectingConnection}.
 *
 * <p>Spool hands one connection to the whole process; without the wrapper a dead
 * session (database restart, idle timeout, network blip) stayed dead for the rest of
 * the process lifetime. These tests cover both recovery paths: reopening a closed
 * delegate and retrying statement creation after a connection-loss failure.</p>
 */
class ReconnectingConnectionTest {

    @Test
    void reopensAfterTheUnderlyingConnectionDies() throws Exception {
        String url = "jdbc:h2:mem:reconnect_close;DB_CLOSE_DELAY=-1";
        Connection raw = DriverManager.getConnection(url);
        Connection connection = ReconnectingConnection.wrap(url, null, null, raw);

        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE reconnect_probe (id INT PRIMARY KEY)");
        }

        // The database session dies (what a server restart / wait_timeout does).
        raw.close();
        assertTrue(raw.isClosed(), "precondition: the session is gone");

        // The very next use must reopen and keep working.
        try (Statement st = connection.createStatement()) {
            st.execute("INSERT INTO reconnect_probe VALUES (1)");
        }
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM reconnect_probe")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "row must survive the reconnect");
        }

        connection.close();
        assertTrue(connection.isClosed(), "closing the wrapper closes it for good");
    }

    @Test
    void retriesStatementCreationAfterConnectionLoss() throws Exception {
        Connection healthy = DriverManager.getConnection("jdbc:h2:mem:reconnect_retry;DB_CLOSE_DELAY=-1");
        try (Statement st = healthy.createStatement()) {
            st.execute("CREATE TABLE reconnect_retry (id INT)");
        }

        AtomicInteger failures = new AtomicInteger();
        InvocationHandler flaky = (proxy, method, args) -> {
            boolean statementCreation = method.getName().startsWith("prepare")
                    || method.getName().equals("createStatement");
            if (statementCreation && failures.getAndIncrement() == 0) {
                // First statement creation trips over the dead session...
                throw new SQLException("Communications link failure", "08S01");
            }
            try {
                return method.invoke(healthy, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        Connection flakyConnection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, flaky);

        Connection connection = ReconnectingConnection.wrap(
                "jdbc:h2:mem:reconnect_retry;DB_CLOSE_DELAY=-1", null, null, flakyConnection);

        // ...and the same call must succeed against the reopened session.
        try (Statement st = connection.createStatement()) {
            st.execute("INSERT INTO reconnect_retry VALUES (1)");
        }
        assertEquals(1, failures.get(), "the dead session must have been hit exactly once");

        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM reconnect_retry")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        connection.close();
        healthy.close();
    }
}
