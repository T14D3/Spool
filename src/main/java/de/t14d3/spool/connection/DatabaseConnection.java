package de.t14d3.spool.connection;

import de.t14d3.spool.core.Persister;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnection {
    private final Connection conn;
    private final QueryExecutor executor;
    private final Persister persister;

    public DatabaseConnection(String jdbcUrl) {
        try {
            this.conn = DriverManager.getConnection(jdbcUrl);
            this.executor = new QueryExecutor(conn);
            this.persister = new Persister(executor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public QueryExecutor getExecutor() {
        return executor;
    }

    public Persister getPersister() {
        return persister;
    }

    public void close() {
        try { conn.close(); }
        catch (Exception ignored) {}
    }
}
