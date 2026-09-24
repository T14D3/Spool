package de.t14d3.spool.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts calls to {@code prepareStatement} on a JDBC connection so tests can
 * assert that a code path did (or did not) issue SQL.
 */
final class QueryCountingConnection implements InvocationHandler {
    private final Connection delegate;
    private final AtomicInteger preparedStatements = new AtomicInteger();
    private final Connection proxy;

    QueryCountingConnection(Connection delegate) {
        this.delegate = delegate;
        this.proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                this);
    }

    Connection getProxy() {
        return proxy;
    }

    void reset() {
        preparedStatements.set(0);
    }

    int getPreparedStatements() {
        return preparedStatements.get();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("prepareStatement".equals(method.getName())) {
            preparedStatements.incrementAndGet();
        }
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
