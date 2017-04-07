package org.graylog.jest.okhttp.config.idle;

import io.searchbox.client.config.idle.ReapableConnectionManager;
import okhttp3.ConnectionPool;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class HttpReapableConnectionManager implements ReapableConnectionManager {
    private final ConnectionPool connectionPool;

    public HttpReapableConnectionManager(ConnectionPool connectionPool) {
        this.connectionPool = requireNonNull(connectionPool, "ConnectionPool must not be null");
    }

    @Override
    public void closeIdleConnections(long idleTimeout, TimeUnit unit) {
        connectionPool.evictAll();
    }
}
