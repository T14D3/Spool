package de.t14d3.spool.cache;

public final class NoOpCacheEventSink implements CacheEventSink {
    @Override
    public void append(CacheEvent event) {
        // no-op
    }
}

