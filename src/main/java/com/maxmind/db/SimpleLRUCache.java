package com.maxmind.db;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A simplistic cache using a {@link  LinkedHashMap}.
 */
public class SimpleLRUCache implements NodeCache {

    private static final int DEFAULT_CAPACITY = 4096;

    private final LinkedHashMapLRU cache;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public SimpleLRUCache() {
        this(DEFAULT_CAPACITY);
    }

    public SimpleLRUCache(int capacity) {
        this.cache = new LinkedHashMapLRU(capacity);
    }

    @Override
    public Object get(CacheKey key, Loader loader) throws IOException {
        Object value;
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
             value = cache.get(key);
        } finally {
            readLock.unlock();
        }
        if (value == null) {
            value = loader.load(key);
        }

        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            cache.put(key, value);
        } finally {
            writeLock.unlock();
        }
        return value;
    }

}
