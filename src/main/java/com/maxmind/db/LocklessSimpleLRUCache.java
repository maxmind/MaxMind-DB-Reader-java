package com.maxmind.db;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A simplistic cache using a {@link  LinkedHashMap}.
 */
public class LocklessSimpleLRUCache implements NodeCache {

    private static final int DEFAULT_CAPACITY = 4096;

    private final LinkedHashMapLRU cache;

    public LocklessSimpleLRUCache() {
        this(DEFAULT_CAPACITY);
    }

    public LocklessSimpleLRUCache(int capacity) {
        this.cache = new LinkedHashMapLRU(capacity);
    }

    @Override
    public CacheValue get(CacheKey key, Loader loader) throws IOException {
        CacheValue value = cache.get(key);
        if (value == null) {
            value = loader.load(key);
        }

        cache.put(key, value);
        return value;
    }

}
