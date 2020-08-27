package com.maxmind.db;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simplistic cache using a {@link ConcurrentHashMap}. There's no eviction
 * policy, it just fills up until reaching the specified capacity <small>(or
 * close enough at least, bounds check is not atomic :)</small>
 */
public class LRUCache implements NodeCache {

    private static final int DEFAULT_CAPACITY = 4096;

    private final int capacity;
    private final ConcurrentHashMap<CacheKey, LRUElement> cache;
    private LRUElement oldest;
    private LRUElement newest;


    public LRUCache() {
        this(DEFAULT_CAPACITY);
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public CacheValue get(CacheKey key, Loader loader) throws IOException {
        LRUElement element = cache.get(key);
        if (element == null) {
            element = new LRUElement();
            element.key = key;
            element.value =  loader.load(key);
            element.older = newest;
            if (oldest == null) {
                // first element
                oldest = element;
            } else {
                newest.newer = element;
            }
            newest = element;
            cache.put(key, element);
            maybeRemoveOldest();
        } else {
            makeNewest(element);
        }
        return element.value;
    }

     void makeNewest(LRUElement element) {
        if (newest == element) {
            // already newest
            return;
        }

        if (oldest == element) {
            element.newer.older = null;
            oldest = element.newer;
        } else {
            element.older.newer = element.newer;
            element.newer.older = element.older;
        }

        element.newer = null;
        element.older = newest;
        newest.newer = element;
        newest = element;
    }

    void maybeRemoveOldest() {
        if (cache.size() < capacity) {
            return;
        }
        cache.remove(oldest.key);
        oldest = oldest.newer;
        oldest.older = null;
    }
}
