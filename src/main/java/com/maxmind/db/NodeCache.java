package com.maxmind.db;

import java.io.IOException;

/**
 * NodeCache is an interface for a cache that stores decoded nodes.
 */
public interface NodeCache {
    /**
     * A loader is used to load a value for a key that is not in the cache.
     */
    interface Loader {
        DecodedValue load(CacheKey key) throws IOException;
    }

    DecodedValue get(CacheKey key, Loader loader) throws IOException;

}
