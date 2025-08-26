package com.maxmind.db;

import java.io.IOException;

/**
 * NodeCache is an interface for a cache that stores decoded values from the
 * data section of the database.
 */
public interface NodeCache {
    /**
     * A loader is used to load a value for a key that is not in the cache.
     */
    interface Loader {
        /**
         * @param key
         *            the key to load
         * @return the value for the key
         * @throws IOException
         *             if there is an error loading the value
         */
        DecodedValue load(CacheKey key) throws IOException;
    }

    /**
     * This method returns the value for the key. If the key is not in the cache
     * then the loader is called to load the value.
     *
     * @param key
     *            the key to look up
     * @param loader
     *            the loader to use if the key is not in the cache
     * @return the value for the key
     * @throws IOException
     *             if there is an error loading the value
     */
    DecodedValue get(CacheKey key, Loader loader) throws IOException;

}
