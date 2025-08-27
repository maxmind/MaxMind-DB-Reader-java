package com.maxmind.db;

import java.io.IOException;

/**
 * A no-op cache singleton.
 */
public final class NoCache implements NodeCache {

    private static final NoCache INSTANCE = new NoCache();

    private NoCache() {
    }

    @Override
    public DecodedValue get(CacheKey<?> key, Loader loader) throws IOException {
        return loader.load(key);
    }
   
    /**
     * @return the singleton instance of the NoCache class
     */
    public static NoCache getInstance() {
        return INSTANCE;
    }

}
