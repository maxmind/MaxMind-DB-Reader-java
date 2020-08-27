package com.maxmind.db;

import java.io.IOException;

public interface NodeCache {

    interface Loader {
        CacheValue load(CacheKey key) throws IOException;
    }

    CacheValue get(CacheKey key, Loader loader) throws IOException;

}
