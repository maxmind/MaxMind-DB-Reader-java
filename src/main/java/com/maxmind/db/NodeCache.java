package com.maxmind.db;

import java.io.IOException;

public interface NodeCache {

    interface Loader {
        Object load(CacheKey key) throws IOException;
    }

    Object get(CacheKey key, Loader loader) throws IOException;

}
