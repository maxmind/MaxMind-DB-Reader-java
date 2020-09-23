package com.maxmind.db;

import java.io.IOException;

public interface NodeCache {

    interface Loader {
        DecodedValue load(CacheKey key) throws IOException;
    }

    DecodedValue get(CacheKey key, Loader loader) throws IOException;

}
