package com.maxmind.db;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A no-op cache.
 */
public class NoCache implements NodeCache {

    @Override
    public JsonNode get(int key, Loader loader) throws IOException {
        return loader.load(key);
    }

}
