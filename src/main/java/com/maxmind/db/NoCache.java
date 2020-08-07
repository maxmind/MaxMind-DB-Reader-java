package com.maxmind.db;

import java.io.IOException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.reflect.InvocationTargetException;

/**
 * A no-op cache singleton.
 */
public class NoCache implements NodeCache {

    private static final NoCache INSTANCE = new NoCache();

    private NoCache() {
    }

    @Override
    public Object get(int key, Class<?> cls, Loader loader)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        return loader.load(key, cls);
    }

    public static NoCache getInstance() {
        return INSTANCE;
    }

}
