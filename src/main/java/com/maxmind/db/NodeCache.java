package com.maxmind.db;

import java.io.IOException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.reflect.InvocationTargetException;

public interface NodeCache {

    interface Loader {
        Object load(CacheKey key)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException,
                   NoSuchMethodException;
    }

    Object get(CacheKey key, Loader loader)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException,
                   NoSuchMethodException;

}
