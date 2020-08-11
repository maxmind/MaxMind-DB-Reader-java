package com.maxmind.db;

import java.io.IOException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.reflect.InvocationTargetException;

public interface NodeCache {

    interface Loader {
        Object load(int key, Class<?> cls)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException,
                   NoSuchMethodException ;
    }

    Object get(int key, Class<?> cls, Loader loader)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException,
                   NoSuchMethodException;

}
