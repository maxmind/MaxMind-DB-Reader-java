package com.maxmind.db;

import java.lang.reflect.Constructor;
import java.util.Map;

record CachedConstructor<T>(
    Constructor<T> constructor,
    Class<?>[] parameterTypes,
    java.lang.reflect.Type[] parameterGenericTypes,
    Map<String, Integer> parameterIndexes,
    Object[] parameterDefaults
) {}
