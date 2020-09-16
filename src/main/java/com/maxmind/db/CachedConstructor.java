package com.maxmind.db;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class CachedConstructor<T> {
    private final Constructor<T> constructor;
    private final Class<?>[] parameterTypes;
    private final java.lang.reflect.Type[] parameterGenericTypes;
    private final Map<ParameterName, Integer> parameterIndexes;
    private final Map<ParameterName, Integer> dbParameterIndexes;

    CachedConstructor(
            Constructor<T> constructor,
            Class<?>[] parameterTypes,
            java.lang.reflect.Type[] parameterGenericTypes,
            Map<ParameterName, Integer> parameterIndexes
    ) {
        this.constructor = constructor;
        this.parameterTypes = parameterTypes;
        this.parameterGenericTypes = parameterGenericTypes;
        this.parameterIndexes = parameterIndexes;
        this.dbParameterIndexes = new ConcurrentHashMap<ParameterName, Integer>();
    }

    Constructor<T> getConstructor() {
        return this.constructor;
    }

    Class<?>[] getParameterTypes() {
        return this.parameterTypes;
    }

    java.lang.reflect.Type[] getParameterGenericTypes() {
        return this.parameterGenericTypes;
    }

    Map<ParameterName, Integer> getParameterIndexes() {
        return this.parameterIndexes;
    }

    Map<ParameterName, Integer> getDBParameterIndexes() {
        return this.dbParameterIndexes;
    }
}
