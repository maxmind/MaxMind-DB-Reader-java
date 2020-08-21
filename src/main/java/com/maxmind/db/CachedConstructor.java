package com.maxmind.db;

import java.lang.reflect.Constructor;
import java.util.Map;

final class CachedConstructor<T> {
    private final Constructor<T> constructor;
    private final Class<?>[] parameterTypes;
    private final java.lang.reflect.Type[] parameterGenericTypes;
    private final Map<String, Integer> parameterIndexes;

    CachedConstructor(
            Constructor<T> constructor,
            Class<?>[] parameterTypes,
            java.lang.reflect.Type[] parameterGenericTypes,
            Map<String, Integer> parameterIndexes
    ) {
        this.constructor = constructor;
        this.parameterTypes = parameterTypes;
        this.parameterGenericTypes = parameterGenericTypes;
        this.parameterIndexes = parameterIndexes;
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

    Map<String, Integer> getParameterIndexes() {
        return this.parameterIndexes;
    }
}
