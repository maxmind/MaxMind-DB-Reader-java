package com.maxmind.db;

import java.lang.reflect.Method;

/**
 * Cached creator method information for efficient deserialization.
 * A creator method is a static factory method annotated with {@link MaxMindDbCreator}
 * that converts a decoded value to the target type.
 *
 * @param method the static factory method annotated with {@link MaxMindDbCreator}
 * @param parameterType the parameter type accepted by the creator method
 */
record CachedCreator(
    Method method,
    Class<?> parameterType
) {}
