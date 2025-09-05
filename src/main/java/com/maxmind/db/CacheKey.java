package com.maxmind.db;

/**
 * {@code CacheKey} is used as a key in the data-section cache. It contains the offset of the
 * value in the database file, the class of the value, and the type
 * of the value.
 *
 * @param <T> the type of value
 * @param offset the offset of the value in the database file
 * @param cls the class of the value
 * @param type the type of the value
 */
public record CacheKey<T>(long offset, Class<T> cls, java.lang.reflect.Type type) {
}
