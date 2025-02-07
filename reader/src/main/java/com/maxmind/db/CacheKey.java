package com.maxmind.db;

/**
 * {@code CacheKey} is used as a key in the data-section cache. It contains the offset of the
 * value in the database file, the class of the value, and the type
 * of the value.
 *
 * @param <T> the type of value
 */
public final class CacheKey<T> {
    private final long offset;
    private final Class<T> cls;
    private final java.lang.reflect.Type type;

    CacheKey(long offset, Class<T> cls, java.lang.reflect.Type type) {
        this.offset = offset;
        this.cls = cls;
        this.type = type;
    }

    long getOffset() {
        return this.offset;
    }

    Class<T> getCls() {
        return this.cls;
    }

    java.lang.reflect.Type getType() {
        return this.type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        CacheKey other = (CacheKey) o;

        if (this.offset != other.offset) {
            return false;
        }

        if (this.cls == null) {
            if (other.cls != null) {
                return false;
            }
        } else if (!this.cls.equals(other.cls)) {
            return false;
        }

        if (this.type == null) {
            return other.type == null;
        }
        return this.type.equals(other.type);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + (cls == null ? 0 : cls.hashCode());
        result = 31 * result + (type == null ? 0 : type.hashCode());
        return result;
    }
}
