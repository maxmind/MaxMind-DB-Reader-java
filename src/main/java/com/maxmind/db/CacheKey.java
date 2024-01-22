package com.maxmind.db;

/**
 * {@code CacheKey} is used as a key in the node cache. It contains the offset of the
 * node in the database file, the class of the data at that node, and the type
 * of the data at that node.
 *
 * @param <T> the type of data stored at the node
 */
public final class CacheKey<T> {
    private final int offset;
    private final Class<T> cls;
    private final java.lang.reflect.Type type;

    CacheKey(int offset, Class<T> cls, java.lang.reflect.Type type) {
        this.offset = offset;
        this.cls = cls;
        this.type = type;
    }

    int getOffset() {
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
        int result = offset;
        result = 31 * result + (cls == null ? 0 : cls.hashCode());
        result = 31 * result + (type == null ? 0 : type.hashCode());
        return result;
    }
}
