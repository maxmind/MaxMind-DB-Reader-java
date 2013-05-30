package com.maxmind.maxminddb;

enum Type {
    EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP, INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN;

    public static Type get(int i) {
        // XXX - Type.values() might be expensive. Consider caching it.
        return Type.values()[i];
    }

    public static Type get(byte b) {
        // bytes are signed, but we want to treat them as unsigned here
        // XXX - Type.values() might be expensive. Consider caching it.
        return Type.get(b & 0xFF);
    }

    public static Type fromControlByte(int b) {
        // The type is encoded in the first 3 bits of the byte.
        return Type.get((byte) ((0xFF & b) >>> 5));
    }
}