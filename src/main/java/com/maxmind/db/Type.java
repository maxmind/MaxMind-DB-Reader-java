package com.maxmind.db;

enum Type {
    EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP, INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN, FLOAT;

    // Java clones the array when you call values(). Caching it increased
    // the speed by about 5000 requests per second on my machine.
    final static Type[] values = Type.values();

    static Type get(int i) throws InvalidDatabaseException {
        if (i >= Type.values.length) {
            throw new InvalidDatabaseException("The MaxMind DB file's data section contains bad data");
        }
        return Type.values[i];
    }

    private static Type get(byte b) throws InvalidDatabaseException {
        // bytes are signed, but we want to treat them as unsigned here
        return Type.get(b & 0xFF);
    }

    static Type fromControlByte(int b) throws InvalidDatabaseException {
        // The type is encoded in the first 3 bits of the byte.
        return Type.get((byte) ((0xFF & b) >>> 5));
    }
}
