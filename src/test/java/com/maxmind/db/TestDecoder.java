package com.maxmind.db;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;

final class TestDecoder extends Decoder {

    TestDecoder(NodeCache cache, Buffer buffer, long pointerBase) {
        super(cache, buffer, pointerBase);
    }

    @Override
    DecodedValue decodePointer(long pointer, Class<?> cls, Type genericType) {
        // bypass cache
        return new DecodedValue(pointer);
    }

}
