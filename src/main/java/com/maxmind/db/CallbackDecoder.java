package com.maxmind.db;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Callback decoder for MaxMind DB data.
 *
 * This class CANNOT be shared between threads
 */
final class CallbackDecoder {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final int[] POINTER_VALUE_OFFSETS = {0, 0, 1 << 11, (1 << 19) + ((1) << 11), 0};

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();

    private final ByteBuffer buffer;
    private final long pointerBase;

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

    CallbackDecoder(ByteBuffer buffer, long pointerBase) {
        this.buffer = buffer;
        this.pointerBase = pointerBase;
    }
}
