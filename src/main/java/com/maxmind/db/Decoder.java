package com.maxmind.db;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

/*
 * Decoder for MaxMind DB data.
 */
final class Decoder {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int[] POINTER_VALUE_OFFSETS = { 0, 0, 1 << 11, (1 << 19) + ((1) << 11), 0 };

    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;

    private final NodeCache cache;

    private final long pointerBase;

    private final ByteBuffer buffer;

    static enum Type {
        EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP, INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN, FLOAT;

        // Java clones the array when you call values(). Caching it increased
        // the speed by about 5000 requests per second on my machine.
        final static Type[] values = Type.values();

        public static Type get(int i) {
            return Type.values[i];
        }

        private static Type get(byte b) {
            // bytes are signed, but we want to treat them as unsigned here
            return Type.get(b & 0xFF);
        }

        public static Type fromControlByte(int b) {
            // The type is encoded in the first 3 bits of the byte.
            return Type.get((byte) ((0xFF & b) >>> 5));
        }
    }

    Decoder(NodeCache cache, ByteBuffer buffer, long pointerBase) {
        this.cache = cache;
        this.pointerBase = pointerBase;
        this.buffer = buffer;
    }

    private final NodeCache.Loader cacheLoader = new NodeCache.Loader() {
        @Override
        public JsonNode load(int key) throws IOException {
            return decode(key);
        }
    };

    JsonNode decode(int offset) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "pointer larger than the database.");
        }

        return decode(new AtomicInteger(offset));
    }

    JsonNode decode(AtomicInteger offset) throws IOException {
        int ctrlByte = 0xFF & this.buffer.get(offset.getAndIncrement());

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            int packed = this.decodeInteger(offset, base, pointerSize);
            long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            // for unit testing
            if (this.POINTER_TEST_HACK) {
                return new LongNode(pointer);
            }

            int targetOffset = (int) pointer;
            JsonNode node = cache.get(targetOffset, cacheLoader);
            return node;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.buffer.get(offset.getAndIncrement());

            int typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new InvalidDatabaseException(
                        "Something went horribly wrong in the decoder. An extended type "
                                + "resolved to a type number < 8 (" + typeNum
                                + ")");
            }

            type = Type.get(typeNum);
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            int bytesToRead = size - 28;
            int i = this.decodeInteger(offset, bytesToRead);
            switch (size) {
            case 29:
                size = 29 + i;
                break;
            case 30:
                size = 285 + i;
                break;
            default:
                size = 65821 + (i & (0x0FFFFFFF >>> 32 - 8 * bytesToRead));
            }
        }

        return this.decodeByType(offset, type, size);
    }

    private JsonNode decodeByType(AtomicInteger offset, Type type, int size)
            throws IOException {
        switch (type) {
            case MAP:
                return this.decodeMap(offset, size);
            case ARRAY:
                return this.decodeArray(offset, size);
            case BOOLEAN:
                return Decoder.decodeBoolean(size);
            case UTF8_STRING:
                return new TextNode(this.decodeString(offset, size));
            case DOUBLE:
                return this.decodeDouble(offset, size);
            case FLOAT:
                return this.decodeFloat(offset, size);
            case BYTES:
                return new BinaryNode(this.getByteArray(offset, size));
            case UINT16:
                return this.decodeUint16(offset, size);
            case UINT32:
                return this.decodeUint32(offset, size);
            case INT32:
                return this.decodeInt32(offset, size);
            case UINT64:
                return this.decodeBigInteger(offset, size);
            case UINT128:
                return this.decodeBigInteger(offset, size);
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private String decodeString(AtomicInteger offset, int size) throws CharacterCodingException {
        // the best case when it has direct access to buffer
        if (buffer.hasArray()) {
            return new String(buffer.array(), offset.getAndAdd(size), size, UTF_8);
        // it hasn't so it should read the buffer
        } else {
            byte[] bytes = getByteArray(offset, size);
            return new String(bytes, UTF_8);
        }
    }

    private IntNode decodeUint16(AtomicInteger offset, int size) {
        return new IntNode(this.decodeInteger(offset, size));
    }

    private IntNode decodeInt32(AtomicInteger offset, int size) {
        return new IntNode(this.decodeInteger(offset, size));
    }

    private long decodeLong(AtomicInteger offset, int size) {
        long integer = 0;
        final int o = offset.getAndAdd(size);
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (this.buffer.get(o + i) & 0xFF);
        }
        return integer;
    }

    private LongNode decodeUint32(AtomicInteger offset, int size) {
        return new LongNode(this.decodeLong(offset, size));
    }

    private int decodeInteger(AtomicInteger offset, int size) {
        return this.decodeInteger(offset,0, size);
    }

    private int decodeInteger(AtomicInteger offset, int base, int size) {
        return Decoder.decodeInteger(offset, this.buffer, base, size);
    }

    static int decodeInteger(AtomicInteger offset, ByteBuffer buffer, int base, int size) {
        return decodeInteger(offset.getAndAdd(size), buffer, base, size);
    }

    static int decodeInteger(int offset, ByteBuffer buffer, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get(offset + i) & 0xFF);
        }
        return integer;
    }

    private BigIntegerNode decodeBigInteger(AtomicInteger offset, int size) {
        byte[] bytes = this.getByteArray(offset, size);
        return new BigIntegerNode(new BigInteger(1, bytes));
    }

    private DoubleNode decodeDouble(AtomicInteger offset, int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of double.");
        }
        return new DoubleNode(this.buffer.getDouble(offset.getAndAdd(8)));
    }

    private FloatNode decodeFloat(AtomicInteger offset, int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of float.");
        }
        return new FloatNode(this.buffer.getFloat(offset.getAndAdd(4)));
    }

    private static BooleanNode decodeBoolean(int size)
            throws InvalidDatabaseException {
        switch (size) {
            case 0:
                return BooleanNode.FALSE;
            case 1:
                return BooleanNode.TRUE;
            default:
                throw new InvalidDatabaseException(
                        "The MaxMind DB file's data section contains bad data: "
                                + "invalid size of boolean.");
        }
    }

    private JsonNode decodeArray(AtomicInteger offset, int size) throws IOException {

        List<JsonNode> array = new ArrayList<JsonNode>(size);
        for (int i = 0; i < size; i++) {
            JsonNode r = this.decode(offset);
            array.add(r);
        }

        return new ArrayNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableList(array));
    }

    private JsonNode decodeMap(AtomicInteger offset, int size) throws IOException {
        int capacity = (int) (size / 0.75F + 1.0F);
        Map<String, JsonNode> map = new HashMap<String, JsonNode>(capacity);

        for (int i = 0; i < size; i++) {
            String key = this.decode(offset).asText();
            JsonNode value = this.decode(offset);
            map.put(key, value);
        }

        return new ObjectNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableMap(map));
    }

    private byte[] getByteArray(AtomicInteger offset, int length) {
        return Decoder.getByteArray(offset, this.buffer, length);
    }

    private static byte[] getByteArray(AtomicInteger offset, ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        final int o = offset.getAndAdd(length);
        // the best case when we has access to byte array
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), o, bytes, 0, length);
        // we hasn't got direct access
        } else {
            for (int i = 0; i < length; i++) {
                bytes[i] = buffer.get(o + i);
            }
        }
        return bytes;
    }
}
