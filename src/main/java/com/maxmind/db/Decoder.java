package com.maxmind.db;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

/*
 * Decoder for MaxMind DB data.
 *
 * This class CANNOT be shared between threads
 */
final class Decoder {
    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;
    private final long pointerBase;

    private final ObjectMapper objectMapper;

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

    static class Result {
        private final JsonNode node;
        private int offset;

        Result(JsonNode node, int offset) {
            this.node = node;
            this.offset = offset;
        }

        JsonNode getNode() {
            return this.node;
        }

        int getOffset() {
            return this.offset;
        }

        void setOffset(int offset) {
            this.offset = offset;
        }

    }

    Decoder(ByteBuffer buffer, long pointerBase) {
        this.pointerBase = pointerBase;
        this.buffer = buffer;
        this.objectMapper = new ObjectMapper();
    }

    Result decode(int offset) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        int ctrlByte = 0xFF & this.buffer.get();
        offset++;

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            Result pointer = this.decodePointer(ctrlByte, offset);

            // for unit testing
            if (this.POINTER_TEST_HACK) {
                return pointer;
            }
            Result result = this.decode((pointer.getNode().asInt()));
            result.setOffset(pointer.getOffset());
            return result;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.buffer.get();

            int typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new InvalidDatabaseException(
                        "Something went horribly wrong in the decoder. An extended type "
                                + "resolved to a type number < 8 (" + typeNum
                                + ")");
            }

            type = Type.get(typeNum);
            offset++;
        }

        int[] sizeArray = this.sizeFromCtrlByte(ctrlByte, offset);
        int size = sizeArray[0];
        offset = sizeArray[1];

        return this.decodeByType(type, offset, size);
    }

    private Result decodeByType(Type type, int offset, int size)
            throws IOException {
        // MAP, ARRAY, and BOOLEAN do not use newOffset as we don't read the
        // next <code>size</code> bytes. For all other types, we do.
        int newOffset = offset + size;
        switch (type) {
            case MAP:
                return this.decodeMap(size, offset);
            case ARRAY:
                return this.decodeArray(size, offset);
            case BOOLEAN:
                return new Result(Decoder.decodeBoolean(size), offset);
            case UTF8_STRING:
                TextNode s = new TextNode(this.decodeString(size));
                return new Result(s, newOffset);
            case DOUBLE:
                return new Result(this.decodeDouble(size), newOffset);
            case FLOAT:
                return new Result(this.decodeFloat(size), newOffset);
            case BYTES:
                BinaryNode b = new BinaryNode(this.getByteArray(size));
                return new Result(b, newOffset);
            case UINT16:
                IntNode i = this.decodeUint16(size);
                return new Result(i, newOffset);
            case UINT32:
                LongNode l = this.decodeUint32(size);
                return new Result(l, newOffset);
            case INT32:
                IntNode int32 = this.decodeInt32(size);
                return new Result(int32, newOffset);
            case UINT64:
                BigIntegerNode bi = this.decodeBigInteger(size);
                return new Result(bi, newOffset);
            case UINT128:
                BigIntegerNode uint128 = this.decodeBigInteger(size);
                return new Result(uint128, newOffset);
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private final int[] pointerValueOffset = { 0, 0, 1 << 11,
            (1 << 19) + ((1) << 11), 0 };

    private Result decodePointer(int ctrlByte, int offset) {
        int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
        int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
        int packed = this.decodeInteger(base, pointerSize);
        long pointer = packed + this.pointerBase
                + this.pointerValueOffset[pointerSize];

        return new Result(new LongNode(pointer), offset + pointerSize);
    }

    private String decodeString(int size) {
        ByteBuffer buffer = this.buffer.slice();
        buffer.limit(size);
        return Charset.forName("UTF-8").decode(buffer).toString();
    }

    private IntNode decodeUint16(int size) {
        return new IntNode(this.decodeInteger(size));
    }

    private IntNode decodeInt32(int size) {
        return new IntNode(this.decodeInteger(size));
    }

    private long decodeLong(int size) {
        long integer = 0;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (this.buffer.get() & 0xFF);
        }
        return integer;
    }

    private LongNode decodeUint32(int size) {
        return new LongNode(this.decodeLong(size));
    }

    private int decodeInteger(int size) {
        return this.decodeInteger(0, size);
    }

    private int decodeInteger(int base, int size) {
        return Decoder.decodeInteger(this.buffer, base, size);
    }

    static int decodeInteger(ByteBuffer buffer, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get() & 0xFF);
        }
        return integer;
    }

    private BigIntegerNode decodeBigInteger(int size) {
        byte[] bytes = this.getByteArray(size);
        return new BigIntegerNode(new BigInteger(1, bytes));
    }

    private DoubleNode decodeDouble(int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of double.");
        }
        return new DoubleNode(this.buffer.getDouble());
    }

    private FloatNode decodeFloat(int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of float.");
        }
        return new FloatNode(this.buffer.getFloat());
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

    private Result decodeArray(int size, int offset) throws IOException {
        ArrayNode array = this.objectMapper.createArrayNode();

        for (int i = 0; i < size; i++) {
            Result r = this.decode(offset);
            offset = r.getOffset();
            array.add(r.getNode());
        }

        return new Result(array, offset);
    }

    private Result decodeMap(int size, int offset) throws IOException {
        ObjectNode map = this.objectMapper.createObjectNode();

        for (int i = 0; i < size; i++) {
            Result keyResult = this.decode(offset);
            String key = keyResult.getNode().asText();
            offset = keyResult.getOffset();

            Result valueResult = this.decode(offset);
            JsonNode value = valueResult.getNode();
            offset = valueResult.getOffset();

            map.set(key, value);
        }

        return new Result(map, offset);
    }

    private int[] sizeFromCtrlByte(int ctrlByte, int offset) {
        int size = ctrlByte & 0x1f;
        int bytesToRead = size < 29 ? 0 : size - 28;

        if (size == 29) {
            int i = this.decodeInteger(bytesToRead);
            size = 29 + i;
        } else if (size == 30) {
            int i = this.decodeInteger(bytesToRead);
            size = 285 + i;
        } else if (size > 30) {
            int i = this.decodeInteger(bytesToRead)
                    & (0x0FFFFFFF >>> (32 - (8 * bytesToRead)));
            size = 65821 + i;
        }
        return new int[] { size, offset + bytesToRead };
    }

    private byte[] getByteArray(int length) {
        return Decoder.getByteArray(this.buffer, length);
    }

    private static byte[] getByteArray(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}
