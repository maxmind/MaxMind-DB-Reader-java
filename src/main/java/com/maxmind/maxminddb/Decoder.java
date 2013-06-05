package com.maxmind.maxminddb;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

final class Decoder {

    private final boolean DEBUG;
    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;
    private final long pointerBase;

    private final ObjectMapper objectMapper;

    private final ThreadBuffer threadBuffer;

    static enum Type {
        EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP, INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN;

        public static Type get(int i) {
            // XXX - Type.values() might be expensive. Consider caching it.
            return Type.values()[i];
        }

        private static Type get(byte b) {
            // bytes are signed, but we want to treat them as unsigned here
            // XXX - Type.values() might be expensive. Consider caching it.
            return Type.get(b & 0xFF);
        }

        public static Type fromControlByte(int b) {
            // The type is encoded in the first 3 bits of the byte.
            return Type.get((byte) ((0xFF & b) >>> 5));
        }
    }

    class Result {
        private final JsonNode node;
        private long offset;

        Result(JsonNode node, long offset) {
            this.node = node;
            this.offset = offset;
        }

        JsonNode getNode() {
            return this.node;
        }

        long getOffset() {
            return this.offset;
        }

        void setOffset(long offset) {
            this.offset = offset;
        }

    }

    Decoder(ThreadBuffer threadBuffer, long pointerBase) {
        this.pointerBase = pointerBase;
        this.threadBuffer = threadBuffer;
        this.objectMapper = new ObjectMapper();
        this.DEBUG = System.getenv().get("MAXMIND_DB_DECODER_DEBUG") != null;
    }

    Result decode(long offset) throws MaxMindDbException, IOException {
        ByteBuffer buffer = this.threadBuffer.get();

        buffer.position((int) offset);

        if (this.DEBUG) {
            Log.debug("Offset", String.valueOf(offset));
        }

        int ctrlByte = 0xFF & buffer.get();

        offset++;

        if (this.DEBUG) {
            Log.debugBinary("Control byte", ctrlByte);
        }

        Type type = Type.fromControlByte(ctrlByte);

        if (this.DEBUG) {
            Log.debug("Type", type.name());
        }

        // Pointers are a special case, we don't read the next $size bytes, we
        // use
        // the size to determine the length of the pointer and then follow it.
        if (type.equals(Type.POINTER)) {
            Result pointer = this.decodePointer(ctrlByte, offset);

            if (this.POINTER_TEST_HACK) {
                return pointer;
            }
            Result result = this.decode((pointer.getNode().asLong()));
            result.setOffset(pointer.getOffset());
            return result;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = buffer.get();

            if (this.DEBUG) {
                Log.debug("Next byte", nextByte);
            }

            int typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new MaxMindDbException(
                        "Something went horribly wrong in the decoder. An extended type "
                                + "resolved to a type number < 8 (" + typeNum
                                + ")");
            }

            type = Type.get(typeNum);
            offset++;
        }

        // consider passing by reference or something once I have a better feel
        // for the logic
        long[] sizeArray = this.sizeFromCtrlByte(ctrlByte, offset);
        int size = (int) sizeArray[0];
        offset = sizeArray[1];

        if (this.DEBUG) {
            Log.debug("Size", String.valueOf(size));
        }

        // The map and array types are special cases, since we don't read the
        // next
        // <code>size</code> bytes. For all other types, we do.
        if (type.equals(Type.MAP)) {
            return this.decodeMap(size, offset);
        }

        if (type.equals(Type.ARRAY)) {
            return this.decodeArray(size, offset);
        }

        if (type.equals(Type.BOOLEAN)) {
            return this.decodeBoolean(size, offset);
        }

        if (this.DEBUG) {
            Log.debug("Offset", offset);
            Log.debug("Size", size);
        }

        long new_offset = offset + size;
        switch (type) {
            case UTF8_STRING:
                TextNode s = new TextNode(this.decodeString(size));
                return new Result(s, new_offset);
            case DOUBLE:
                DoubleNode d = this.decodeDouble(size);
                return new Result(d, new_offset);
            case BYTES:
                BinaryNode b = new BinaryNode(this.getByteArray(size));
                return new Result(b, new_offset);
            case UINT16:
                IntNode i = this.decodeUint16(size);
                return new Result(i, new_offset);
            case UINT32:
                LongNode l = this.decodeUint32(size);
                return new Result(l, new_offset);
            case INT32:
                IntNode int32 = this.decodeInt32(size);
                return new Result(int32, new_offset);
            case UINT64:
                BigIntegerNode bi = this.decodeBigInteger(size);
                return new Result(bi, new_offset);
            case UINT128:
                BigIntegerNode uint128 = this.decodeBigInteger(size);
                return new Result(uint128, new_offset);
            default:
                throw new MaxMindDbException("Unknown or unexpected type: "
                        + type.name());

        }
    }

    private final long[] pointerValueOffset = { 0, 0, 1 << 11,
            (((long) 1) << 19) + ((1) << 11), 0 };

    private Result decodePointer(int ctrlByte, long offset) {

        int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;

        if (this.DEBUG) {
            Log.debug("Pointer size", String.valueOf(pointerSize));
        }

        int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);

        long packed = this.decodeLong(base, pointerSize);

        if (this.DEBUG) {
            Log.debug("Packed pointer", String.valueOf(packed));
            Log.debug("Pointer base", this.pointerBase);
            Log.debug("Pointer value offset",
                    this.pointerValueOffset[pointerSize]);
        }

        long pointer = packed + this.pointerBase
                + this.pointerValueOffset[pointerSize];

        if (this.DEBUG) {
            Log.debug("Pointer to", String.valueOf(pointer));
        }

        return new Result(new LongNode(pointer), offset + pointerSize);
    }

    private String decodeString(int size) {
        ByteBuffer buffer = this.threadBuffer.get().slice();
        buffer.limit(size);
        return Charset.forName("UTF-8").decode(buffer).toString();
    }

    private IntNode decodeUint16(int size) {
        return new IntNode(this.decodeInteger(size));
    }

    private IntNode decodeInt32(int size) {
        return new IntNode(this.decodeInteger(size));
    }

    private int decodeInteger(int size) {
        ByteBuffer buffer = this.threadBuffer.get();
        int integer = 0;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get() & 0xFF);
        }
        return integer;
    }

    private LongNode decodeUint32(int size) {
        return new LongNode(this.decodeLong(size));
    }

    private long decodeLong(int size) {
        return this.decodeLong(0, size);
    }

    private long decodeLong(long base, int size) {
        ByteBuffer buffer = this.threadBuffer.get();
        return Decoder.decodeLong(buffer, base, size);
    }

    static long decodeLong(ByteBuffer buffer, long base, int size) {

        long longInt = base;
        for (int i = 0; i < size; i++) {
            longInt = (longInt << 8) | (buffer.get() & 0xFF);
        }
        return longInt;
    }

    private BigIntegerNode decodeBigInteger(int size) {
        byte[] bytes = this.getByteArray(size);
        return new BigIntegerNode(new BigInteger(1, bytes));
    }

    private DoubleNode decodeDouble(int size) {
        byte[] bytes = this.getByteArray(size);
        return new DoubleNode(Double.parseDouble(new String(bytes, Charset
                .forName("US-ASCII"))));
    }

    private Result decodeBoolean(long size, long offset) {
        BooleanNode b = size == 0 ? BooleanNode.FALSE : BooleanNode.TRUE;

        return new Result(b, offset);
    }

    private Result decodeArray(long size, long offset)
            throws MaxMindDbException, IOException {
        if (this.DEBUG) {
            Log.debug("Array size", size);
        }

        ArrayNode array = this.objectMapper.createArrayNode();

        for (int i = 0; i < size; i++) {
            Result r = this.decode(offset);
            offset = r.getOffset();

            if (this.DEBUG) {
                Log.debug("Value " + i, r.getNode().toString());
            }
            array.add(r.getNode());
        }

        if (this.DEBUG) {
            Log.debug("Decoded array", array.toString());
        }

        return new Result(array, offset);
    }

    private Result decodeMap(long size, long offset) throws MaxMindDbException,
            IOException {
        if (this.DEBUG) {
            Log.debug("Map size", size);
        }

        ObjectNode map = this.objectMapper.createObjectNode();

        for (int i = 0; i < size; i++) {
            Result keyResult = this.decode(offset);
            String key = keyResult.getNode().asText();
            offset = keyResult.getOffset();

            Result valueResult = this.decode(offset);
            JsonNode value = valueResult.getNode();
            offset = valueResult.getOffset();

            if (this.DEBUG) {
                Log.debug("Key " + i, key);
                Log.debug("Value " + i, value.toString());
            }
            map.put(key, value);
        }

        if (this.DEBUG) {
            Log.debug("Decoded map", map.toString());
        }

        return new Result(map, offset);

    }

    private long[] sizeFromCtrlByte(int ctrlByte, long offset) {
        int size = ctrlByte & 0x1f;

        if (size < 29) {
            return new long[] { size, offset };
        }

        int bytesToRead = size - 28;

        if (size == 29) {
            int i = this.decodeInteger(bytesToRead);
            size = 29 + i;
        } else if (size == 30) {
            int i = this.decodeInteger(bytesToRead);
            size = 285 + i;
        } else {
            int i = this.decodeInteger(bytesToRead)
                    & (0x0FFFFFFF >>> (32 - (8 * bytesToRead)));
            size = 65821 + i;
        }

        return new long[] { size, offset + bytesToRead };
    }

    private byte[] getByteArray(int length) {
        return Decoder.getByteArray(this.threadBuffer.get(), length);
    }

    private static byte[] getByteArray(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}
