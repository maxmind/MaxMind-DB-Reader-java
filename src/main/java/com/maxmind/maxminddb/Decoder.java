package com.maxmind.maxminddb;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

public class Decoder {
    private final FileChannel in;

    private final boolean DEBUG;
    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;
    private final long pointerBase;

    private final ObjectMapper objectMapper;

    class Result {
        private final JsonNode obj;
        private long offset;
        private final Type type;

        Result(JsonNode obj, Type t, long offset) {
            this.type = t;
            this.obj = obj;
            this.offset = offset;

        }

        JsonNode getObject() {
            return this.obj;
        }

        long getOffset() {
            return this.offset;
        }

        void setOffset(long offset) {
            this.offset = offset;
        }

    }

    public Decoder(FileChannel in, long pointerBase) {
        this.in = in;
        this.pointerBase = pointerBase;
        this.objectMapper = new ObjectMapper();
        this.DEBUG = System.getenv().get("MAXMIND_DB_DECODER_DEBUG") != null;
    }

    // FIXME - Move most of this method to a switch statement
    public Result decode(long offset) throws MaxMindDbException, IOException {
        this.in.position(offset);
        if (this.DEBUG) {
            Log.debug("Offset", String.valueOf(offset));
        }

        ByteBuffer buffer = ByteBuffer.wrap(new byte[1]);
        this.in.read(buffer);
        int ctrlByte = 0xFF & buffer.get(0);
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
            Result result = this.decode((pointer.getObject().asLong()));
            result.setOffset(pointer.getOffset());
            return result;
        }

        if (type.equals(Type.EXTENDED)) {
            buffer = ByteBuffer.wrap(new byte[1]);
            this.in.read(buffer);
            int nextByte = buffer.get(0);

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
            // FIXME - cast or whatever
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

        buffer = ByteBuffer.wrap(new byte[size]);
        this.in.read(buffer);
        byte[] bytes = buffer.array();

        long new_offset = offset + size;
        switch (type) {
            case UTF8_STRING:
                TextNode s = new TextNode(Decoder.decodeString(bytes));
                return new Result(s, type, new_offset);
            case DOUBLE:
                DoubleNode d = Decoder.decodeDouble(bytes);
                return new Result(d, type, new_offset);
            case BYTES:
                BinaryNode b = new BinaryNode(Decoder.decodeBytes(bytes));
                return new Result(b, type, new_offset);
            case UINT16:
                IntNode i = Decoder.decodeUint16(bytes);
                return new Result(i, type, new_offset);
            case UINT32:
                LongNode l = Decoder.decodeUint32(bytes);
                return new Result(l, type, new_offset);
            case INT32:
                IntNode int32 = Decoder.decodeInt32(bytes);
                return new Result(int32, type, new_offset);
            case UINT64:
                BigIntegerNode bi = Decoder.decodeUint64(bytes);
                return new Result(bi, type, new_offset);
            case UINT128:
                BigIntegerNode uint128 = Decoder.decodeUint128(bytes);
                return new Result(uint128, type, new_offset);
            default:
                throw new MaxMindDbException("Unknown or unexpected type: "
                        + type.name());

        }
    }

    private final long[] pointerValueOffset = { 0, 0, 1 << 11,
            (((long) 1) << 19) + ((1) << 11), 0 };

    private Result decodePointer(int ctrlByte, long offset) throws IOException {

        int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;

        if (this.DEBUG) {
            Log.debug("Pointer size", String.valueOf(pointerSize));
        }

        ByteBuffer buffer = ByteBuffer.wrap(new byte[pointerSize + 1]);

        this.in.read(buffer, this.in.position() - 1);

        if (this.DEBUG) {
            Log.debug("Buffer", buffer);
        }

        buffer.put(0, pointerSize == 4 ? (byte) 0
                : (byte) (ctrlByte & 0x7));

        long packed = Util.decodeLong(buffer.array());

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

        return new Result(new LongNode(pointer), Type.POINTER, offset
                + pointerSize);
    }

    private static String decodeString(byte[] bytes) {
        return new String(bytes, Charset.forName("UTF-8"));
    }

    // XXX - nop
    private static byte[] decodeBytes(byte[] bytes) {
        return bytes;
    }

    static IntNode decodeUint16(byte[] bytes) {
        return new IntNode(Util.decodeInteger(bytes));
    }

    static IntNode decodeInt32(byte[] bytes) {
        return new IntNode(Util.decodeInteger(bytes));
    }

    static LongNode decodeUint32(byte[] bytes) {
        return new LongNode(Util.decodeLong(bytes));
    }

    static BigIntegerNode decodeUint64(byte[] bytes) {
        return new BigIntegerNode(new BigInteger(1, bytes));
    }

    static BigIntegerNode decodeUint128(byte[] bytes) {
        return new BigIntegerNode(new BigInteger(1, bytes));
    }

    private static DoubleNode decodeDouble(byte[] bytes) {
        return new DoubleNode(Double.parseDouble(new String(bytes,
                Charset.forName("US-ASCII"))));
    }

    private Result decodeBoolean(long size, long offset) {
        BooleanNode b = size == 0 ? BooleanNode.FALSE : BooleanNode.TRUE;

        return new Result(b, Type.BOOLEAN, offset);
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
                Log.debug("Value " + i, r.getObject().toString());
            }
            array.add(r.getObject());
        }

        if (this.DEBUG) {
            Log.debug("Decoded array", array.toString());
        }

        return new Result(array, Type.ARRAY, offset);
    }

    private Result decodeMap(long size, long offset) throws MaxMindDbException,
            IOException {
        if (this.DEBUG) {
            Log.debug("Map size", size);
        }

        ObjectNode map = this.objectMapper.createObjectNode();

        for (int i = 0; i < size; i++) {
            Result keyResult = this.decode(offset);
            String key = keyResult.getObject().asText();
            offset = keyResult.getOffset();

            Result valueResult = this.decode(offset);
            JsonNode value = valueResult.getObject();
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

        return new Result(map, Type.MAP, offset);

    }

    private long[] sizeFromCtrlByte(int ctrlByte, long offset)
            throws IOException {
        int size = ctrlByte & 0x1f;

        if (size < 29) {
            return new long[] { size, offset };
        }

        int bytesToRead = size - 28;

        ByteBuffer buffer = ByteBuffer.wrap(new byte[bytesToRead]);
        this.in.read(buffer);

        if (size == 29) {
            int i = Util.decodeInteger(buffer.array());
            size = 29 + i;
        } else if (size == 30) {
            int i = Util.decodeInteger(buffer.array());
            size = 285 + i;
        } else {
            buffer.put(0, (byte) (buffer.get(0) & 0x0F));
            int i = Util.decodeInteger(buffer.array());
            size = 65821 + i;
        }

        return new long[] { size, offset + bytesToRead };
    }

}
