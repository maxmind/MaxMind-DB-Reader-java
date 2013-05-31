package com.maxmind.maxminddb;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Decoder {
    private final InputStream in;

    private final boolean DEBUG = true;
    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;
    private final long pointerBase;

    class Result {
        private final Object obj;
        private final long new_offset;
        private final Type type;

        Result(Object obj, Type t, long new_offset) {
            this.type = t;
            this.obj = obj;
            this.new_offset = new_offset;
        }

        Object getObject() {
            return this.obj;
        }

        long getOffset() {
            return this.new_offset;
        }

        Type getType() {
            return this.type;
        }
    }

    public Decoder(InputStream in, long pointerBase) {
        this.in = in;
        this.pointerBase = pointerBase;
    }

    // FIXME - Move most of this method to a switch statement
    public Result decode(long offset) throws MaxMindDbException, IOException {

        if (this.DEBUG) {
            this.debug("Offset", String.valueOf(offset));
        }

        int ctrlByte = this.in.read();
        offset++;

        if (this.DEBUG) {
            this.debug("Control byte", ctrlByte);
        }

        Type type = Type.fromControlByte(ctrlByte);

        if (this.DEBUG) {
            this.debug("Type", type.name());
        }

        // Pointers are a special case, we don't read the next $size bytes, we
        // use
        // the size to determine the length of the pointer and then follow it.
        if (type.equals(Type.POINTER)) {
            Result pointer = this.decodePointer(ctrlByte, offset);

            if (this.POINTER_TEST_HACK) {
                return pointer;
            }

            return this.decode(((Long) pointer.getObject()).longValue());
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.in.read();

            if (this.DEBUG) {
                this.debug("Next byte", nextByte);
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
            this.debug("Size", String.valueOf(size));
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
            this.debug("Offset", offset);
            this.debug("Size", size);
        }
        byte[] buffer = new byte[size];

        this.in.read(buffer, 0, size);

        long new_offset = offset + size;
        switch (type) {
            case UTF8_STRING:
                String s = this.decodeString(buffer);
                return new Result(s, type, new_offset);
            case DOUBLE:
                double d = this.decodeDouble(buffer);
                return new Result(d, type, new_offset);
            case BYTES:
                byte[] b = this.decodeBytes(buffer);
                return new Result(b, type, new_offset);
            case UINT16:
                int i = Decoder.decodeUint16(buffer);
                return new Result(i, type, new_offset);
            case UINT32:
                long l = Decoder.decodeUint32(buffer);
                return new Result(l, type, new_offset);
            case INT32:
                int int32 = Decoder.decodeInt32(buffer);
                return new Result(int32, type, new_offset);
            case UINT64:
                BigInteger bi = Decoder.decodeUint64(buffer);
                return new Result(bi, type, new_offset);
            case UINT128:
                BigInteger uint128 = Decoder.decodeUint128(buffer);
                return new Result(uint128, type, new_offset);
            default:
                throw new MaxMindDbException("Unknown or unexpected type: "
                        + type.name());

        }
    }

    private void debug(String name, int value) {
        this.debug(name, String.valueOf(value));
    }

    private final long[] pointerValueOffset = { 0, 0, 1 << 11,
            (((long) 1) << 19) + ((1) << 11), 0 };

    private Result decodePointer(int ctrlByte, long offset) throws IOException {

        int pointerSize = ((ctrlByte >>> 3) & 0b00000011) + 1;

        if (this.DEBUG) {
            this.debug("Pointer size", String.valueOf(pointerSize));
        }

        byte buffer[] = new byte[pointerSize + 1];

        this.in.read(buffer, 1, pointerSize);

        if (this.DEBUG) {
            this.debug("Buffer", buffer);
        }

        buffer[0] = pointerSize == 4 ? (byte) 0
                : (byte) (ctrlByte & 0b00000111);

        long packed = decodeLong(buffer);

        if (this.DEBUG) {
            this.debug("Packed pointer", String.valueOf(packed));
            this.debug("Pointer base", this.pointerBase);
            this.debug("Pointer value offset",
                    this.pointerValueOffset[pointerSize]);
        }

        long pointer = packed + this.pointerBase
                + this.pointerValueOffset[pointerSize];

        if (this.DEBUG) {
            this.debug("Pointer to", String.valueOf(pointer));
        }

        return new Result(pointer, Type.POINTER, offset + pointerSize);
    }

    private String decodeString(byte[] bytes) {
        return new String(bytes, Charset.forName("UTF-8"));
    }

    // XXX - nop
    private byte[] decodeBytes(byte[] bytes) {
        return bytes;
    }

    static int decodeUint16(byte[] bytes) {
        return Decoder.decodeInt32(bytes);
    }

    static int decodeInt32(byte[] bytes) {
        return decodeInteger(bytes);
    }

    static long decodeUint32(byte[] bytes) {
        return decodeLong(bytes);
    }

    static BigInteger decodeUint64(byte[] bytes) {
        return new BigInteger(1, bytes);
    }

    static BigInteger decodeUint128(byte[] bytes) {
        return new BigInteger(1, bytes);
    }

    static int decodeInteger(byte[] bytes) {
        int i = 0;
        for (byte b : bytes) {
            i = (i << 8) | (b & 0xFF);
        }
        return i;
    }

    static long decodeLong(byte[] bytes) {
        long i = 0;
        for (byte b : bytes) {
            i = (i << 8) | (b & 0xFF);
        }
        return i;
    }

    private double decodeDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    private Result decodeBoolean(long size, long offset) {
        boolean b = size == 0 ? false : true;

        return new Result(b, Type.BOOLEAN, offset);
    }

    private Result decodeArray(long size, long offset)
            throws MaxMindDbException, IOException {
        if (this.DEBUG) {
            this.debug("Array size", size);
        }

        Object[] array = new Object[(int) size];

        for (int i = 0; i < array.length; i++) {
            Result r = this.decode(offset);
            offset = r.getOffset();

            if (this.DEBUG) {
                this.debug("Value " + i, r.getObject().toString());
            }
            array[i] = r.getObject();
        }

        if (this.DEBUG) {
            this.debug("Decoded array", Arrays.toString(array));
        }

        return new Result(array, Type.ARRAY, offset);
    }

    private Result decodeMap(long size, long offset) throws MaxMindDbException,
            IOException {
        if (this.DEBUG) {
            this.debug("Map size", size);
        }

        Map<String, Object> map = new HashMap<String, Object>();

        for (int i = 0; i < size; i++) {
            Result keyResult = this.decode(offset);
            String key = (String) keyResult.getObject();
            offset = keyResult.getOffset();

            Result valueResult = this.decode(offset);
            Object value = valueResult.getObject();
            offset = valueResult.getOffset();

            if (this.DEBUG) {
                this.debug("Key " + i, key);
                this.debug("Value " + i, value.toString());
            }
            map.put(key, value);
        }

        if (this.DEBUG) {
            this.debug("Decoded map", map.toString());
        }

        return new Result(map, Type.MAP, offset);

    }

    private long[] sizeFromCtrlByte(int ctrlByte, long offset)
            throws IOException {
        int size = ctrlByte & 0b00011111;

        if (size < 29) {
            return new long[] { size, offset };
        }

        int bytesToRead = size - 28;

        byte[] buffer = new byte[bytesToRead];
        this.in.read(buffer, 0, bytesToRead);

        if (size == 29) {
            int i = decodeInteger(buffer);
            size = 29 + i;
        } else if (size == 30) {
            int i = decodeInteger(buffer);
            size = 285 + i;
        } else {
            buffer[0] &= 0x0F;
            int i = decodeInteger(buffer);
            size = 65821 + i;
        }

        return new long[] { size, offset + bytesToRead };
    }

    private void debug(String name, long offset) {
        this.debug(name, String.valueOf(offset));
    }

    private void debug(String string, byte[] buffer) {
        String binary = "";
        for (byte b : buffer) {
            binary += Integer.toBinaryString(b & 0xFF) + " ";
        }
        this.debug(string, binary);

    }

    private void debug(String string, byte b) {
        this.debug(string, Integer.toBinaryString(b & 0xFF));
    }

    private void debug(String name, String value) {
        System.out.println(name + ": " + value);
    }

}
