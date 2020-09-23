package com.maxmind.db;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
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
    private static final int STRING_BUFFER_INITIAL_SIZE = 1024;

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();
    private final CharBuffer charBuffer = CharBuffer.allocate(STRING_BUFFER_INITIAL_SIZE);
    private final StringBuilder stringBuffer = new StringBuilder();

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


    <State> void decode(int offset, AreasOfInterest.Callback<State> callback, State state) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
					       "The MaxMind DB file's data section contains bad data: "
					       + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        decode(callback, state);
    }

    private <State> void decode(AreasOfInterest.Callback<State> callback, State state) throws IOException {
        int ctrlByte = 0xFF & this.buffer.get();

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            int packed = this.decodeInteger(base, pointerSize);
            long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            int targetOffset = (int) pointer;
            int position = buffer.position(); // Save
	    buffer.position(targetOffset);
	    decode(callback, state);
            buffer.position(position); // Restore
	    return;
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
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
	    case 29:
		size = 29 + (0xFF & buffer.get());
		break;
	    case 30:
		size = 285 + decodeInteger(2);
		break;
	    default:
		size = 65821 + decodeInteger(3);
            }
        }

        decodeByType(type, size, callback, state);
    }

    private void skip() throws IOException {
        int ctrlByte = 0xFF & this.buffer.get();

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            //int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            this.skipInteger(pointerSize);
            //long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            //int targetOffset = (int) pointer;
            //int position = buffer.position();
	    return;
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
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
	    case 29:
		size = 29 + (0xFF & buffer.get());
		break;
	    case 30:
		size = 285 + decodeInteger(2);
		break;
	    default:
		size = 65821 + decodeInteger(3);
            }
        }

        skipByType(type, size);
    }

    /** The output is only valid until the next time we decode a string. */
    private CharSequence decodeAsText() throws IOException {
        int ctrlByte = 0xFF & this.buffer.get();

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            int packed = this.decodeInteger(base, pointerSize);
            long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            int targetOffset = (int) pointer;
            int position = buffer.position(); // Save
	    buffer.position(targetOffset);
	    CharSequence result = decodeAsText();
            buffer.position(position); // Restore
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
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
	    case 29:
		size = 29 + (0xFF & buffer.get());
		break;
	    case 30:
		size = 285 + decodeInteger(2);
		break;
	    default:
		size = 65821 + decodeInteger(3);
            }
        }

        return decodeAsTextByType(type, size);
    }

    private <State> void decodeByType(Type type, int size, AreasOfInterest.Callback<State> callback, State state)
            throws IOException {
        switch (type) {
	    case MAP:
		if (callback instanceof AreasOfInterest.ObjectNode) {
		    AreasOfInterest.ObjectNode<State> cb = (AreasOfInterest.ObjectNode<State>)callback;
		    decodeMap(size, cb, state);
		} else {
		    skipMap(size);
		}
		return;
	//throw new RuntimeException("Not implemented"); // return this.decodeMap(size);
            case ARRAY:
                throw new RuntimeException("Not implemented"); // return this.decodeArray(size);
            case BOOLEAN:
                throw new RuntimeException("Not implemented"); // return Decoder.decodeBoolean(size);
            case UTF8_STRING:
		if (callback instanceof AreasOfInterest.TextNode) {
		    AreasOfInterest.TextNode<State> cb = (AreasOfInterest.TextNode<State>)callback;
		    decodeString(size, cb, state); return;
		} else {
		    skipString(size);
		}
            case DOUBLE:
                throw new RuntimeException("Not implemented"); // return this.decodeDouble(size);
            case FLOAT:
                throw new RuntimeException("Not implemented"); // return this.decodeFloat(size);
            case BYTES:
                throw new RuntimeException("Not implemented"); // return new BinaryNode(this.getByteArray(size));
            case UINT16:
                throw new RuntimeException("Not implemented"); // return this.decodeUint16(size);
            case UINT32:
                throw new RuntimeException("Not implemented"); // return this.decodeUint32(size);
            case INT32:
                throw new RuntimeException("Not implemented"); // return this.decodeInt32(size);
            case UINT64:
            case UINT128:
                throw new RuntimeException("Not implemented"); // return this.decodeBigInteger(size);
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private CharSequence decodeAsTextByType(Type type, int size)
            throws IOException {
        switch (type) {
	    case MAP:
		skipMap(size); return "";
            case ARRAY:
		skipArray(size); return "";
            case BOOLEAN:
                return Boolean.toString(decodeBoolean(size));
            case UTF8_STRING:
		return decodeStringAsText(size);
            case DOUBLE:
                throw new RuntimeException("Not implemented"); // return this.decodeDouble(size);
            case FLOAT:
                throw new RuntimeException("Not implemented"); // return this.decodeFloat(size);
            case BYTES:
                throw new RuntimeException("Not implemented"); // return new BinaryNode(this.getByteArray(size));
            case UINT16:
                throw new RuntimeException("Not implemented"); // return this.decodeUint16(size);
            case UINT32:
                throw new RuntimeException("Not implemented"); // return this.decodeUint32(size);
            case INT32:
                throw new RuntimeException("Not implemented"); // return this.decodeInt32(size);
            case UINT64:
            case UINT128:
                throw new RuntimeException("Not implemented"); // return this.decodeBigInteger(size);
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private <State> void skipByType(Type type, int size)
            throws IOException {
        switch (type) {
	    case MAP:
		skipMap(size); return;
            case ARRAY:
		skipArray(size); return;
            case BOOLEAN:
		return;
            case UTF8_STRING:
		skipString(size); return;
            case DOUBLE:
		skipDouble(size); return;
            case FLOAT:
		skipFloat(size); return;
            case BYTES:
		skipByteArray(size); return;
            case UINT16:
		skipInteger(size); return;
            case UINT32:
		skipInteger(size); return;
            case INT32:
		skipInteger(size); return;
            case UINT64:
            case UINT128:
                skipBigInteger(size); return;
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private <State> void decodeString(int size, AreasOfInterest.TextNode<State> callback, State state) throws CharacterCodingException {
        CharSequence value = decodeStringAsText(size);
        callback.setValue(state, value);
    }

    private CharSequence decodeStringAsText(int size) throws CharacterCodingException {
	//System.err.println("DBG| decodeStringAsText @ " + buffer.position());
	charBuffer.clear();

        int oldLimit = buffer.limit();
        buffer.limit(buffer.position() + size);
	{
	    utfDecoder.reset();
	    CoderResult result = utfDecoder.decode(buffer, charBuffer, true);
	    if (result.isError()) throw new CharacterCodingException();
	    result = utfDecoder.flush(charBuffer);
	    if (result.isError()) throw new CharacterCodingException();
	    //TODO: Handle OVERFLOW decently.
	}
        buffer.limit(oldLimit);

	charBuffer.flip();
        return charBuffer;
    }

    private void skipString(int size) {
	skipBytes(size);
    }

    // private IntNode decodeUint16(int size) {
    //     return new IntNode(this.decodeInteger(size));
    // }

    // private IntNode decodeInt32(int size) {
    //     return new IntNode(this.decodeInteger(size));
    // }

    private long decodeLong(int size) {
        long integer = 0;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (this.buffer.get() & 0xFF);
        }
        return integer;
    }

    // private LongNode decodeUint32(int size) {
    //     return new LongNode(this.decodeLong(size));
    // }

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

    private void skipInteger(int size) {
        skipBytes(size);
    }

    // private BigIntegerNode decodeBigInteger(int size) {
    //     byte[] bytes = this.getByteArray(size);
    //     return new BigIntegerNode(new BigInteger(1, bytes));
    // }

    private void skipBigInteger(int size) {
	skipByteArray(size);
    }

    private double decodeDouble(int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of double.");
        }
        return this.buffer.getDouble();
    }

    private float decodeFloat(int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of float.");
        }
        return this.buffer.getFloat();
    }

    private void skipDouble(int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
					       "The MaxMind DB file's data section contains bad data: "
					       + "invalid size of double.");
        }
	skipBytes(8);
    }

    private void skipFloat(int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
					       "The MaxMind DB file's data section contains bad data: "
					       + "invalid size of float.");
        }
	skipBytes(4);
    }

    private static boolean decodeBoolean(int size)
            throws InvalidDatabaseException {
        switch (size) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw new InvalidDatabaseException(
                        "The MaxMind DB file's data section contains bad data: "
                                + "invalid size of boolean.");
        }
    }

    /*
    private JsonNode decodeArray(int size) throws IOException {

        List<JsonNode> array = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            JsonNode r = this.decode();
            array.add(r);
        }

        return new ArrayNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableList(array));
    }
    */

    private void skipArray(int size) throws IOException {
        for (int i = 0; i < size; i++) {
	    skip();
        }
    }


    private <State> void decodeMap(int size, AreasOfInterest.ObjectNode<State> callback, State state) throws IOException {
	callback.objectBegin(state);
        for (int i = 0; i < size; i++) {
	    CharSequence key = this.decodeAsText();
	    AreasOfInterest.Callback<State> fieldCallback = callback.callbackForField(key);
	    //System.err.println("DBG| - map key="+key+" hasCallback="+(fieldCallback != null));
	    if (fieldCallback != null) {
		decode(fieldCallback, state); // Value is of interest.
	    } else {
		skip();
	    }
	}
	callback.objectEnd(state);}

    private void skipMap(int size) throws IOException {
        for (int i = 0; i < size; i++) {
            skip(); // key
            skip(); // value
        }
    }

    private void skipByteArray(int length) {
        skipBytes(length);
    }

    private void skipBytes(int size) {
        buffer.position(buffer.position() + size);
    }
}
