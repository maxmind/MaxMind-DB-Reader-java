package com.maxmind.db;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.math.BigInteger;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Decoder for MaxMind DB data.
 *
 * This class CANNOT be shared between threads
 */
class Decoder {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final int[] POINTER_VALUE_OFFSETS = {0, 0, 1 << 11, (1 << 19) + (1 << 11), 0};

    private final NodeCache cache;

    private final long pointerBase;

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();

    private final Buffer buffer;

    private final ConcurrentHashMap<Class<?>, CachedConstructor<?>> constructors;

    Decoder(NodeCache cache, Buffer buffer, long pointerBase) {
        this(
            cache,
            buffer,
            pointerBase,
            new ConcurrentHashMap<>()
        );
    }

    Decoder(
        NodeCache cache,
        Buffer buffer,
        long pointerBase,
        ConcurrentHashMap<Class<?>, CachedConstructor<?>> constructors
    ) {
        this.cache = cache;
        this.pointerBase = pointerBase;
        this.buffer = buffer;
        this.constructors = constructors;
    }

    private final NodeCache.Loader cacheLoader = this::decode;

    <T> T decode(long offset, Class<T> cls) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        return cls.cast(decode(cls, null).value());
    }

    private <T> DecodedValue decode(CacheKey<T> key) throws IOException {
        long offset = key.offset();
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        Class<T> cls = key.cls();
        return decode(cls, key.type());
    }

    private <T> DecodedValue decode(Class<T> cls, java.lang.reflect.Type genericType)
        throws IOException {
        var ctrlByte = 0xFF & this.buffer.get();

        var type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            var pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            var base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            var packed = this.decodeInteger(base, pointerSize);
            var pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            return decodePointer(pointer, cls, genericType);
        }

        if (type.equals(Type.EXTENDED)) {
            var nextByte = this.buffer.get();

            var typeNum = nextByte + 7;

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
            size = switch (size) {
                case 29 -> 29 + (0xFF & buffer.get());
                case 30 -> 285 + decodeInteger(2);
                default -> 65821 + decodeInteger(3);
            };
        }

        return new DecodedValue(this.decodeByType(type, size, cls, genericType));
    }

    DecodedValue decodePointer(long pointer, Class<?> cls, java.lang.reflect.Type genericType)
            throws IOException {
        var targetOffset = pointer;
        var position = buffer.position();

        var key = new CacheKey<>(targetOffset, cls, genericType);
        var o = cache.get(key, cacheLoader);

        buffer.position(position);
        return o;
    }

    private <T> Object decodeByType(
        Type type,
        int size,
        Class<T> cls,
        java.lang.reflect.Type genericType
    ) throws IOException {
        switch (type) {
            case MAP:
                return this.decodeMap(size, cls, genericType);
            case ARRAY:
                Class<?> elementClass = Object.class;
                if (genericType instanceof ParameterizedType ptype) {
                    var actualTypes = ptype.getActualTypeArguments();
                    if (actualTypes.length == 1) {
                        elementClass = (Class<?>) actualTypes[0];
                    }
                }
                return this.decodeArray(size, cls, elementClass);
            case BOOLEAN:
                return Decoder.decodeBoolean(size);
            case UTF8_STRING:
                return this.decodeString(size);
            case DOUBLE:
                return this.decodeDouble(size);
            case FLOAT:
                return this.decodeFloat(size);
            case BYTES:
                return this.getByteArray(size);
            case UINT16:
                return this.decodeUint16(size);
            case UINT32:
                return this.decodeUint32(size);
            case INT32:
                return this.decodeInt32(size);
            case UINT64:
            case UINT128:
                return this.decodeBigInteger(size);
            default:
                throw new InvalidDatabaseException(
                    "Unknown or unexpected type: " + type.name());
        }
    }

    private String decodeString(long size) throws CharacterCodingException {
        var oldLimit = buffer.limit();
        buffer.limit(buffer.position() + size);
        var s = buffer.decode(utfDecoder);
        buffer.limit(oldLimit);
        return s;
    }

    private int decodeUint16(int size) {
        return this.decodeInteger(size);
    }

    private int decodeInt32(int size) {
        return this.decodeInteger(size);
    }

    private long decodeLong(int size) {
        return Decoder.decodeLong(this.buffer, 0, size);
    }

    static long decodeLong(Buffer buffer, int base, int size) {
        long integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get() & 0xFF);
        }
        return integer;
    }

    private long decodeUint32(int size) {
        return this.decodeLong(size);
    }

    private int decodeInteger(int size) {
        return this.decodeInteger(0, size);
    }

    private int decodeInteger(int base, int size) {
        return Decoder.decodeInteger(this.buffer, base, size);
    }

    static int decodeInteger(Buffer buffer, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get() & 0xFF);
        }
        return integer;
    }

    private BigInteger decodeBigInteger(int size) {
        var bytes = this.getByteArray(size);
        return new BigInteger(1, bytes);
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

    private static boolean decodeBoolean(int size)
        throws InvalidDatabaseException {
        return switch (size) {
            case 0 -> false;
            case 1 -> true;
            default -> throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "invalid size of boolean.");
        };
    }

    private <T, V> List<V> decodeArray(
        int size,
        Class<T> cls,
        Class<V> elementClass
    ) throws IOException {
        if (!List.class.isAssignableFrom(cls) && !cls.equals(Object.class)) {
            throw new DeserializationException("Unable to deserialize an array into an " + cls);
        }

        List<V> array;
        if (cls.equals(List.class) || cls.equals(Object.class)) {
            array = new ArrayList<>(size);
        } else {
            Constructor<T> constructor;
            try {
                constructor = cls.getConstructor(Integer.TYPE);
            } catch (NoSuchMethodException e) {
                throw new DeserializationException(
                    "No constructor found for the List: " + e.getMessage(), e);
            }
            var parameters = new Object[]{size};
            try {
                @SuppressWarnings("unchecked")
                var array2 = (List<V>) constructor.newInstance(parameters);
                array = array2;
            } catch (InstantiationException
                     | IllegalAccessException
                     | InvocationTargetException e) {
                throw new DeserializationException("Error creating list: " + e.getMessage(), e);
            }
        }

        for (int i = 0; i < size; i++) {
            var e = this.decode(elementClass, null).value();
            array.add(elementClass.cast(e));
        }

        return array;
    }

    private <T> Object decodeMap(
        int size,
        Class<T> cls,
        java.lang.reflect.Type genericType
    ) throws IOException {
        if (Map.class.isAssignableFrom(cls) || cls.equals(Object.class)) {
            Class<?> valueClass = Object.class;
            if (genericType instanceof ParameterizedType ptype) {
                var actualTypes = ptype.getActualTypeArguments();
                if (actualTypes.length == 2) {
                    var keyClass = (Class<?>) actualTypes[0];
                    if (!keyClass.equals(String.class)) {
                        throw new DeserializationException("Map keys must be strings.");
                    }

                    valueClass = (Class<?>) actualTypes[1];
                }
            }
            return this.decodeMapIntoMap(cls, size, valueClass);
        }

        return this.decodeMapIntoObject(size, cls);
    }

    private <T, V> Map<String, V> decodeMapIntoMap(
        Class<T> cls,
        int size,
        Class<V> valueClass
    ) throws IOException {
        Map<String, V> map;
        if (cls.equals(Map.class) || cls.equals(Object.class)) {
            map = new HashMap<>(size);
        } else {
            Constructor<T> constructor;
            try {
                constructor = cls.getConstructor(Integer.TYPE);
            } catch (NoSuchMethodException e) {
                throw new DeserializationException(
                    "No constructor found for the Map: " + e.getMessage(), e);
            }
            var parameters = new Object[]{size};
            try {
                @SuppressWarnings("unchecked")
                var map2 = (Map<String, V>) constructor.newInstance(parameters);
                map = map2;
            } catch (InstantiationException
                     | IllegalAccessException
                     | InvocationTargetException e) {
                throw new DeserializationException("Error creating map: " + e.getMessage(), e);
            }
        }

        for (int i = 0; i < size; i++) {
            var key = (String) this.decode(String.class, null).value();
            var value = this.decode(valueClass, null).value();
            try {
                map.put(key, valueClass.cast(value));
            } catch (ClassCastException e) {
                throw new DeserializationException(
                        "Error creating map entry for '" + key + "': " + e.getMessage(), e);
            }
        }

        return map;
    }

    private <T> Object decodeMapIntoObject(int size, Class<T> cls)
        throws IOException {
        var cachedConstructor = getCachedConstructor(cls);
        Constructor<T> constructor;
        Class<?>[] parameterTypes;
        java.lang.reflect.Type[] parameterGenericTypes;
        Map<String, Integer> parameterIndexes;
        if (cachedConstructor == null) {
            constructor = findConstructor(cls);

            parameterTypes = constructor.getParameterTypes();

            parameterGenericTypes = constructor.getGenericParameterTypes();

            parameterIndexes = new HashMap<>();
            var annotations = constructor.getParameterAnnotations();
            for (int i = 0; i < constructor.getParameterCount(); i++) {
                var parameterName = getParameterName(cls, i, annotations[i]);
                parameterIndexes.put(parameterName, i);
            }

            this.constructors.put(
                cls,
                new CachedConstructor<>(
                    constructor,
                    parameterTypes,
                    parameterGenericTypes,
                    parameterIndexes
                )
            );
        } else {
            constructor = cachedConstructor.constructor();
            parameterTypes = cachedConstructor.parameterTypes();
            parameterGenericTypes = cachedConstructor.parameterGenericTypes();
            parameterIndexes = cachedConstructor.parameterIndexes();
        }

        var parameters = new Object[parameterTypes.length];
        for (int i = 0; i < size; i++) {
            var key = (String) this.decode(String.class, null).value();

            var parameterIndex = parameterIndexes.get(key);
            if (parameterIndex == null) {
                var offset = this.nextValueOffset(this.buffer.position(), 1);
                this.buffer.position(offset);
                continue;
            }

            parameters[parameterIndex] = this.decode(
                parameterTypes[parameterIndex],
                parameterGenericTypes[parameterIndex]
            ).value();
        }

        try {
            return constructor.newInstance(parameters);
        } catch (InstantiationException
                 | IllegalAccessException
                 | InvocationTargetException e) {
            throw new DeserializationException("Error creating object: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            var sbErrors = new StringBuilder();
            for (var key : parameterIndexes.keySet()) {
                var index = parameterIndexes.get(key);
                if (parameters[index] != null
                    && !parameters[index].getClass().isAssignableFrom(parameterTypes[index])) {
                    sbErrors.append(" argument type mismatch in " + key + " MMDB Type: "
                        + parameters[index].getClass().getCanonicalName()
                        + " Java Type: " + parameterTypes[index].getCanonicalName());
                }
            }
            throw new DeserializationException(
                "Error creating object of type: " + cls.getSimpleName() + " - " + sbErrors, e);
        }
    }

    private <T> CachedConstructor<T> getCachedConstructor(Class<T> cls) {
        // This cast is safe because we only put CachedConstructor<T> for Class<T> as the key
        @SuppressWarnings("unchecked")
        CachedConstructor<T> result = (CachedConstructor<T>) this.constructors.get(cls);
        return result;
    }

    private static <T> Constructor<T> findConstructor(Class<T> cls)
        throws ConstructorNotFoundException {
        var constructors = cls.getConstructors();
        for (var constructor : constructors) {
            if (constructor.getAnnotation(MaxMindDbConstructor.class) == null) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Constructor<T> constructor2 = (Constructor<T>) constructor;
            return constructor2;
        }

        throw new ConstructorNotFoundException("No constructor on class " + cls.getName()
            + " with the MaxMindDbConstructor annotation was found.");
    }

    private static <T> String getParameterName(
        Class<T> cls,
        int index,
        Annotation[] annotations
    ) throws ParameterNotFoundException {
        for (var annotation : annotations) {
            if (!annotation.annotationType().equals(MaxMindDbParameter.class)) {
                continue;
            }
            var paramAnnotation = (MaxMindDbParameter) annotation;
            return paramAnnotation.name();
        }
        throw new ParameterNotFoundException(
            "Constructor parameter " + index + " on class " + cls.getName()
                + " is not annotated with MaxMindDbParameter.");
    }

    private long nextValueOffset(long offset, int numberToSkip)
        throws InvalidDatabaseException {
        if (numberToSkip == 0) {
            return offset;
        }

        var ctrlData = this.getCtrlData(offset);
        var ctrlByte = ctrlData.ctrlByte();
        var size = ctrlData.size();
        offset = ctrlData.offset();

        var type = ctrlData.type();
        switch (type) {
            case POINTER:
                var pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
                offset += pointerSize;
                break;
            case MAP:
                numberToSkip += 2 * size;
                break;
            case ARRAY:
                numberToSkip += size;
                break;
            case BOOLEAN:
                break;
            default:
                offset += size;
                break;
        }

        return nextValueOffset(offset, numberToSkip - 1);
    }

    private CtrlData getCtrlData(long offset)
        throws InvalidDatabaseException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        var ctrlByte = 0xFF & this.buffer.get();
        offset++;

        var type = Type.fromControlByte(ctrlByte);

        if (type.equals(Type.EXTENDED)) {
            var nextByte = this.buffer.get();

            var typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new InvalidDatabaseException(
                    "Something went horribly wrong in the decoder. An extended type "
                        + "resolved to a type number < 8 (" + typeNum
                        + ")");
            }

            type = Type.get(typeNum);
            offset++;
        }

        var size = ctrlByte & 0x1f;
        if (size >= 29) {
            var bytesToRead = size - 28;
            offset += bytesToRead;
            size = switch (size) {
                case 29 -> 29 + (0xFF & buffer.get());
                case 30 -> 285 + decodeInteger(2);
                default -> 65821 + decodeInteger(3);
            };
        }

        return new CtrlData(type, ctrlByte, offset, size);
    }

    private byte[] getByteArray(int length) {
        return Decoder.getByteArray(this.buffer, length);
    }

    private static byte[] getByteArray(Buffer buffer, int length) {
        var bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}
