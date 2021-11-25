package com.maxmind.db;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Decoder for MaxMind DB data.
 *
 * This class CANNOT be shared between threads
 */
final class Decoder {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final int[] POINTER_VALUE_OFFSETS = {0, 0, 1 << 11, (1 << 19) + ((1) << 11), 0};

    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;

    private final NodeCache cache;

    private final long pointerBase;

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();

    private final ByteBuffer buffer;

    private final ConcurrentHashMap<Class, CachedConstructor> constructors;

    Decoder(NodeCache cache, ByteBuffer buffer, long pointerBase) {
        this(
                cache,
                buffer,
                pointerBase,
                new ConcurrentHashMap<>()
        );
    }

    Decoder(
            NodeCache cache,
            ByteBuffer buffer,
            long pointerBase,
            ConcurrentHashMap<Class, CachedConstructor> constructors
    ) {
        this.cache = cache;
        this.pointerBase = pointerBase;
        this.buffer = buffer;
        this.constructors = constructors;
    }

    private final NodeCache.Loader cacheLoader = this::decode;

    public <T> T decode(int offset, Class<T> cls) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        return cls.cast(decode(cls, null).getValue());
    }

    private <T> DecodedValue decode(CacheKey<T> key) throws IOException {
        int offset = key.getOffset();
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        Class<T> cls = key.getCls();
        return decode(cls, key.getType());
    }

    private <T> DecodedValue decode(Class<T> cls, java.lang.reflect.Type genericType)
            throws IOException {
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

            // for unit testing
            if (this.POINTER_TEST_HACK) {
                return new DecodedValue(pointer);
            }

            int targetOffset = (int) pointer;
            int position = buffer.position();

            CacheKey key = new CacheKey(targetOffset, cls, genericType);
            DecodedValue o = cache.get(key, cacheLoader);

            buffer.position(position);
            return o;
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

        return new DecodedValue(this.decodeByType(type, size, cls, genericType));
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
                if (genericType instanceof ParameterizedType) {
                    ParameterizedType pType = (ParameterizedType) genericType;
                    java.lang.reflect.Type[] actualTypes
                        = pType.getActualTypeArguments();
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

    private String decodeString(int size) throws CharacterCodingException {
        int oldLimit = buffer.limit();
        buffer.limit(buffer.position() + size);
        String s = utfDecoder.decode(buffer).toString();
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
        long integer = 0;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (this.buffer.get() & 0xFF);
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

    static int decodeInteger(ByteBuffer buffer, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get() & 0xFF);
        }
        return integer;
    }

    private BigInteger decodeBigInteger(int size) {
        byte[] bytes = this.getByteArray(size);
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
                throw new DeserializationException("No constructor found for the List: " + e);
            }
            Object[] parameters = {size};
            try {
                @SuppressWarnings("unchecked")
                List<V> array2 = (List<V>) constructor.newInstance(parameters);
                array = array2;
            } catch (InstantiationException |
                    IllegalAccessException |
                    InvocationTargetException e) {
                throw new DeserializationException("Error creating list: " + e);
            }
        }

        for (int i = 0; i < size; i++) {
            Object e = this.decode(elementClass, null).getValue();
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
            if (genericType instanceof ParameterizedType) {
                ParameterizedType pType = (ParameterizedType) genericType;
                java.lang.reflect.Type[] actualTypes
                    = pType.getActualTypeArguments();
                if (actualTypes.length == 2) {
                    Class<?> keyClass = (Class<?>) actualTypes[0];
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
                throw new DeserializationException("No constructor found for the Map: " + e);
            }
            Object[] parameters = {size};
            try {
                @SuppressWarnings("unchecked")
                Map<String, V> map2 = (Map<String, V>) constructor.newInstance(parameters);
                map = map2;
            } catch (InstantiationException |
                    IllegalAccessException |
                    InvocationTargetException e) {
                throw new DeserializationException("Error creating map: " + e);
            }
        }

        for (int i = 0; i < size; i++) {
            String key = (String) this.decode(String.class, null).getValue();
            Object value = this.decode(valueClass, null).getValue();
            map.put(key, valueClass.cast(value));
        }

        return map;
    }

    private <T> Object decodeMapIntoObject(int size, Class<T> cls)
            throws IOException {
        CachedConstructor<T> cachedConstructor = this.constructors.get(cls);
        Constructor<T> constructor;
        Class<?>[] parameterTypes;
        java.lang.reflect.Type[] parameterGenericTypes;
        Map<String, Integer> parameterIndexes;
        if (cachedConstructor == null) {
            constructor = this.findConstructor(cls);

            parameterTypes = constructor.getParameterTypes();

            parameterGenericTypes = constructor.getGenericParameterTypes();

            parameterIndexes = new HashMap<>();
            Annotation[][] annotations = constructor.getParameterAnnotations();
            for (int i = 0; i < constructor.getParameterTypes().length; i++) {
                String parameterName = this.getParameterName(cls, i, annotations[i]);
                parameterIndexes.put(parameterName, i);
            }

            this.constructors.put(
                    cls,
                    new CachedConstructor(
                        constructor,
                        parameterTypes,
                        parameterGenericTypes,
                        parameterIndexes
                    )
            );
        } else {
            constructor = cachedConstructor.getConstructor();
            parameterTypes = cachedConstructor.getParameterTypes();
            parameterGenericTypes = cachedConstructor.getParameterGenericTypes();
            parameterIndexes = cachedConstructor.getParameterIndexes();
        }

        Object[] parameters = new Object[parameterTypes.length];
        for (int i = 0; i < size; i++) {
            String key = (String) this.decode(String.class, null).getValue();

            Integer parameterIndex = parameterIndexes.get(key);
            if (parameterIndex == null) {
                int offset = this.nextValueOffset(this.buffer.position(), 1);
                this.buffer.position(offset);
                continue;
            }

            parameters[parameterIndex] = this.decode(
                parameterTypes[parameterIndex],
                parameterGenericTypes[parameterIndex]
            ).getValue();
        }

        try {
            return constructor.newInstance(parameters);
        } catch (InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {
            throw new DeserializationException("Error creating object: " + e);
        }
    }

    private static <T> Constructor<T> findConstructor(Class<T> cls)
            throws ConstructorNotFoundException {
        Constructor<?>[] constructors = cls.getConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getAnnotation(MaxMindDbConstructor.class) == null) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Constructor<T> constructor2 = (Constructor<T>) constructor;
            return constructor2;
        }

        throw new ConstructorNotFoundException("No constructor on class " + cls.getName() + " with the MaxMindDbConstructor annotation was found.");
    }

    private static <T> String getParameterName(
            Class<T> cls,
            int index,
            Annotation[] annotations
    ) throws ParameterNotFoundException {
        for (Annotation annotation : annotations) {
            if (!annotation.annotationType().equals(MaxMindDbParameter.class)) {
                continue;
            }
            MaxMindDbParameter paramAnnotation = (MaxMindDbParameter) annotation;
            return paramAnnotation.name();
        }
        throw new ParameterNotFoundException("Constructor parameter " + index + " on class " + cls.getName() + " is not annotated with MaxMindDbParameter.");
    }

    private int nextValueOffset(int offset, int numberToSkip)
        throws InvalidDatabaseException {
        if (numberToSkip == 0) {
            return offset;
        }

        CtrlData ctrlData = this.getCtrlData(offset);
        int ctrlByte = ctrlData.getCtrlByte();
        int size = ctrlData.getSize();
        offset = ctrlData.getOffset();

        Type type = ctrlData.getType();
        switch (type) {
        case POINTER:
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
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

    private CtrlData getCtrlData(int offset)
        throws InvalidDatabaseException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        int ctrlByte = 0xFF & this.buffer.get();
        offset++;

        Type type = Type.fromControlByte(ctrlByte);

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

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            int bytesToRead = size - 28;
            offset += bytesToRead;
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

        return new CtrlData(type, ctrlByte, offset, size);
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
