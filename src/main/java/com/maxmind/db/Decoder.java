package com.maxmind.db;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
        var position = buffer.position();

        var key = new CacheKey<>(pointer, cls, genericType);
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
                var s = this.decodeString(size);
                var created = tryCreateFromScalar(cls, s);
                if (created != null) {
                    return created;
                }
                if (cls.isEnum()) {
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    var enumClass = (Class<? extends Enum>) cls;
                    try {
                        // Attempt a forgiving mapping commonly needed for ConnectionType
                        var candidate = s.trim()
                            .replace(' ', '_')
                            .replace('-', '_')
                            .replace('/', '_')
                            .toUpperCase();
                        return Enum.valueOf(enumClass, candidate);
                    } catch (IllegalArgumentException ignored) {
                        // fall through to return the raw string
                    }
                }
                return s;
            case DOUBLE:
                var d = this.decodeDouble(size);
                {
                    var created2 = tryCreateFromScalar(cls, d);
                    if (created2 != null) {
                        return created2;
                    }
                }
                return d;
            case FLOAT:
                var f = this.decodeFloat(size);
                {
                    var created3 = tryCreateFromScalar(cls, f);
                    if (created3 != null) {
                        return created3;
                    }
                }
                return f;
            case BYTES:
                var bytes = this.getByteArray(size);
                {
                    var created4 = tryCreateFromScalar(cls, bytes);
                    if (created4 != null) {
                        return created4;
                    }
                }
                return bytes;
            case UINT16:
                return coerceFromInt(this.decodeUint16(size), cls);
            case UINT32:
                return coerceFromLong(this.decodeUint32(size), cls);
            case INT32:
                return coerceFromInt(this.decodeInt32(size), cls);
            case UINT64:
            case UINT128:
                var bi = this.decodeBigInteger(size);
                {
                    var created5 = tryCreateFromScalar(cls, bi);
                    if (created5 != null) {
                        return created5;
                    }
                }
                return bi;
            default:
                throw new InvalidDatabaseException(
                    "Unknown or unexpected type: " + type.name());
        }
    }

    private static Object coerceFromInt(int value, Class<?> target) {
        // If a creator exists that accepts an Integer-compatible value, use it
        var created = tryCreateFromScalar(target, Integer.valueOf(value));
        if (created != null) {
            return created;
        }
        if (target.equals(Object.class)
            || target.equals(Integer.TYPE)
            || target.equals(Integer.class)) {
            return value;
        }
        if (target.equals(Long.TYPE) || target.equals(Long.class)) {
            return (long) value;
        }
        if (target.equals(Short.TYPE) || target.equals(Short.class)) {
            if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                throw new DeserializationException("Value " + value + " out of range for short");
            }
            return (short) value;
        }
        if (target.equals(Byte.TYPE) || target.equals(Byte.class)) {
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new DeserializationException("Value " + value + " out of range for byte");
            }
            return (byte) value;
        }
        if (target.equals(Double.TYPE) || target.equals(Double.class)) {
            return (double) value;
        }
        if (target.equals(Float.TYPE) || target.equals(Float.class)) {
            return (float) value;
        }
        if (target.equals(BigInteger.class)) {
            return BigInteger.valueOf(value);
        }
        // Fallback: return as Integer; caller may attempt to cast/assign
        return value;
    }

    private static Object coerceFromLong(long value, Class<?> target) {
        var created = tryCreateFromScalar(target, Long.valueOf(value));
        if (created != null) {
            return created;
        }
        if (target.equals(Object.class) || target.equals(Long.TYPE) || target.equals(Long.class)) {
            return value;
        }
        if (target.equals(Integer.TYPE) || target.equals(Integer.class)) {
            if (value < 0 || value > Integer.MAX_VALUE) {
                throw new DeserializationException("Value " + value + " out of range for int");
            }
            return (int) value;
        }
        if (target.equals(Short.TYPE) || target.equals(Short.class)) {
            if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                throw new DeserializationException("Value " + value + " out of range for short");
            }
            return (short) value;
        }
        if (target.equals(Byte.TYPE) || target.equals(Byte.class)) {
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new DeserializationException("Value " + value + " out of range for byte");
            }
            return (byte) value;
        }
        if (target.equals(Double.TYPE) || target.equals(Double.class)) {
            return (double) value;
        }
        if (target.equals(Float.TYPE) || target.equals(Float.class)) {
            return (float) value;
        }
        if (target.equals(BigInteger.class)) {
            return BigInteger.valueOf(value);
        }
        return value;
    }

    private static Object tryCreateFromScalar(Class<?> target, Object value) {
        if (target.equals(Object.class)) {
            return null;
        }
        if (value != null && target.isAssignableFrom(value.getClass())) {
            return null;
        }
        Method creator = findSingleArgCreator(target);
        if (creator == null) {
            return null;
        }
        var paramType = creator.getParameterTypes()[0];
        Object argument = value;
        if (value != null && !paramType.isAssignableFrom(value.getClass())) {
            // Minimal adaptation: allow converting to String for String parameters
            if (paramType.equals(String.class)) {
                argument = String.valueOf(value);
            } else {
                return null;
            }
        }
        try {
            return creator.invoke(null, argument);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DeserializationException("Error invoking @MaxMindDbCreator on "
                + target.getName() + ": " + e.getMessage(), e);
        }
    }

    private static Method findSingleArgCreator(Class<?> target) {
        for (var m : target.getDeclaredMethods()) {
            if (m.getAnnotation(MaxMindDbCreator.class) == null) {
                continue;
            }
            if (!java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            if (!java.lang.reflect.Modifier.isPublic(m.getModifiers())) {
                // To avoid module access issues, only consider public methods
                continue;
            }
            if (!target.isAssignableFrom(m.getReturnType())) {
                continue;
            }
            if (m.getParameterCount() != 1) {
                continue;
            }
            return m;
        }
        return null;
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
        Object[] parameterDefaults;
        if (cachedConstructor == null) {
            constructor = findConstructor(cls);

            parameterTypes = constructor.getParameterTypes();

            parameterGenericTypes = constructor.getGenericParameterTypes();

            parameterIndexes = new HashMap<>();
            parameterDefaults = new Object[constructor.getParameterCount()];
            var annotations = constructor.getParameterAnnotations();
            for (int i = 0; i < constructor.getParameterCount(); i++) {
                var ann = getParameterAnnotation(annotations[i]);
                var name = ann != null ? ann.name() : null;
                if (name == null) {
                    // Fallbacks: record component name, then Java parameter name
                    // (requires -parameters)
                    if (cls.isRecord()) {
                        name = cls.getRecordComponents()[i].getName();
                    } else {
                        var param = constructor.getParameters()[i];
                        if (param.isNamePresent()) {
                            name = param.getName();
                        } else {
                            throw new ParameterNotFoundException(
                                "Parameter name for index " + i + " on class " + cls.getName()
                                    + " is not available. Annotate with @MaxMindDbParameter "
                                    + "or compile with -parameters.");
                        }
                    }
                }
                // Prepare parsed defaults once and cache them
                if (ann != null && ann.useDefault()) {
                    parameterDefaults[i] = parseDefault(ann.defaultValue(), parameterTypes[i]);
                }
                parameterIndexes.put(name, i);
            }

            this.constructors.put(
                cls,
                new CachedConstructor<>(
                    constructor,
                    parameterTypes,
                    parameterGenericTypes,
                    parameterIndexes,
                    parameterDefaults
                )
            );
        } else {
            constructor = cachedConstructor.constructor();
            parameterTypes = cachedConstructor.parameterTypes();
            parameterGenericTypes = cachedConstructor.parameterGenericTypes();
            parameterIndexes = cachedConstructor.parameterIndexes();
            parameterDefaults = cachedConstructor.parameterDefaults();
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

        // Apply cached defaults for missing parameters, if any
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i] == null && parameterDefaults[i] != null) {
                parameters[i] = parameterDefaults[i];
            }
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
        // Prefer explicitly annotated constructor
        for (var constructor : constructors) {
            if (constructor.getAnnotation(MaxMindDbConstructor.class) == null) {
                continue;
            }
            @SuppressWarnings("unchecked")
            var selected = (Constructor<T>) constructor;
            return selected;
        }

        // Fallback for records: use canonical constructor
        if (cls.isRecord()) {
            try {
                var components = cls.getRecordComponents();
                var types = new Class<?>[components.length];
                for (int i = 0; i < components.length; i++) {
                    types[i] = components[i].getType();
                }
                var c = cls.getDeclaredConstructor(types);
                @SuppressWarnings("unchecked")
                var selected = (Constructor<T>) c;
                return selected;
            } catch (NoSuchMethodException e) {
                // ignore and continue to next fallback
            }
        }

        // Fallback for single-constructor classes
        if (constructors.length == 1) {
            var only = constructors[0];
            @SuppressWarnings("unchecked")
            var selected = (Constructor<T>) only;
            return selected;
        }

        throw new ConstructorNotFoundException(
            "No usable constructor on class " + cls.getName()
                + ". Annotate a constructor with MaxMindDbConstructor, "
                + "provide a record canonical constructor, or a single public constructor.");
    }

    private static MaxMindDbParameter getParameterAnnotation(Annotation[] annotations) {
        for (var annotation : annotations) {
            if (!annotation.annotationType().equals(MaxMindDbParameter.class)) {
                continue;
            }
            return (MaxMindDbParameter) annotation;
        }
        return null;
    }

    private static Object parseDefault(String value, Class<?> target) {
        try {
            if (target.equals(Boolean.TYPE) || target.equals(Boolean.class)) {
                return value.isEmpty() ? false : Boolean.parseBoolean(value);
            }
            if (target.equals(Byte.TYPE) || target.equals(Byte.class)) {
                var v = value.isEmpty() ? 0 : Integer.parseInt(value);
                if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                    throw new DeserializationException(
                        "Default value out of range for byte");
                }
                return (byte) v;
            }
            if (target.equals(Short.TYPE) || target.equals(Short.class)) {
                var v = value.isEmpty() ? 0 : Integer.parseInt(value);
                if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                    throw new DeserializationException(
                        "Default value out of range for short");
                }
                return (short) v;
            }
            if (target.equals(Integer.TYPE) || target.equals(Integer.class)) {
                return value.isEmpty() ? 0 : Integer.parseInt(value);
            }
            if (target.equals(Long.TYPE) || target.equals(Long.class)) {
                return value.isEmpty() ? 0L : Long.parseLong(value);
            }
            if (target.equals(Float.TYPE) || target.equals(Float.class)) {
                return value.isEmpty() ? 0.0f : Float.parseFloat(value);
            }
            if (target.equals(Double.TYPE) || target.equals(Double.class)) {
                return value.isEmpty() ? 0.0d : Double.parseDouble(value);
            }
            if (target.equals(String.class)) {
                return value;
            }
        } catch (NumberFormatException e) {
            throw new DeserializationException(
                "Invalid default '" + value + "' for type " + target.getSimpleName(), e);
        }
        throw new DeserializationException(
            "Defaults are only supported for primitives, boxed types, and String.");
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
