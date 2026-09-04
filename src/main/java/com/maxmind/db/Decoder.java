package com.maxmind.db;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.math.BigInteger;
import java.net.InetAddress;
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

    // Sentinel to cache "no creator method exists" to avoid repeated method scanning
    private static final CachedCreator NO_CREATOR = new CachedCreator(null, null);

    private final NodeCache cache;

    // Per-operation resource limits. The MaxMind DB specification recommends
    // depth and value limits, but permits equivalent reader-specific accounting.
    // This decoder charges each decode invocation, including a pointer and its
    // uncached target, so the value limit bounds actual decoder work rather than
    // the specification's example flat value count. Container depth, together
    // with rejecting illegal pointer-to-pointer values, bounds recursive calls.
    // The payload limit bounds encoded string and bytes data materialized by
    // this Java decoder.
    // The lower depth limit leaves room on a 512 KiB thread stack even for
    // pointer-backed maps, which use more Java frames per logical container
    // than inline values. A Decoder serves one decode operation on one thread,
    // so these fields need no synchronization.
    private static final int MAX_DEPTH = 128;
    private static final int MAX_VALUES = 1 << 16;
    private static final long MAX_PAYLOAD_BYTES = 1 << 21;

    // A collection's declared size is its logical child count, but it is not
    // proof that the input contains that many decodable children. When deriving
    // an initial capacity from it, limit unused capacity on the active recursion
    // path; completed children remain bounded by MAX_VALUES.
    private static final int MAX_INITIAL_COLLECTION_CAPACITY = 128;
    private int depth;
    private int valuesRemaining = MAX_VALUES;
    private long payloadRemaining = MAX_PAYLOAD_BYTES;

    private final long pointerBase;

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();

    private final Buffer buffer;

    private final ConcurrentHashMap<Class<?>, CachedConstructor<?>> constructors;

    private final ConcurrentHashMap<Class<?>, CachedCreator> creators;

    private final InetAddress lookupIp;
    private final Network lookupNetwork;

    Decoder(NodeCache cache, Buffer buffer, long pointerBase) {
        this(
            cache,
            buffer,
            pointerBase,
            new ConcurrentHashMap<>(),
            new ConcurrentHashMap<>(),
            null,
            null
        );
    }

    Decoder(
        NodeCache cache,
        Buffer buffer,
        long pointerBase,
        ConcurrentHashMap<Class<?>, CachedConstructor<?>> constructors
    ) {
        this(
            cache,
            buffer,
            pointerBase,
            constructors,
            new ConcurrentHashMap<>(),
            null,
            null
        );
    }

    Decoder(
        NodeCache cache,
        Buffer buffer,
        long pointerBase,
        ConcurrentHashMap<Class<?>, CachedConstructor<?>> constructors,
        ConcurrentHashMap<Class<?>, CachedCreator> creators,
        InetAddress lookupIp,
        Network lookupNetwork
    ) {
        this.cache = cache;
        this.pointerBase = pointerBase;
        this.buffer = buffer;
        this.constructors = constructors;
        this.creators = creators;
        this.lookupIp = lookupIp;
        this.lookupNetwork = lookupNetwork;
    }

    private final NodeCache.Loader cacheLoader = this::decode;

    <T> T decode(long offset, Class<T> cls) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "pointer larger than the database.");
        }

        this.valuesRemaining = MAX_VALUES;
        this.payloadRemaining = MAX_PAYLOAD_BYTES;
        this.depth = 0;
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
        // Validate a target when the cache loader decodes it. A target that was
        // loaded successfully has already passed this check, so cache hits do
        // not need to reread its control byte.
        if (Type.fromControlByte(0xFF & this.buffer.get(offset)) == Type.POINTER) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains a pointer to a pointer");
        }

        this.buffer.position(offset);
        Class<T> cls = key.cls();
        return decode(cls, key.type());
    }

    private <T> DecodedValue decode(Class<T> cls, java.lang.reflect.Type genericType)
        throws IOException {
        if (--this.valuesRemaining < 0) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section exceeds the maximum number of values");
        }
        var ctrlByte = 0xFF & this.buffer.get();

        var type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            var pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            var base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            var packed = Decoder.decodeLong(this.buffer, base, pointerSize);
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
        DecodedValue value;
        if (requiresLookupContext(cls)) {
            value = this.decode(key);
        } else {
            value = cache.get(key, cacheLoader);
        }

        buffer.position(position);
        return value;
    }

    private boolean requiresLookupContext(Class<?> cls) {
        if (cls == null
            || cls.equals(Object.class)
            || Map.class.isAssignableFrom(cls)
            || List.class.isAssignableFrom(cls)
            || cls.isEnum()
            || isSimpleType(cls)) {
            return false;
        }

        // Non-enum classes with @MaxMindDbCreator don't require lookup context
        // since they just convert simple values (strings, booleans, etc.)
        if (getCachedCreator(cls) != null) {
            return false;
        }

        var cached = getCachedConstructor(cls);
        if (cached == null) {
            cached = loadConstructorMetadata(cls);
        }
        return cached.requiresLookupContext();
    }

    private static boolean isSimpleType(Class<?> cls) {
        if (cls.isPrimitive() || cls.isArray()) {
            return true;
        }
        return cls.equals(String.class)
            || Number.class.isAssignableFrom(cls)
            || cls.equals(Boolean.class)
            || cls.equals(Character.class)
            || cls.equals(BigInteger.class);
    }

    // A container cannot hold more entries than there are bytes left to encode
    // them: every key, value, and element occupies at least one byte. Reject an
    // impossible declared size before it is used as an allocation hint, so a
    // tiny crafted database cannot force a huge list or map preallocation and
    // exhaust memory. valueCount is the number of encoded values the container
    // declares (an array of N declares N, a map of N declares 2N).
    private void checkContainerSize(long valueCount) throws InvalidDatabaseException {
        // A container cannot decode more values than the per-operation budget
        // allows, so reject an oversized declaration before allocating for it
        // rather than after the per-value limit stops the decode.
        if (valueCount > this.valuesRemaining) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section exceeds the maximum number of values");
        }
        if (valueCount > this.buffer.capacity() - this.buffer.position()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "a container declares more entries than the data section can hold");
        }
    }

    // Charge a string or bytes payload against the per-operation budget before
    // it is materialized. A payload amplification points many pointers at one large
    // value. A cache miss that re-decodes the target charges its payload again;
    // a cache hit reuses the completed value without materializing it again. The
    // comparison is against the remaining budget so it cannot overflow. The
    // limit is inclusive: a total exactly at the limit is allowed.
    private void chargePayload(long length) throws InvalidDatabaseException {
        if (length > this.payloadRemaining) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section exceeds the maximum payload size");
        }
        this.checkDataSize(length);
        this.payloadRemaining -= length;
    }

    private void checkDataSize(long length) throws InvalidDatabaseException {
        if (length > this.buffer.capacity() - this.buffer.position()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "a value extends beyond the end of the data section.");
        }
    }

    private static int initialMapCapacity(int size) {
        // HashMap's constructor argument is a table capacity rather than an
        // expected entry count. Account for its default 0.75 load factor when
        // that can be done without exceeding the allocation-hint limit.
        return Math.min(
            size + (size + 2) / 3,
            MAX_INITIAL_COLLECTION_CAPACITY
        );
    }

    private <T> Object decodeByType(
        Type type,
        int size,
        Class<T> cls,
        java.lang.reflect.Type genericType
    ) throws IOException {
        switch (type) {
            case MAP: {
                if (++this.depth > MAX_DEPTH) {
                    throw new InvalidDatabaseException(
                        "The MaxMind DB file's data section exceeds the maximum depth");
                }
                this.checkContainerSize((long) size * 2);
                var map = this.decodeMap(size, cls, genericType);
                this.depth--;
                return map;
            }
            case ARRAY:
                Class<?> elementClass = Object.class;
                if (genericType instanceof ParameterizedType ptype) {
                    var actualTypes = ptype.getActualTypeArguments();
                    if (actualTypes.length == 1) {
                        elementClass = (Class<?>) actualTypes[0];
                    }
                }
                if (++this.depth > MAX_DEPTH) {
                    throw new InvalidDatabaseException(
                        "The MaxMind DB file's data section exceeds the maximum depth");
                }
                this.checkContainerSize(size);
                var array = this.decodeArray(size, cls, elementClass);
                this.depth--;
                return array;
            case BOOLEAN:
                Boolean bool = Decoder.decodeBoolean(size);
                return convertValue(bool, cls);
            case UTF8_STRING:
                String str = this.decodeString(size);
                return convertValue(str, cls);
            case DOUBLE:
                return this.decodeDouble(size);
            case FLOAT:
                return this.decodeFloat(size);
            case BYTES:
                return this.getByteArray(size);
            case UINT16:
                this.checkIntegerSize("uint16", size, 2);
                return coerceFromInt(this.decodeUint16(size), cls);
            case UINT32:
                this.checkIntegerSize("uint32", size, 4);
                return coerceFromLong(this.decodeUint32(size), cls);
            case INT32:
                this.checkIntegerSize("int32", size, 4);
                return coerceFromInt(this.decodeInt32(size), cls);
            case UINT64:
                this.checkIntegerSize("uint64", size, 8);
                return this.decodeLargeUint(size, cls);
            case UINT128:
                this.checkIntegerSize("uint128", size, 16);
                return this.decodeLargeUint(size, cls);
            default:
                throw new InvalidDatabaseException(
                    "Unknown or unexpected type: " + type.name());
        }
    }

    private Object decodeLargeUint(int size, Class<?> cls)
        throws InvalidDatabaseException {
        // For typed fields, avoid BigInteger allocation when the value fits in
        // long. Keep Object.class behavior unchanged for backward compatibility.
        if (size < 8 && !cls.equals(Object.class)) {
            return coerceFromLong(this.decodeLong(size), cls);
        }
        return coerceFromBigInteger(this.decodeBigInteger(size), cls);
    }

    private void checkIntegerSize(String type, int size, int maximum)
        throws InvalidDatabaseException {
        if (size > maximum) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains bad data: "
                    + "invalid size of " + type + ".");
        }
        this.checkDataSize(size);
    }

    private static Object coerceFromInt(int value, Class<?> target) {
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
        if (target.equals(Object.class) || target.equals(Long.TYPE) || target.equals(Long.class)) {
            return value;
        }
        if (target.equals(Integer.TYPE) || target.equals(Integer.class)) {
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
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

    private static Object coerceFromBigInteger(BigInteger value, Class<?> target) {
        if (target.equals(Object.class) || target.equals(BigInteger.class)) {
            return value;
        }
        if (target.equals(Long.TYPE) || target.equals(Long.class)) {
            if (value.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                || value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                throw new DeserializationException("Value " + value + " out of range for long");
            }
            return value.longValue();
        }
        if (target.equals(Integer.TYPE) || target.equals(Integer.class)) {
            if (value.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0
                || value.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
                throw new DeserializationException("Value " + value + " out of range for int");
            }
            return value.intValue();
        }
        if (target.equals(Short.TYPE) || target.equals(Short.class)) {
            if (value.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0
                || value.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
                throw new DeserializationException("Value " + value + " out of range for short");
            }
            return value.shortValue();
        }
        if (target.equals(Byte.TYPE) || target.equals(Byte.class)) {
            if (value.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0
                || value.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
                throw new DeserializationException("Value " + value + " out of range for byte");
            }
            return value.byteValue();
        }
        if (target.equals(Double.TYPE) || target.equals(Double.class)) {
            return value.doubleValue();
        }
        if (target.equals(Float.TYPE) || target.equals(Float.class)) {
            return value.floatValue();
        }
        return value;
    }

    private String decodeString(long size) throws IOException {
        this.chargePayload(size);
        var oldLimit = buffer.limit();
        try {
            buffer.limit(buffer.position() + size);
            return buffer.decode(utfDecoder);
        } catch (CharacterCodingException e) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's data section contains an invalid UTF-8 string", e);
        } finally {
            buffer.limit(oldLimit);
        }
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

    private BigInteger decodeBigInteger(int size) throws InvalidDatabaseException {
        var bytes = Decoder.getByteArray(this.buffer, size);
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
        var initialCapacity = Math.min(size, MAX_INITIAL_COLLECTION_CAPACITY);
        if (cls.equals(List.class) || cls.equals(Object.class)) {
            array = new ArrayList<>(initialCapacity);
        } else {
            Constructor<T> constructor;
            try {
                constructor = cls.getConstructor(Integer.TYPE);
            } catch (NoSuchMethodException e) {
                throw new DeserializationException(
                    "No constructor found for the List: " + e.getMessage(), e);
            }
            var parameters = new Object[]{initialCapacity};
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
            map = new HashMap<>(initialMapCapacity(size));
        } else {
            var initialCapacity = Math.min(size, MAX_INITIAL_COLLECTION_CAPACITY);
            Constructor<T> constructor;
            try {
                constructor = cls.getConstructor(Integer.TYPE);
            } catch (NoSuchMethodException e) {
                throw new DeserializationException(
                    "No constructor found for the Map: " + e.getMessage(), e);
            }
            var parameters = new Object[]{initialCapacity};
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

    private <T> CachedConstructor<T> loadConstructorMetadata(Class<T> cls) {
        var cached = getCachedConstructor(cls);
        if (cached != null) {
            return cached;
        }

        var constructor = findConstructor(cls);

        var parameterTypes = constructor.getParameterTypes();
        var parameterGenericTypes = constructor.getGenericParameterTypes();
        var parameterIndexes = new HashMap<String, Integer>();
        var parameterDefaults = new Object[constructor.getParameterCount()];
        var parameterInjections = new ParameterInjection[constructor.getParameterCount()];
        boolean requiresContext = false;

        var annotations = constructor.getParameterAnnotations();
        for (int i = 0; i < constructor.getParameterCount(); i++) {
            var injection = getParameterInjection(annotations[i]);
            parameterInjections[i] = injection;

            var parameterAnnotation = getParameterAnnotation(annotations[i]);

            if (injection != ParameterInjection.NONE) {
                requiresContext = true;
                if (parameterAnnotation != null) {
                    throw new DeserializationException(
                        "Parameter index " + i + " on class " + cls.getName()
                            + " cannot have both @MaxMindDbParameter and a lookup context "
                            + "annotation.");
                }
                validateInjectionTarget(cls, i, parameterTypes[i], injection);
                continue;
            }

            if (parameterAnnotation != null && parameterAnnotation.useDefault()) {
                parameterDefaults[i] =
                    parseDefault(parameterAnnotation.defaultValue(), parameterTypes[i]);
            }

            String name = parameterAnnotation != null ? parameterAnnotation.name() : null;
            if (name == null) {
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
            parameterIndexes.put(name, i);
        }

        // Check for transitive context requirements: if any non-injection parameter type
        // itself requires context (e.g., nested objects with @MaxMindDbIpAddress annotations),
        // then this parent class also requires context to avoid incorrect caching.
        if (!requiresContext) {
            for (int i = 0; i < parameterTypes.length; i++) {
                if (parameterInjections[i] == ParameterInjection.NONE) {
                    if (shouldInstantiateFromContext(parameterTypes[i])) {
                        requiresContext = true;
                        break;
                    }
                }
            }
        }

        var cachedConstructor = new CachedConstructor<>(
            constructor,
            parameterTypes,
            parameterGenericTypes,
            parameterIndexes,
            parameterDefaults,
            parameterInjections,
            requiresContext
        );
        @SuppressWarnings("unchecked")
        var existing = (CachedConstructor<T>) this.constructors.putIfAbsent(cls, cachedConstructor);
        return existing != null ? existing : cachedConstructor;
    }

    private <T> Object decodeMapIntoObject(int size, Class<T> cls)
        throws IOException {
        var cachedConstructor = loadConstructorMetadata(cls);

        var constructor = cachedConstructor.constructor();
        var parameterTypes = cachedConstructor.parameterTypes();
        var parameterGenericTypes = cachedConstructor.parameterGenericTypes();
        var parameterIndexes = cachedConstructor.parameterIndexes();
        var parameterDefaults = cachedConstructor.parameterDefaults();
        var parameterInjections = cachedConstructor.parameterInjections();

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

        for (int i = 0; i < parameters.length; i++) {
            if (parameterInjections[i] != ParameterInjection.NONE) {
                parameters[i] = injectParameter(parameterInjections[i], parameterTypes[i]);
                continue;
            }
            if (parameters[i] != null) {
                continue;
            }
            if (parameterDefaults[i] != null) {
                parameters[i] = parameterDefaults[i];
                continue;
            }
            if (shouldInstantiateFromContext(parameterTypes[i])) {
                parameters[i] = instantiateWithLookupContext(parameterTypes[i]);
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
                if (parameters[index] == null && parameterTypes[index].isPrimitive()) {
                    sbErrors.append(" null value for primitive ")
                        .append(parameterTypes[index].getName())
                        .append(" parameter '").append(key).append("'.");
                } else if (parameters[index] != null
                    && !isAssignableType(parameters[index].getClass(), parameterTypes[index])) {
                    sbErrors.append(" argument type mismatch in ").append(key)
                        .append(" MMDB Type: ")
                        .append(parameters[index].getClass().getCanonicalName())
                        .append(" Java Type: ")
                        .append(parameterTypes[index].getCanonicalName())
                        .append(".");
                }
            }
            throw new DeserializationException(
                "Error creating object of type: " + cls.getSimpleName() + " -" + sbErrors, e);
        }
    }

    /**
     * Checks if an actual type can be assigned to an expected type,
     * accounting for primitive/boxed equivalents (e.g., Boolean to boolean).
     */
    private static boolean isAssignableType(Class<?> actual, Class<?> expected) {
        if (expected.isAssignableFrom(actual)) {
            return true;
        }
        // Check primitive/boxed equivalents for auto-unboxing
        if (expected.isPrimitive()) {
            return actual.equals(boxedType(expected));
        }
        return false;
    }

    private static Class<?> boxedType(Class<?> primitive) {
        if (primitive == boolean.class) {
            return Boolean.class;
        }
        if (primitive == byte.class) {
            return Byte.class;
        }
        if (primitive == short.class) {
            return Short.class;
        }
        if (primitive == int.class) {
            return Integer.class;
        }
        if (primitive == long.class) {
            return Long.class;
        }
        if (primitive == float.class) {
            return Float.class;
        }
        if (primitive == double.class) {
            return Double.class;
        }
        if (primitive == char.class) {
            return Character.class;
        }
        return primitive;
    }

    private boolean shouldInstantiateFromContext(Class<?> parameterType) {
        if (parameterType == null
            || parameterType.isPrimitive()
            || parameterType.isEnum()
            || isSimpleType(parameterType)
            || Map.class.isAssignableFrom(parameterType)
            || List.class.isAssignableFrom(parameterType)) {
            return false;
        }
        return requiresLookupContext(parameterType);
    }

    private Object instantiateWithLookupContext(Class<?> parameterType) {
        var metadata = loadConstructorMetadata(parameterType);
        if (metadata == null || !metadata.requiresLookupContext()) {
            return null;
        }

        var ctor = metadata.constructor();
        var types = metadata.parameterTypes();
        var defaults = metadata.parameterDefaults();
        var injections = metadata.parameterInjections();
        var args = new Object[types.length];

        for (int i = 0; i < args.length; i++) {
            if (injections[i] != ParameterInjection.NONE) {
                args[i] = injectParameter(injections[i], types[i]);
            } else if (defaults[i] != null) {
                args[i] = defaults[i];
            } else if (types[i].isPrimitive()) {
                args[i] = primitiveDefault(types[i]);
            } else {
                args[i] = null;
            }
        }

        try {
            return ctor.newInstance(args);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new DeserializationException(
                "Error creating object of type: " + parameterType.getName(), e);
        }
    }

    private static Object primitiveDefault(Class<?> type) {
        if (type.equals(Boolean.TYPE)) {
            return false;
        }
        if (type.equals(Byte.TYPE)) {
            return (byte) 0;
        }
        if (type.equals(Short.TYPE)) {
            return (short) 0;
        }
        if (type.equals(Integer.TYPE)) {
            return 0;
        }
        if (type.equals(Long.TYPE)) {
            return 0L;
        }
        if (type.equals(Float.TYPE)) {
            return 0.0f;
        }
        if (type.equals(Double.TYPE)) {
            return 0.0d;
        }
        if (type.equals(Character.TYPE)) {
            return '\0';
        }
        return null;
    }

    private Object injectParameter(ParameterInjection injection, Class<?> parameterType) {
        return switch (injection) {
            case IP_ADDRESS -> getLookupIpValue(parameterType);
            case NETWORK -> getLookupNetworkValue(parameterType);
            case NONE -> null;
        };
    }

    private Object getLookupIpValue(Class<?> parameterType) {
        if (this.lookupIp == null) {
            throw new DeserializationException(
                "Cannot inject lookup IP address because no lookup context is available.");
        }
        if (String.class.equals(parameterType)) {
            return this.lookupIp.getHostAddress();
        }
        if (InetAddress.class.isAssignableFrom(parameterType)) {
            return this.lookupIp;
        }
        throw new DeserializationException(
            "Unsupported parameter type " + parameterType.getName()
                + " for @MaxMindDbIpAddress; expected java.net.InetAddress or "
                + "java.lang.String.");
    }

    private Object getLookupNetworkValue(Class<?> parameterType) {
        if (this.lookupNetwork == null) {
            throw new DeserializationException(
                "Cannot inject lookup network because no lookup context is available.");
        }
        if (String.class.equals(parameterType)) {
            return this.lookupNetwork.toString();
        }
        if (Network.class.isAssignableFrom(parameterType)) {
            return this.lookupNetwork;
        }
        throw new DeserializationException(
            "Unsupported parameter type " + parameterType.getName()
                + " for @MaxMindDbNetwork; expected com.maxmind.db.Network or "
                + "java.lang.String.");
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

    private static ParameterInjection getParameterInjection(Annotation[] annotations) {
        ParameterInjection injection = ParameterInjection.NONE;
        for (var annotation : annotations) {
            var type = annotation.annotationType();
            if (type.equals(MaxMindDbIpAddress.class)) {
                if (injection != ParameterInjection.NONE) {
                    throw new DeserializationException(
                        "Constructor parameters may have at most one lookup context annotation.");
                }
                injection = ParameterInjection.IP_ADDRESS;
            } else if (type.equals(MaxMindDbNetwork.class)) {
                if (injection != ParameterInjection.NONE) {
                    throw new DeserializationException(
                        "Constructor parameters may have at most one lookup context annotation.");
                }
                injection = ParameterInjection.NETWORK;
            }
        }
        return injection;
    }

    private static void validateInjectionTarget(
        Class<?> cls,
        int parameterIndex,
        Class<?> parameterType,
        ParameterInjection injection
    ) {
        if (injection == ParameterInjection.IP_ADDRESS) {
            if (!InetAddress.class.isAssignableFrom(parameterType)
                && !String.class.equals(parameterType)) {
                throw new DeserializationException(
                    "Parameter index " + parameterIndex + " on class " + cls.getName()
                        + " annotated with @MaxMindDbIpAddress must be of type "
                        + "java.net.InetAddress or java.lang.String.");
            }
        } else if (injection == ParameterInjection.NETWORK) {
            if (!Network.class.isAssignableFrom(parameterType)
                && !String.class.equals(parameterType)) {
                throw new DeserializationException(
                    "Parameter index " + parameterIndex + " on class " + cls.getName()
                        + " annotated with @MaxMindDbNetwork must be of type "
                        + "com.maxmind.db.Network or java.lang.String.");
            }
        }
    }

    /**
     * Converts a decoded value to the target type using a creator method if available.
     * If no creator method is found, returns the original value.
     */
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null || targetType == null
            || targetType == Object.class
            || targetType.isInstance(value)) {
            return value;
        }

        CachedCreator creator = getCachedCreator(targetType);
        if (creator == null) {
            return value;
        }

        if (!creator.parameterType().isInstance(value)) {
            return value;
        }

        try {
            return creator.method().invoke(null, value);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DeserializationException(
                "Error invoking creator method " + creator.method().getName()
                    + " on class " + targetType.getName(), e);
        }
    }

    private CachedCreator getCachedCreator(Class<?> cls) {
        CachedCreator cached = this.creators.get(cls);
        if (cached == NO_CREATOR) {
            return null;  // Known to have no creator
        }
        if (cached != null) {
            return cached;
        }

        CachedCreator creator = findCreatorMethod(cls);
        this.creators.putIfAbsent(cls, creator != null ? creator : NO_CREATOR);
        return creator;
    }

    private static CachedCreator findCreatorMethod(Class<?> cls) {
        Method[] methods = cls.getDeclaredMethods();
        for (Method method : methods) {
            if (!method.isAnnotationPresent(MaxMindDbCreator.class)) {
                continue;
            }
            if (!Modifier.isStatic(method.getModifiers())) {
                throw new DeserializationException(
                    "Creator method " + method.getName() + " on class " + cls.getName()
                        + " must be static.");
            }
            if (method.getParameterCount() != 1) {
                throw new DeserializationException(
                    "Creator method " + method.getName() + " on class " + cls.getName()
                        + " must have exactly one parameter.");
            }
            if (!cls.isAssignableFrom(method.getReturnType())) {
                throw new DeserializationException(
                    "Creator method " + method.getName() + " on class " + cls.getName()
                        + " must return " + cls.getName() + " or a subtype.");
            }
            return new CachedCreator(method, method.getParameterTypes()[0]);
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
        // Iterate over siblings so a large flat unknown value cannot exhaust
        // the Java stack. Recursion is only used to track structural nesting,
        // which is bounded by the same limit as normal decoding.
        for (var i = 0; i < numberToSkip; i++) {
            if (--this.valuesRemaining < 0) {
                throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section exceeds the maximum number of values");
            }

            var ctrlData = this.getCtrlData(offset);
            var ctrlByte = ctrlData.ctrlByte();
            var size = ctrlData.size();
            offset = ctrlData.offset();

            switch (ctrlData.type()) {
                case POINTER:
                    var pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
                    offset += pointerSize;
                    break;
                case MAP:
                    this.checkContainerSize((long) size * 2);
                    if (++this.depth > MAX_DEPTH) {
                        throw new InvalidDatabaseException(
                            "The MaxMind DB file's data section exceeds the maximum depth");
                    }
                    try {
                        offset = this.nextValueOffset(offset, 2 * size);
                    } finally {
                        this.depth--;
                    }
                    break;
                case ARRAY:
                    this.checkContainerSize(size);
                    if (++this.depth > MAX_DEPTH) {
                        throw new InvalidDatabaseException(
                            "The MaxMind DB file's data section exceeds the maximum depth");
                    }
                    try {
                        offset = this.nextValueOffset(offset, size);
                    } finally {
                        this.depth--;
                    }
                    break;
                case BOOLEAN:
                    break;
                case UINT16:
                    this.checkIntegerSize("uint16", size, 2);
                    offset += size;
                    break;
                case UINT32:
                    this.checkIntegerSize("uint32", size, 4);
                    offset += size;
                    break;
                case INT32:
                    this.checkIntegerSize("int32", size, 4);
                    offset += size;
                    break;
                case UINT64:
                    this.checkIntegerSize("uint64", size, 8);
                    offset += size;
                    break;
                case UINT128:
                    this.checkIntegerSize("uint128", size, 16);
                    offset += size;
                    break;
                default:
                    offset += size;
                    break;
            }
        }
        return offset;
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

    private byte[] getByteArray(int length) throws InvalidDatabaseException {
        this.chargePayload(length);
        return Decoder.getByteArray(this.buffer, length);
    }

    private static byte[] getByteArray(Buffer buffer, int length) {
        var bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}
