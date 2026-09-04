package com.maxmind.db;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"boxing", "static-method"})
public class DecoderTest {

    private static final int TEST_MAX_DEPTH = 128;

    private static Map<Integer, byte[]> int32() {
        int max = (2 << 30) - 1;
        var int32 = new HashMap<Integer, byte[]>();

        int32.put(0, new byte[] {0x0, 0x1});
        int32.put(-1, new byte[] {0x4, 0x1, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff});
        int32.put((2 << 7) - 1, new byte[] {0x1, 0x1, (byte) 0xff});
        int32.put(1 - (2 << 7), new byte[] {0x4, 0x1, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, 0x1});
        int32.put(500, new byte[] {0x2, 0x1, 0x1, (byte) 0xf4});

        int32.put(-500, new byte[] {0x4, 0x1, (byte) 0xff, (byte) 0xff,
            (byte) 0xfe, 0xc});

        int32.put((2 << 15) - 1, new byte[] {0x2, 0x1, (byte) 0xff,
            (byte) 0xff});
        int32.put(1 - (2 << 15), new byte[] {0x4, 0x1, (byte) 0xff,
            (byte) 0xff, 0x0, 0x1});
        int32.put((2 << 23) - 1, new byte[] {0x3, 0x1, (byte) 0xff,
            (byte) 0xff, (byte) 0xff});
        int32.put(1 - (2 << 23), new byte[] {0x4, 0x1, (byte) 0xff, 0x0, 0x0,
            0x1});
        int32.put(max, new byte[] {0x4, 0x1, 0x7f, (byte) 0xff, (byte) 0xff,
            (byte) 0xff});
        int32.put(-max, new byte[] {0x4, 0x1, (byte) 0x80, 0x0, 0x0, 0x1});
        return int32;
    }

    private static Map<Long, byte[]> uint32() {
        long max = (((long) 1) << 32) - 1;
        var uint32s = new HashMap<Long, byte[]>();

        uint32s.put((long) 0, new byte[] {(byte) 0xc0});
        uint32s.put((long) ((1 << 8) - 1), new byte[] {(byte) 0xc1,
            (byte) 0xff});
        uint32s.put((long) 500, new byte[] {(byte) 0xc2, 0x1, (byte) 0xf4});
        uint32s.put((long) 10872, new byte[] {(byte) 0xc2, 0x2a, 0x78});
        uint32s.put((long) ((1 << 16) - 1), new byte[] {(byte) 0xc2,
            (byte) 0xff, (byte) 0xff});
        uint32s.put((long) ((1 << 24) - 1), new byte[] {(byte) 0xc3,
            (byte) 0xff, (byte) 0xff, (byte) 0xff});
        uint32s.put(max, new byte[] {(byte) 0xc4, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff});

        return uint32s;
    }

    private static Map<Integer, byte[]> uint16() {
        int max = (1 << 16) - 1;

        var uint16s = new HashMap<Integer, byte[]>();

        uint16s.put(0, new byte[] {(byte) 0xa0});
        uint16s.put((1 << 8) - 1, new byte[] {(byte) 0xa1, (byte) 0xff});
        uint16s.put(500, new byte[] {(byte) 0xa2, 0x1, (byte) 0xf4});
        uint16s.put(10872, new byte[] {(byte) 0xa2, 0x2a, 0x78});
        uint16s.put(max, new byte[] {(byte) 0xa2, (byte) 0xff, (byte) 0xff});
        return uint16s;
    }

    private static Map<BigInteger, byte[]> largeUint(int bits) {
        var uints = new HashMap<BigInteger, byte[]>();

        byte ctrlByte = (byte) (bits == 64 ? 0x2 : 0x3);

        uints.put(BigInteger.valueOf(0), new byte[] {0x0, ctrlByte});
        uints.put(BigInteger.valueOf(500), new byte[] {0x2, ctrlByte, 0x1,
            (byte) 0xf4});
        uints.put(BigInteger.valueOf(10872), new byte[] {0x2, ctrlByte, 0x2a,
            0x78});

        for (int power = 1; power <= bits / 8; power++) {

            var key = BigInteger.valueOf(2).pow(8 * power)
                .subtract(BigInteger.valueOf(1));

            var value = new byte[2 + power];
            value[0] = (byte) power;
            value[1] = ctrlByte;
            for (int i = 2; i < value.length; i++) {
                value[i] = (byte) 0xff;
            }
            uints.put(key, value);
        }
        return uints;

    }

    private static Map<Long, byte[]> pointers() {
        var pointers = new HashMap<Long, byte[]>();

        pointers.put((long) 0, new byte[] {0x20, 0x0});
        pointers.put((long) 5, new byte[] {0x20, 0x5});
        pointers.put((long) 10, new byte[] {0x20, 0xa});
        pointers.put((long) ((1 << 10) - 1), new byte[] {0x23, (byte) 0xff,});
        pointers.put((long) 3017, new byte[] {0x28, 0x3, (byte) 0xc9});
        pointers.put((long) ((1 << 19) - 5), new byte[] {0x2f, (byte) 0xf7,
            (byte) 0xfb});
        pointers.put((long) ((1 << 19) + (1 << 11) - 1), new byte[] {0x2f,
            (byte) 0xff, (byte) 0xff});
        pointers.put((long) ((1 << 27) - 2), new byte[] {0x37, (byte) 0xf7,
            (byte) 0xf7, (byte) 0xfe});
        pointers.put((((long) 1) << 27) + (1 << 19) + (1 << 11) - 1,
            new byte[] {0x37, (byte) 0xff, (byte) 0xff, (byte) 0xff});

        pointers.put((((long) 1) << 31) - 1, new byte[] {0x38, (byte) 0x7f,
            (byte) 0xff, (byte) 0xff, (byte) 0xff});

        return pointers;
    }

    private static Map<String, byte[]> strings() {
        var strings = new HashMap<String, byte[]>();

        DecoderTest.addTestString(strings, (byte) 0x40, "");
        DecoderTest.addTestString(strings, (byte) 0x41, "1");
        DecoderTest.addTestString(strings, (byte) 0x43, "人");
        DecoderTest.addTestString(strings, (byte) 0x43, "123");
        DecoderTest.addTestString(strings, (byte) 0x5b,
            "123456789012345678901234567");
        DecoderTest.addTestString(strings, (byte) 0x5c,
            "1234567890123456789012345678");
        DecoderTest.addTestString(strings, (byte) 0x5c,
            "1234567890123456789012345678");
        DecoderTest.addTestString(strings, new byte[] {0x5d, 0x0},
            "12345678901234567890123456789");
        DecoderTest.addTestString(strings, new byte[] {0x5d, (byte) 128},
            "x".repeat(157));

        DecoderTest
            .addTestString(strings, new byte[] {0x5d, 0x0, (byte) 0xd7},
                "x".repeat(500));

        DecoderTest
            .addTestString(strings, new byte[] {0x5e, 0x0, (byte) 0xd7},
                "x".repeat(500));
        DecoderTest.addTestString(strings,
            new byte[] {0x5e, 0x6, (byte) 0xb3},
            "x".repeat(2000));
        DecoderTest.addTestString(strings,
            new byte[] {0x5f, 0x0, 0x10, 0x53,},
            "x".repeat(70000));

        return strings;

    }

    private static Map<byte[], byte[]> bytes() {
        var bytes = new HashMap<byte[], byte[]>();

        var strings = DecoderTest.strings();

        for (String s : strings.keySet()) {
            var ba = strings.get(s);
            ba[0] ^= 0xc0;

            bytes.put(s.getBytes(StandardCharsets.UTF_8), ba);
        }

        return bytes;
    }

    private static void addTestString(Map<String, byte[]> tests, byte ctrl,
                                      String str) {
        DecoderTest.addTestString(tests, new byte[] {ctrl}, str);
    }

    private static void addTestString(Map<String, byte[]> tests, byte[] ctrl,
                                      String str) {

        var sb = str.getBytes(StandardCharsets.UTF_8);
        var bytes = new byte[ctrl.length + sb.length];

        System.arraycopy(ctrl, 0, bytes, 0, ctrl.length);
        System.arraycopy(sb, 0, bytes, ctrl.length, sb.length);
        tests.put(str, bytes);
    }

    private static Map<Double, byte[]> doubles() {
        var doubles = new HashMap<Double, byte[]>();
        doubles.put(0.0, new byte[] {0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
            0x0});
        doubles.put(0.5, new byte[] {0x68, 0x3F, (byte) 0xE0, 0x0, 0x0, 0x0,
            0x0, 0x0, 0x0});
        doubles.put(3.14159265359, new byte[] {0x68, 0x40, 0x9, 0x21,
            (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA});
        doubles.put(123.0, new byte[] {0x68, 0x40, 0x5E, (byte) 0xC0, 0x0,
            0x0, 0x0, 0x0, 0x0});
        doubles.put(1073741824.12457, new byte[] {0x68, 0x41, (byte) 0xD0,
            0x0, 0x0, 0x0, 0x7, (byte) 0xF8, (byte) 0xF4});
        doubles.put(-0.5, new byte[] {0x68, (byte) 0xBF, (byte) 0xE0, 0x0,
            0x0, 0x0, 0x0, 0x0, 0x0});
        doubles.put(-3.14159265359, new byte[] {0x68, (byte) 0xC0, 0x9, 0x21,
            (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA});
        doubles.put(-1073741824.12457, new byte[] {0x68, (byte) 0xC1,
            (byte) 0xD0, 0x0, 0x0, 0x0, 0x7, (byte) 0xF8, (byte) 0xF4});

        return doubles;
    }

    private static Map<Float, byte[]> floats() {
        var floats = new HashMap<Float, byte[]>();
        floats.put((float) 0.0, new byte[] {0x4, 0x8, 0x0, 0x0, 0x0, 0x0});
        floats.put((float) 1.0, new byte[] {0x4, 0x8, 0x3F, (byte) 0x80, 0x0,
            0x0});
        floats.put((float) 1.1, new byte[] {0x4, 0x8, 0x3F, (byte) 0x8C,
            (byte) 0xCC, (byte) 0xCD});
        floats.put((float) 3.14, new byte[] {0x4, 0x8, 0x40, 0x48,
            (byte) 0xF5, (byte) 0xC3});
        floats.put((float) 9999.99, new byte[] {0x4, 0x8, 0x46, 0x1C, 0x3F,
            (byte) 0xF6});
        floats.put((float) -1.0, new byte[] {0x4, 0x8, (byte) 0xBF,
            (byte) 0x80, 0x0, 0x0});
        floats.put((float) -1.1, new byte[] {0x4, 0x8, (byte) 0xBF,
            (byte) 0x8C, (byte) 0xCC, (byte) 0xCD});
        floats.put((float) -3.14, new byte[] {0x4, 0x8, (byte) 0xC0, 0x48,
            (byte) 0xF5, (byte) 0xC3});
        floats.put((float) -9999.99, new byte[] {0x4, 0x8, (byte) 0xC6, 0x1C,
            0x3F, (byte) 0xF6});

        return floats;
    }

    private static Map<Boolean, byte[]> booleans() {
        var booleans = new HashMap<Boolean, byte[]>();

        booleans.put(Boolean.FALSE, new byte[] {0x0, 0x7});
        booleans.put(Boolean.TRUE, new byte[] {0x1, 0x7});
        return booleans;
    }

    private static Map<Map<String, ?>, byte[]> maps() {
        var maps = new HashMap<Map<String, ?>, byte[]>();

        var empty = Map.<String, Object>of();
        maps.put(empty, new byte[] {(byte) 0xe0});

        var one = new HashMap<String, String>();
        one.put("en", "Foo");
        maps.put(one, new byte[] {(byte) 0xe1, /* en */0x42, 0x65, 0x6e,
            /* Foo */0x43, 0x46, 0x6f, 0x6f});

        var two = new HashMap<String, String>();
        two.put("en", "Foo");
        two.put("zh", "人");
        maps.put(two, new byte[] {(byte) 0xe2,
            /* en */
            0x42, 0x65, 0x6e,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* zh */
            0x42, 0x7a, 0x68,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        var nested = new HashMap<String, Map<String, String>>();
        nested.put("name", two);

        maps.put(nested, new byte[] {(byte) 0xe1, /* name */
            0x44, 0x6e, 0x61, 0x6d, 0x65, (byte) 0xe2, /* en */
            0x42, 0x65, 0x6e,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* zh */
            0x42, 0x7a, 0x68,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        var guess = new HashMap<String, List<Object>>();
        var languages = new ArrayList<Object>();
        languages.add("en");
        languages.add("zh");
        guess.put("languages", languages);
        maps.put(guess, new byte[] {(byte) 0xe1, /* languages */
            0x49, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x73,
            /* array */
            0x2, 0x4,
            /* en */
            0x42, 0x65, 0x6e,
            /* zh */
            0x42, 0x7a, 0x68});

        return maps;
    }

    private static Map<List<String>, byte[]> arrays() {
        var arrays = new HashMap<List<String>, byte[]>();

        var f1 = new ArrayList<String>();
        f1.add("Foo");
        arrays.put(f1, new byte[] {0x1, 0x4,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f});

        var f2 = new ArrayList<String>();
        f2.add("Foo");
        f2.add("人");
        arrays.put(f2, new byte[] {0x2, 0x4,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        var empty = new ArrayList<String>();
        arrays.put(empty, new byte[] {0x0, 0x4});

        return arrays;
    }

    @Test
    public void testUint16() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT16, uint16());
    }

    @Test
    public void testUint32() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT32, uint32());
    }

    @Test
    public void testInt32() throws IOException {
        DecoderTest.testTypeDecoding(Type.INT32, int32());
    }

    @Test
    public void testUint64() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT64, largeUint(64));
    }

    @Test
    public void testUint128() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT128, largeUint(128));
    }

    @Test
    public void testDoubles() throws IOException {
        DecoderTest
            .testTypeDecoding(Type.DOUBLE, DecoderTest.doubles());
    }

    @Test
    public void testFloats() throws IOException {
        DecoderTest.testTypeDecoding(Type.FLOAT, DecoderTest.floats());
    }

    @Test
    public void testPointers() throws IOException {
        DecoderTest.testTypeDecoding(Type.POINTER, pointers());
    }

    @Test
    public void testStrings() throws IOException {
        DecoderTest.testTypeDecoding(Type.UTF8_STRING,
            DecoderTest.strings());
    }

    @Test
    public void testBooleans() throws IOException {
        DecoderTest.testTypeDecoding(Type.BOOLEAN,
            DecoderTest.booleans());
    }

    @Test
    public void testBytes() throws IOException {
        DecoderTest.testTypeDecoding(Type.BYTES, DecoderTest.bytes());
    }

    @Test
    public void testMaps() throws IOException {
        DecoderTest.testTypeDecoding(Type.MAP, DecoderTest.maps());
    }

    @Test
    public void testArrays() throws IOException {
        DecoderTest.testTypeDecoding(Type.ARRAY, DecoderTest.arrays());
    }

    @Test
    public void testInvalidControlByte() {
        var buffer = SingleBuffer.wrap(new byte[] {0x0, 0xF});

        var decoder = new Decoder(new CHMCache(), buffer, 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, String.class));
        assertThat(ex.getMessage(),
                containsString("The MaxMind DB file's data section contains bad data"));
    }

    private static void writePointer1(ByteArrayOutputStream out, int target) {
        // One-byte-payload pointer (type 1, pointer_size 1) with base 0.
        out.write((1 << 5) | ((target >> 8) & 0x7));
        out.write(target & 0xFF);
    }

    private static void writePointer(ByteArrayOutputStream out, int target) {
        if (target < 1 << 11) {
            writePointer1(out, target);
            return;
        }

        var packed = target - (1 << 11);
        out.write((1 << 5) | (1 << 3) | ((packed >> 16) & 0x7));
        out.write((packed >> 8) & 0xFF);
        out.write(packed & 0xFF);
    }

    private static void writeArrayHeader(ByteArrayOutputStream out, int size) {
        if (size < 29) {
            out.write(size);
            out.write(0x04);
            return;
        }
        if (size < 285) {
            out.write(29);
            out.write(0x04);
            out.write(size - 29);
            return;
        }

        var encoded = size - 285;
        out.write(30);
        out.write(0x04);
        out.write((encoded >> 8) & 0xFF);
        out.write(encoded & 0xFF);
    }

    private static void writeMapHeader(ByteArrayOutputStream out, int size) {
        if (size <= 28) {
            out.write(0xE0 | size);
            return;
        }
        if (size <= 284) {
            out.write(0xFD);
            out.write(size - 29);
            return;
        }
        out.write(0xFE);
        var encoded = size - 285;
        out.write((encoded >>> 8) & 0xFF);
        out.write(encoded & 0xFF);
    }

    private static byte[] unknownFieldWithFlatArray(int size) {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        writeArrayHeader(out, size);
        for (var i = 0; i < size; i++) {
            out.write(0xA0); // uint16 with value 0
        }
        return out.toByteArray();
    }

    private static byte[] unknownFieldWithFlatMap(int size) {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        writeMapHeader(out, size);
        for (var i = 0; i < size; i++) {
            out.write(0x40); // empty UTF-8 string key
            out.write(0xA0); // uint16 with value 0
        }
        return out.toByteArray();
    }

    private static byte[] nestedArrays(int depth) {
        var out = new ByteArrayOutputStream();
        for (var i = 0; i < depth; i++) {
            out.write(0x01); // extended type, one element
            out.write(0x04); // array
        }
        out.write(0xA0); // uint16 with value 0
        return out.toByteArray();
    }

    private static byte[] nestedMaps(int depth) {
        var out = new ByteArrayOutputStream();
        for (var i = 0; i < depth; i++) {
            out.write(0xE1); // map with one key/value pair
            out.write(0x40); // empty UTF-8 string key
        }
        out.write(0xA0); // uint16 with value 0
        return out.toByteArray();
    }

    private record EncodedValue(byte[] data, int offset) {
    }

    private static EncodedValue pointerNestedArrays(int depth) {
        var out = new ByteArrayOutputStream();
        out.write(0xA0); // uint16 with value 0
        var previous = 0;
        for (var i = 0; i < depth; i++) {
            var offset = out.size();
            out.write(0x01); // extended type, one element
            out.write(0x04); // array
            writePointer(out, previous);
            previous = offset;
        }
        return new EncodedValue(out.toByteArray(), previous);
    }

    private static EncodedValue pointerNestedMaps(int depth) {
        var out = new ByteArrayOutputStream();
        out.write(0xA0); // uint16 with value 0
        var previous = 0;
        for (var i = 0; i < depth; i++) {
            var offset = out.size();
            out.write(0xE1); // map with one key/value pair
            out.write(0x40); // empty UTF-8 string key
            writePointer(out, previous);
            previous = offset;
        }
        return new EncodedValue(out.toByteArray(), previous);
    }

    private static byte[] inlineArray(int size) {
        var out = new ByteArrayOutputStream();
        writeArrayHeader(out, size);
        for (var i = 0; i < size; i++) {
            out.write(0xA0); // uint16 with value 0
        }
        return out.toByteArray();
    }

    @Test
    public void testPointerFanOutIsBounded() throws IOException {
        // A data section of nested arrays, each holding two pointers to the
        // node below, would cost 2**depth decode operations. The decoder bounds
        // the number of values it decodes per lookup and rejects the database.
        var depth = 100;
        var out = new ByteArrayOutputStream();
        out.write(0xA0); // leaf: uint16 with value 0
        var prev = 0;
        for (var i = 0; i < depth; i++) {
            var offset = out.size();
            out.write(0x02);
            out.write(0x04);
            writePointer1(out, prev);
            writePointer1(out, prev);
            prev = offset;
        }

        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(out.toByteArray()), 0);
        var top = prev;
        assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(top, Object.class));
    }

    @Test
    public void testPointerFreeContainerDepthIsBounded() throws IOException {
        var atLimit = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(nestedArrays(TEST_MAX_DEPTH)), 0);
        atLimit.decode(0, Object.class);

        var overLimit = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(nestedArrays(TEST_MAX_DEPTH + 1)), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> overLimit.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
    }

    @Test
    public void testPointerBackedContainerDepthIsBounded() throws IOException {
        var atLimit = pointerNestedArrays(TEST_MAX_DEPTH);
        var decoderAtLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(atLimit.data()), 0);
        decoderAtLimit.decode(atLimit.offset(), Object.class);

        var overLimit = pointerNestedArrays(TEST_MAX_DEPTH + 1);
        var decoderOverLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(overLimit.data()), 0);
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoderOverLimit.decode(overLimit.offset(), Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
    }

    @Test
    public void testContainerDepthFitsReducedThreadStack() throws Exception {
        runProbe("-Xss512k", StackProbe.class);
    }

    private static void runProbe(String vmArgument, Class<?> probe) throws Exception {
        var executable = System.getProperty("os.name").startsWith("Windows")
            ? "java.exe"
            : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var classPath = System.getProperty(
            "surefire.test.class.path",
            System.getProperty("java.class.path")
        );
        var modulePath = System.getProperty("jdk.module.path");
        if (modulePath != null && !modulePath.isBlank()) {
            classPath = String.join(System.getProperty("path.separator"), classPath, modulePath);
        }
        var process = new ProcessBuilder(
            java,
            vmArgument,
            "-cp",
            classPath,
            probe.getName()
        ).redirectErrorStream(true).start();

        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError(probe.getSimpleName() + " did not finish within 15 seconds");
        }
        var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), output);
    }

    @Test
    public void testJavaValueCountBoundary() throws IOException {
        var atLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(inlineArray(65_535)), 0);
        var result = (List<?>) atLimit.decode(0, Object.class);
        assertEquals(65_535, result.size());

        var overLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(inlineArray(65_536)), 0);
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> overLimit.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum number of values"));
    }

    @Test
    public void testUnknownFieldValueCountIsBounded() {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        out.write(0x1E); // extended type, two-byte size
        out.write(0x04); // array
        out.write(0xFE); // size = 65,535
        out.write(0xE2);
        for (var i = 0; i < 65_535; i++) {
            out.write(0xA0); // uint16 with value 0
        }

        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, EmptyModel.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum number of values"));
    }

    @Test
    public void testUnknownFieldDepthIsBounded() {
        var value = nestedArrays(TEST_MAX_DEPTH);
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        out.writeBytes(value);

        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, EmptyModel.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
    }

    @Test
    public void testHugeContainerIsRejectedBeforeAllocation() throws IOException {
        // An array control byte can declare up to ~16.8 million entries from a
        // few bytes. The value limit must reject this before the decoder uses
        // the declared size as an allocation hint.
        var out = new ByteArrayOutputStream();
        out.write(0x1F); // extended type, size code 31 (three size bytes)
        out.write(0x04); // array
        out.write(0xFF); // size = 65821 + 0xFFFFFF = 16,843,036
        out.write(0xFF);
        out.write(0xFF);

        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(out.toByteArray()), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum number of values"));
    }

    @Test
    public void testArrayInitialCapacityIsBounded() throws IOException {
        var decoder = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(inlineArray(129)), 0);
        var result = decoder.decode(0, CapacityList.class);
        assertEquals(128, result.initialCapacity);
        assertEquals(129, result.size());
    }

    @Test
    public void testMapInitialCapacityIsBounded() throws IOException {
        var out = new ByteArrayOutputStream();
        out.write(0xFD); // map, size code 29
        out.write(100); // 29 + 100 = 129 entries
        for (var i = 0; i < 129; i++) {
            var key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            out.write(0x40 | key.length);
            out.writeBytes(key);
            out.write(0xA0); // uint16 with value 0
        }

        var decoder = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(out.toByteArray()), 0);
        var result = decoder.decode(0, CapacityMap.class);
        assertEquals(128, result.initialCapacity);
        assertEquals(129, result.size());
    }

    @Test
    public void testNestedLargeCollectionsDoNotExhaustHeap() throws Exception {
        runProbe("-Xmx16m", AllocationProbe.class);
    }

    @Test
    public void testImpossibleArrayIsRejectedBeforeAllocation() {
        // The declared size is below the value budget, but two elements cannot
        // be encoded in the one remaining byte.
        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(new byte[] {0x02, 0x04, (byte) 0xA0}), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString(
                "a container declares more entries than the data section can hold"));
    }

    @Test
    public void testImpossibleMapIsRejectedBeforeAllocation() {
        // A one-entry map needs both a key and a value, but only one byte
        // remains after its control byte.
        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(new byte[] {(byte) 0xE1, (byte) 0xA0}), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString(
                "a container declares more entries than the data section can hold"));
    }

    @Test
    public void testCyclicPointerThrows() {
        // A pointer to itself must throw a catchable InvalidDatabaseException
        // rather than recursing until the stack overflows.
        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(new byte[] {0x20, 0x00}), 0);
        assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
    }

    @Test
    public void testAcyclicPointerToPointerThrows() {
        // The pointer chain terminates at a scalar, but pointer-to-pointer is
        // illegal regardless of whether the chain forms a cycle.
        var data = new byte[] {0x20, 0x02, 0x20, 0x04, (byte) 0xA0};
        for (var cache : List.<NodeCache>of(NoCache.getInstance(), new CHMCache())) {
            var decoder = new Decoder(cache, SingleBuffer.wrap(data), 0);
            var ex = assertThrows(
                    InvalidDatabaseException.class,
                    () -> decoder.decode(0, Object.class));
            assertThat(ex.getMessage(), containsString("pointer to a pointer"));
        }
    }

    // Writes a large scalar (bytes or string) at offset 0, followed by an array
    // of pointerCount one-byte pointers that all target it. Every pointer
    // re-decodes the shared value, so the decoder is charged its size once per
    // pointer even though the value count stays tiny.
    private static byte[] sharedScalarFanOut(int scalarType, int scalarSize, int pointerCount) {
        var out = new ByteArrayOutputStream();
        // Scalar header: size code 30 covers 285..65820 bytes.
        out.write((scalarType << 5) | 30);
        var encoded = scalarSize - 285;
        out.write((encoded >> 8) & 0xFF);
        out.write(encoded & 0xFF);
        for (var i = 0; i < scalarSize; i++) {
            out.write(0);
        }
        // Array header (extended type 11), size code 29 covers 29..284 entries.
        out.write(29);
        out.write(0x04);
        out.write(pointerCount - 29);
        for (var i = 0; i < pointerCount; i++) {
            writePointer1(out, 0);
        }
        return out.toByteArray();
    }

    @Test
    public void testPayloadAmplificationIsBounded() throws IOException {
        // 33 pointers to a 65,536-byte value would materialize just over 2 MiB,
        // one byte value at a time, while the value count stays tiny. Only the
        // payload byte bound rejects this.
        var scalarSize = 1 << 16;
        var data = sharedScalarFanOut(4, scalarSize, 33);
        var top = 3 + scalarSize;
        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(top, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum payload size"));
    }

    @Test
    public void testPayloadAmplificationIsBoundedAfterCacheFills() {
        var scalarSize = 1 << 16;
        var data = sharedScalarFanOut(4, scalarSize, 33);
        var top = 3 + scalarSize;
        var decoder = new Decoder(new CHMCache(0), SingleBuffer.wrap(data), 0);
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoder.decode(top, Object.class)
        );
        assertThat(ex.getMessage(), containsString("exceeds the maximum payload size"));
    }

    @Test
    public void testOverBudgetPayloadHeadersAreRejectedBeforePayloadRead() {
        var overBudgetHeaders = List.of(
            new byte[] {0x5F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
            new byte[] {(byte) 0x9F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}
        );
        for (var header : overBudgetHeaders) {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(header), 0);
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class)
            );
            assertThat(ex.getMessage(), containsString("exceeds the maximum payload size"));
        }
    }

    @Test
    public void testTruncatedPayloadsAreRejectedAsInvalidDatabase() {
        var headers = List.of(
            new byte[] {0x41},
            new byte[] {(byte) 0x81}
        );
        for (var header : headers) {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(header), 0);
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class)
            );
            assertThat(ex.getMessage(), containsString("extends beyond the end"));
        }
    }

    @Test
    public void testInvalidStringDoesNotChangeBufferLimit() throws IOException {
        var data = new byte[] {0x41, (byte) 0xFF, 0x41, 'a'};
        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);

        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoder.decode(0, Object.class)
        );
        assertThat(ex.getMessage(), containsString("invalid UTF-8 string"));
        assertEquals("a", decoder.decode(2, Object.class));
    }

    @Test
    public void testSkippedPayloadDoesNotConsumeMaterializationBudget() throws IOException {
        var payloadSize = (1 << 21) + 1;
        var out = new ByteArrayOutputStream();
        out.write(0xE2); // map with two key/value pairs
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        out.write(0x5F); // UTF-8 string, size code 31
        var encodedSize = payloadSize - 65_821;
        out.write((encodedSize >>> 16) & 0xFF);
        out.write((encodedSize >>> 8) & 0xFF);
        out.write(encodedSize & 0xFF);
        out.writeBytes(new byte[payloadSize]);
        out.write(0x45); // five-byte UTF-8 string
        out.writeBytes("known".getBytes(StandardCharsets.UTF_8));
        out.write(0x42); // two-byte UTF-8 string
        out.writeBytes("ok".getBytes(StandardCharsets.UTF_8));

        var decoder = new Decoder(
            NoCache.getInstance(),
            SingleBuffer.wrap(out.toByteArray()),
            0
        );
        var result = decoder.decode(0, KnownFieldModel.class);
        assertEquals("ok", result.known());
    }

    @Test
    public void testPayloadAtLimitIsAccepted() throws IOException {
        // 32 pointers to a 65,536-byte value materialize exactly 2 MiB, at the
        // inclusive limit, so the record must still decode.
        var scalarSize = 1 << 16;
        var data = sharedScalarFanOut(4, scalarSize, 32);
        var top = 3 + scalarSize;
        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
        var result = (List<?>) decoder.decode(top, Object.class);
        assertEquals(32, result.size());
    }

    public static final class StackProbe {
        private StackProbe() {
        }

        public static void main(String[] args) throws IOException {
            decode(nestedArrays(TEST_MAX_DEPTH), 0);
            decode(nestedMaps(TEST_MAX_DEPTH), 0);

            var pointerArray = pointerNestedArrays(TEST_MAX_DEPTH);
            decode(pointerArray.data(), pointerArray.offset());
            var pointerMap = pointerNestedMaps(TEST_MAX_DEPTH);
            decode(pointerMap.data(), pointerMap.offset());

            expectDepthRejection(nestedArrays(TEST_MAX_DEPTH + 1), 0);
            expectDepthRejection(nestedMaps(TEST_MAX_DEPTH + 1), 0);

            pointerArray = pointerNestedArrays(TEST_MAX_DEPTH + 1);
            expectDepthRejection(pointerArray.data(), pointerArray.offset());
            pointerMap = pointerNestedMaps(TEST_MAX_DEPTH + 1);
            expectDepthRejection(pointerMap.data(), pointerMap.offset());

            decodeUnknown(unknownFieldWithFlatArray(65_532));
            decodeUnknown(unknownFieldWithFlatMap(32_766));
        }

        private static void decode(byte[] data, int offset) throws IOException {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
            decoder.decode(offset, Object.class);
        }

        private static void expectDepthRejection(byte[] data, int offset) throws IOException {
            try {
                decode(data, offset);
                throw new AssertionError("over-depth container decoded without rejection");
            } catch (InvalidDatabaseException e) {
                if (!e.getMessage().contains("exceeds the maximum depth")) {
                    throw e;
                }
            }
        }

        private static void decodeUnknown(byte[] data) throws IOException {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
            decoder.decode(0, EmptyModel.class);
        }
    }

    public static final class EmptyModel {
        @MaxMindDbConstructor
        public EmptyModel() {
        }
    }

    public static final class KnownFieldModel {
        private final String known;

        @MaxMindDbConstructor
        public KnownFieldModel(@MaxMindDbParameter(name = "known") String known) {
            this.known = known;
        }

        public String known() {
            return this.known;
        }
    }

    public static final class CapacityList extends ArrayList<Object> {
        private static final long serialVersionUID = 1L;
        private final int initialCapacity;

        public CapacityList(int initialCapacity) {
            super(initialCapacity);
            this.initialCapacity = initialCapacity;
        }
    }

    public static final class CapacityMap extends HashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private final int initialCapacity;

        public CapacityMap(int initialCapacity) {
            super(initialCapacity);
            this.initialCapacity = initialCapacity;
        }
    }

    public static final class AllocationProbe {
        private AllocationProbe() {
        }

        public static void main(String[] args) throws IOException {
            decodeRecursivelyNestedArray();
            decodeRecursivelyNestedMap();
        }

        private static void decodeRecursivelyNestedArray() throws IOException {
            var data = new byte[40_000];
            var encodedSize = 32_768 - 285;
            data[0] = 0x1E; // extended type, size code 30
            data[1] = 0x04; // array
            data[2] = (byte) (encodedSize >> 8);
            data[3] = (byte) encodedSize;
            data[4] = 0x20; // one-byte pointer to offset 0
            data[5] = 0x00;

            expectDepthRejection(data);
        }

        private static void decodeRecursivelyNestedMap() throws IOException {
            var data = new byte[40_000];
            var encodedSize = 16_384 - 285;
            data[0] = (byte) 0xFE; // map, size code 30
            data[1] = (byte) (encodedSize >> 8);
            data[2] = (byte) encodedSize;
            data[3] = 0x41; // one-byte UTF-8 string key
            data[4] = 'a';
            data[5] = (byte) 0xA0; // uint16 with value 0
            data[6] = 0x40; // empty UTF-8 string key
            data[7] = 0x20; // one-byte pointer to offset 0
            data[8] = 0x00;

            expectDepthRejection(data);
        }

        private static void expectDepthRejection(byte[] data) throws IOException {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
            try {
                decoder.decode(0, Object.class);
                throw new AssertionError("nested large collection decoded without rejection");
            } catch (InvalidDatabaseException e) {
                if (!e.getMessage().contains("exceeds the maximum depth")) {
                    throw e;
                }
            }
        }
    }

    private static <T> void testTypeDecoding(Type type, Map<T, byte[]> tests)
            throws IOException {
        var cache = new CHMCache();

        for (Map.Entry<T, byte[]> entry : tests.entrySet()) {
            var expect = entry.getKey();
            var input = entry.getValue();

            var desc = "decoded " + type.name() + " - " + expect;
            var buffer = SingleBuffer.wrap(input);

            var decoder = new TestDecoder(cache, buffer, 0);

            switch (type) {
                case BYTES:
                    assertArrayEquals((byte[]) expect, decoder.decode(0, byte[].class), desc);
                    break;
                case ARRAY:
                    assertEquals(expect, decoder.decode(0, List.class), desc);
                    break;
                case UINT16:
                case INT32:
                    assertEquals(expect, decoder.decode(0, Integer.class), desc);
                    break;
                case UINT32:
                case POINTER:
                    assertEquals(expect, decoder.decode(0, Long.class), desc);
                    break;
                case UINT64:
                case UINT128:
                    assertEquals(expect, decoder.decode(0, BigInteger.class), desc);
                    break;
                case DOUBLE:
                    assertEquals(expect, decoder.decode(0, Double.class), desc);
                    break;
                case FLOAT:
                    assertEquals(expect, decoder.decode(0, Float.class), desc);
                    break;
                case UTF8_STRING:
                    assertEquals(expect, decoder.decode(0, String.class), desc);
                    break;
                case BOOLEAN:
                    assertEquals(expect, decoder.decode(0, Boolean.class), desc);
                    break;
                default: {
                    // We hit this for Type.MAP.

                    var got = decoder.decode(0, Map.class);
                    var expectMap = (Map<?, ?>) expect;

                    assertEquals(expectMap.size(), got.size(), desc);

                    for (Object keyObject : expectMap.keySet()) {
                        var key = (String) keyObject;
                        var value = expectMap.get(key);

                        if (value instanceof Object[] arrayValue) {
                            assertArrayEquals(arrayValue, (Object[]) got.get(key), desc);
                        } else {
                            assertEquals(value, got.get(key), desc);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testUint64Coercion() throws IOException {
        // Test data: small UINT64 values that fit in smaller types
        var testData = largeUint(64);

        var cache = new CHMCache();

        // Test UINT64(0) → Byte
        var zeroBytes = testData.get(BigInteger.ZERO);
        var buffer = SingleBuffer.wrap(zeroBytes);
        var decoder = new TestDecoder(cache, buffer, 0);
        assertEquals((byte) 0, decoder.decode(0, Byte.class), "UINT64(0) should coerce to byte");

        // Test UINT64(500) → Long
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(500L, decoder.decode(0, Long.class), "UINT64(500) should coerce to long");

        // Test UINT64(500) → Integer
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(500, decoder.decode(0, Integer.class), "UINT64(500) should coerce to int");

        // Test UINT64(500) → Short
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals((short) 500, decoder.decode(0, Short.class), "UINT64(500) should coerce to short");

        // Test UINT64(500) → Byte (should fail - out of range)
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        var finalDecoder1 = decoder;
        var ex1 = assertThrows(DeserializationException.class,
            () -> finalDecoder1.decode(0, Byte.class),
            "UINT64(500) should not fit in byte");
        assertThat(ex1.getMessage(), containsString("out of range for byte"));

        // Test UINT64(2^64-1) → Long (should fail - too large)
        var maxUint64 = BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE);
        buffer = SingleBuffer.wrap(testData.get(maxUint64));
        decoder = new TestDecoder(cache, buffer, 0);
        var finalDecoder2 = decoder;
        var ex2 = assertThrows(DeserializationException.class,
            () -> finalDecoder2.decode(0, Long.class),
            "UINT64(2^64-1) should not fit in long");
        assertThat(ex2.getMessage(), containsString("out of range for long"));

        // Test UINT64(2^64-1) → BigInteger (should work)
        buffer = SingleBuffer.wrap(testData.get(maxUint64));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(maxUint64, decoder.decode(0, BigInteger.class),
            "UINT64(2^64-1) should decode to BigInteger");

        // Test UINT64(10872) → Float
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(10872)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(10872.0f, decoder.decode(0, Float.class), 0.001f,
            "UINT64(10872) should coerce to float");

        // Test UINT64(10872) → Double
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(10872)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(10872.0, decoder.decode(0, Double.class), 0.001,
            "UINT64(10872) should coerce to double");
    }

    @Test
    public void testUint128Coercion() throws IOException {
        // Test data: UINT128 values
        var testData = largeUint(128);

        var cache = new CHMCache();

        // Test UINT128(0) → Long
        var zeroBytes = testData.get(BigInteger.ZERO);
        var buffer = SingleBuffer.wrap(zeroBytes);
        var decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(0L, decoder.decode(0, Long.class), "UINT128(0) should coerce to long");

        // Test UINT128(500) → Integer
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(500, decoder.decode(0, Integer.class), "UINT128(500) should coerce to int");

        // Test UINT128(2^128-1) → Long (should fail - way too large)
        var maxUint128 = BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE);
        buffer = SingleBuffer.wrap(testData.get(maxUint128));
        decoder = new TestDecoder(cache, buffer, 0);
        var finalDecoder = decoder;
        var ex = assertThrows(DeserializationException.class,
            () -> finalDecoder.decode(0, Long.class),
            "UINT128(2^128-1) should not fit in long");
        assertThat(ex.getMessage(), containsString("out of range for long"));

        // Test UINT128(2^128-1) → BigInteger (should work)
        buffer = SingleBuffer.wrap(testData.get(maxUint128));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(maxUint128, decoder.decode(0, BigInteger.class),
            "UINT128(2^128-1) should decode to BigInteger");
    }

}
