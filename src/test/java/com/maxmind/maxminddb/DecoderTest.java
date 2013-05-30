package com.maxmind.maxminddb;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class DecoderTest {

    private static Map<Integer, byte[]> int32() {
        int max = (2 << 30) - 1;
        HashMap<Integer, byte[]> int32 = new HashMap<Integer, byte[]>();

        int32.put(Integer.valueOf(0), new byte[] { 0b00000000, 0b00000001 });
        int32.put(Integer.valueOf(-1), new byte[] { 0b00000100, 0b00000001,
                (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111,
                (byte) 0b11111111 });
        int32.put(Integer.valueOf((2 << 7) - 1), new byte[] { 0b00000001,
                0b00000001, (byte) 0b11111111 });
        int32.put(Integer.valueOf(1 - (2 << 7)), new byte[] { 0b00000100,
                0b00000001, (byte) 0b11111111, (byte) 0b11111111,
                (byte) 0b11111111, 0b00000001 });
        int32.put(Integer.valueOf(500), new byte[] { 0b00000010, 0b00000001,
                0b00000001, (byte) 0b11110100 });

        int32.put(Integer.valueOf(-500), new byte[] { 0b00000100, 0b00000001,
                (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111110,
                0b00001100 });

        int32.put(Integer.valueOf((2 << 15) - 1), new byte[] { 0b00000010,
                0b00000001, (byte) 0b11111111, (byte) 0b11111111 });
        int32.put(Integer.valueOf(1 - (2 << 15)), new byte[] { 0b00000100,
                0b00000001, (byte) 0b11111111, (byte) 0b11111111, 0b00000000,
                0b00000001 });
        int32.put(Integer.valueOf((2 << 23) - 1), new byte[] { 0b00000011,
                0b00000001, (byte) 0b11111111, (byte) 0b11111111,
                (byte) 0b11111111 });
        int32.put(Integer.valueOf(1 - (2 << 23)), new byte[] { 0b00000100,
                0b00000001, (byte) 0b11111111, 0b00000000, 0b00000000,
                0b00000001 });
        int32.put(Integer.valueOf(max), new byte[] { 0b00000100, 0b00000001,
                0b01111111, (byte) 0b11111111, (byte) 0b11111111,
                (byte) 0b11111111 });
        int32.put(Integer.valueOf(-max), new byte[] { 0b00000100, 0b00000001,
                (byte) 0b10000000, 0b00000000, 0b00000000, 0b00000001 });
        return int32;
    }

    private static Map<Long, byte[]> uint32() {
        long max = (((long) 1) << 32) - 1;
        HashMap<Long, byte[]> uint32s = new HashMap<Long, byte[]>();

        uint32s.put(Long.valueOf(0), new byte[] { (byte) 0b11000000 });
        uint32s.put(Long.valueOf((1 << 8) - 1), new byte[] { (byte) 0b11000001,
                (byte) 0b11111111 });
        uint32s.put(Long.valueOf(500), new byte[] { (byte) 0b11000010,
                0b00000001, (byte) 0b11110100 });
        uint32s.put(Long.valueOf(10872), new byte[] { (byte) 0b11000010,
                0b00101010, 0b01111000 });
        uint32s.put(Long.valueOf((1 << 16) - 1), new byte[] {
                (byte) 0b11000010, (byte) 0b11111111, (byte) 0b11111111 });
        uint32s.put(Long.valueOf((1 << 24) - 1), new byte[] {
                (byte) 0b11000011, (byte) 0b11111111, (byte) 0b11111111,
                (byte) 0b11111111 });
        uint32s.put(max, new byte[] { (byte) 0b11000100, (byte) 0b11111111,
                (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111 });

        return uint32s;
    }

    @SuppressWarnings("boxing")
    private static Map<Integer, byte[]> uint16() {
        int max = (1 << 16) - 1;

        Map<Integer, byte[]> uint16s = new HashMap<Integer, byte[]>();

        uint16s.put(0, new byte[] { (byte) 0b10100000 });
        uint16s.put((1 << 8) - 1, new byte[] { (byte) 0b10100001,
                (byte) 0b11111111 });
        uint16s.put(500, new byte[] { (byte) 0b10100010, 0b00000001,
                (byte) 0b11110100 });
        uint16s.put(10872, new byte[] { (byte) 0b10100010, 0b00101010,
                0b01111000 });
        uint16s.put(max, new byte[] { (byte) 0b10100010, (byte) 0b11111111,
                (byte) 0b11111111 });
        return uint16s;
    }

    private static Map<BigInteger, byte[]> largeUint(int bits) {
        Map<BigInteger, byte[]> uints = new HashMap<BigInteger, byte[]>();

        byte ctrlByte = (byte) (bits == 64 ? 0b00000010 : 0b00000011);

        uints.put(BigInteger.valueOf(0), new byte[] { 0b00000000, ctrlByte });
        uints.put(BigInteger.valueOf(500), new byte[] { 0b00000010, ctrlByte,
                0b00000001, (byte) 0b11110100 });
        uints.put(BigInteger.valueOf(10872), new byte[] { 0b00000010, ctrlByte,
                0b00101010, 0b01111000 });

        for (int power = 1; power <= bits / 8; power++) {

            BigInteger key = BigInteger.valueOf(2).pow(8 * power)
                    .subtract(BigInteger.valueOf(1));

            byte[] value = new byte[2 + power];
            value[0] = (byte) power;
            value[1] = ctrlByte;
            for (int i = 2; i < value.length; i++) {
                value[i] = (byte) 0b11111111;
            }
        }
        return uints;

    }

    private static Map<Long, byte[]> pointers() {
        Map<Long, byte[]> pointers = new HashMap<Long, byte[]>();

        pointers.put(Long.valueOf(0), new byte[] { 0b00100000, 0b00000000 });
        pointers.put(Long.valueOf(5), new byte[] { 0b00100000, 0b00000101 });
        pointers.put(Long.valueOf(10), new byte[] { 0b00100000, 0b00001010 });
        pointers.put(Long.valueOf((1 << 10) - 1), new byte[] { 0b00100011,
                (byte) 0b11111111, });
        pointers.put(Long.valueOf(3017), new byte[] { 0b00101000, 0b00000011,
                (byte) 0b11001001 });
        pointers.put(Long.valueOf((1 << 19) - 5), new byte[] { 0b00101111,
                (byte) 0b11110111, (byte) 0b11111011 });
        pointers.put(Long.valueOf((1 << 19) + (1 << 11) - 1), new byte[] {
                0b00101111, (byte) 0b11111111, (byte) 0b11111111 });
        pointers.put(Long.valueOf((1 << 27) - 2), new byte[] { 0b00110111,
                (byte) 0b11110111, (byte) 0b11110111, (byte) 0b11111110 });
        pointers.put(
                Long.valueOf((((long) 1) << 27) + (1 << 19) + (1 << 11) - 1),
                new byte[] { 0b00110111, (byte) 0b11111111, (byte) 0b11111111,
                        (byte) 0b11111111 });
        pointers.put(Long.valueOf((((long) 1) << 32) - 1), new byte[] {
                0b00111000, (byte) 0b11111111, (byte) 0b11111111,
                (byte) 0b11111111, (byte) 0b11111111 });
        return pointers;
    }

    @Test
    public void testUint16() throws MaxMindDbException, IOException {
        testTypeDecoding(Type.UINT16, uint16());
    }

    @Test
    public void testUint32() throws MaxMindDbException, IOException {
        testTypeDecoding(Type.UINT32, uint32());
    }

    @Test
    public void testInt32() throws MaxMindDbException, IOException {
        testTypeDecoding(Type.INT32, int32());
    }

    @Test
    public void testUint64() throws MaxMindDbException, IOException {
        testTypeDecoding(Type.UINT64, largeUint(64));
    }

    @Test
    public void testUint128() throws MaxMindDbException, IOException {
        testTypeDecoding(Type.UINT128, largeUint(128));
    }

    @Test
    public void testPointers() throws MaxMindDbException, IOException {
        testTypeDecoding(Type.POINTER, pointers());
    }

    public static <T> void testTypeDecoding(Type type, Map<T, byte[]> tests)
            throws MaxMindDbException, IOException {

        for (Map.Entry<T, byte[]> entry : tests.entrySet()) {
            T expect = entry.getKey();
            byte[] input = entry.getValue();
            System.out.println(Arrays.toString(input));

            String desc = "decoded " + type.name() + " - " + expect;

            InputStream in = new ByteArrayInputStream(input);

            Decoder decoder = new Decoder(in, 0);
            decoder.POINTER_TEST_HACK = true;

            assertEquals(desc, expect, decoder.decode(0).getObject());
        }
    }

    @Test
    public void testDecodeInt32() {
        Map<Integer, byte[]> map = int32();
        for (Map.Entry<Integer, byte[]> entry : map.entrySet()) {
            byte[] bytes = Arrays.copyOfRange(entry.getValue(), 2,
                    entry.getValue().length);
            assertEquals("decode int32: " + entry.getKey(), entry.getKey()
                    .intValue(), Decoder.decodeInt32(bytes));
        }
    }

    @Test
    public void testDecodeUint16() {
        Map<Integer, byte[]> map = uint16();
        for (Map.Entry<Integer, byte[]> entry : map.entrySet()) {
            byte[] bytes = Arrays.copyOfRange(entry.getValue(), 1,
                    entry.getValue().length);
            assertEquals("decode uint16: " + entry.getKey(), entry.getKey()
                    .intValue(), Decoder.decodeUint16(bytes));
        }
    }

    // FIXME - these tests should be combined using generics
    @Test
    public void testDecodeUint32() {
        Map<Long, byte[]> map = uint32();
        for (Map.Entry<Long, byte[]> entry : map.entrySet()) {
            byte[] bytes = Arrays.copyOfRange(entry.getValue(), 1,
                    entry.getValue().length);
            assertEquals("decode uint32: " + entry.getKey(), entry.getKey()
                    .longValue(), Decoder.decodeUint32(bytes));
        }
    }

    @Test
    public void testDecodeUint64() {
        Map<BigInteger, byte[]> map = largeUint(64);
        for (Map.Entry<BigInteger, byte[]> entry : map.entrySet()) {
            byte[] bytes = Arrays.copyOfRange(entry.getValue(), 2,
                    entry.getValue().length);
            assertEquals("decode uint64: " + entry.getKey(), entry.getKey(),
                    Decoder.decodeUint64(bytes));
        }
    }

    @Test
    public void testDecodeUint128() {
        Map<BigInteger, byte[]> map = largeUint(128);
        for (Map.Entry<BigInteger, byte[]> entry : map.entrySet()) {
            byte[] bytes = Arrays.copyOfRange(entry.getValue(), 2,
                    entry.getValue().length);
            assertEquals("decode uint128: " + entry.getKey(), entry.getKey(),
                    Decoder.decodeUint128(bytes));
        }
    }
    //
    // @Test
    // public void testPointers() {
    // Map<Long, byte[]> map = pointers();
    // for (Map.Entry<Long, byte[]> entry : map.entrySet()) {
    //
    // byte[] bytes = entry.getValue();
    // bytes[0] &= 0b00011111;
    // assertEquals("decode uint32: " + entry.getKey(), entry.getKey()
    // .longValue(), Decoder.decodeUint32(bytes));
    // }
    // }

}
