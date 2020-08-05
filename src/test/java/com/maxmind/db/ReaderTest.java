package com.maxmind.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

public class ReaderTest {
    private Reader testReader;

    @Before
    public void setupReader() {
        this.testReader = null;
    }

    @After
    public void teardownReader() throws IOException {
        if (this.testReader != null) {
            this.testReader.close();
        }
    }

    @Test
    public void test()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        for (long recordSize : new long[]{24, 28, 32}) {
            for (int ipVersion : new int[]{4, 6}) {
                File file = getFile("MaxMind-DB-test-ipv" + ipVersion + "-" + recordSize + ".mmdb");
                try (Reader reader = new Reader(file)) {
                    this.testMetadata(reader, ipVersion, recordSize);
                    if (ipVersion == 4) {
                        this.testIpV4(reader, file);
                    } else {
                        this.testIpV6(reader, file);
                    }
                }
            }
        }
    }

    static class GetRecordTest {
        InetAddress ip;
        File db;
        String network;
        boolean hasRecord;

        GetRecordTest(String ip, String file, String network, boolean hasRecord) throws UnknownHostException {
            this.ip = InetAddress.getByName(ip);
            db = getFile(file);
            this.network = network;
            this.hasRecord = hasRecord;
        }
    }

    @Test
    public void testGetRecord()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        GetRecordTest[] tests = {
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv6-32.mmdb", "1.0.0.0/8", false),
                new GetRecordTest("::1:ffff:ffff", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:1:ffff:ffff/128", true),
                new GetRecordTest("::2:0:1", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:2:0:0/122", true),
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.1/32", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.2/31", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::ffff:1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "0:0:0:0:0:0:101:100/120", true),
                new GetRecordTest("200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0.0.0.0/0", true),
                new GetRecordTest("::200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                new GetRecordTest("0:0:0:0:ffff:ffff:ffff:ffff", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                new GetRecordTest("ef00::", "MaxMind-DB-no-ipv4-search-tree.mmdb", "8000:0:0:0:0:0:0:0/1", false)
        };
        for (GetRecordTest test : tests) {
            try (Reader reader = new Reader(test.db)) {
                Record record = reader.getRecord(test.ip, Map.class);

                assertEquals(test.network, record.getNetwork().toString());

                if (test.hasRecord) {
                    assertNotNull(record.getData());
                } else {
                    assertNull(record.getData());
                }
            }
        }
    }


    @Test
    public void testNoIpV4SearchTreeFile()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("MaxMind-DB-no-ipv4-search-tree.mmdb"));
        this.testNoIpV4SearchTree(this.testReader);
    }

    @Test
    public void testNoIpV4SearchTreeStream()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getStream("MaxMind-DB-no-ipv4-search-tree.mmdb"));
        this.testNoIpV4SearchTree(this.testReader);
    }

    private void testNoIpV4SearchTree(Reader reader)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {

        assertEquals("::0/64", reader.get(InetAddress.getByName("1.1.1.1"), String.class));
        assertEquals("::0/64", reader.get(InetAddress.getByName("192.1.1.1"), String.class));
    }

    @Test
    public void testDecodingTypesFile()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(this.testReader);
    }

    @Test
    public void testDecodingTypesStream()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(this.testReader);
    }

    private void testDecodingTypes(Reader reader)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        Map<String, Object> record = reader.get(InetAddress.getByName("::1.1.1.0"), Map.class);

        assertTrue((boolean) record.get("boolean"));

        assertArrayEquals(new byte[]{0, 0, 0, (byte) 42}, (byte[]) record
                .get("bytes"));

        assertEquals("unicode! ☯ - ♫", (String) record.get("utf8_string"));

        Object[] array = (Object[]) record.get("array");
        assertEquals(3, array.length);
        assertEquals(1, (long) array[0]);
        assertEquals(2, (long) array[1]);
        assertEquals(3, (long) array[2]);

        Map map = (Map) record.get("map");
        assertEquals(1, map.size());

        Map mapX = (Map) map.get("mapX");
        assertEquals(2, mapX.size());

        Object[] arrayX = (Object[]) mapX.get("arrayX");
        assertEquals(3, arrayX.length);
        assertEquals(7, (long) arrayX[0]);
        assertEquals(8, (long) arrayX[1]);
        assertEquals(9, (long) arrayX[2]);

        assertEquals("hello", (String) mapX.get("utf8_stringX"));

        assertEquals(42.123456, (double) record.get("double"), 0.000000001);
        assertEquals(1.1, (float) record.get("float"), 0.000001);
        assertEquals(-268435456, (int) record.get("int32"));
        assertEquals(100, (int) record.get("uint16"));
        assertEquals(268435456, (long) record.get("uint32"));
        assertEquals(new BigInteger("1152921504606846976"), (BigInteger) record
                .get("uint64"));
        assertEquals(new BigInteger("1329227995784915872903807060280344576"),
                (BigInteger) record.get("uint128"));
    }

    @Test
    public void testZerosFile()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testZeros(this.testReader);
    }

    @Test
    public void testZerosStream()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testZeros(this.testReader);
    }

    private void testZeros(Reader reader)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        Map record = reader.get(InetAddress.getByName("::"), Map.class);

        assertFalse((boolean) record.get("boolean"));

        assertArrayEquals(new byte[0], (byte[]) record.get("bytes"));

        assertEquals("", (String) record.get("utf8_string"));

        Object[] array = (Object[]) record.get("array");
        assertEquals(0, array.length);

        Map map = (Map) record.get("map");
        assertEquals(0, map.size());

        assertEquals(0, (double) record.get("double"), 0.000000001);
        assertEquals(0, (float) record.get("float"), 0.000001);
        assertEquals(0, (int) record.get("int32"));
        assertEquals(0, (int) record.get("uint16"));
        assertEquals(0, (long) record.get("uint32"));
        assertEquals(BigInteger.ZERO, (BigInteger) record.get("uint64"));
        assertEquals(BigInteger.ZERO, (BigInteger) record.get("uint128"));
    }

    @Test
    public void testBrokenDatabaseFile()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test-Broken-Double-Format.mmdb"));
        this.testBrokenDatabase(this.testReader);
    }

    @Test
    public void testBrokenDatabaseStream()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getStream("GeoIP2-City-Test-Broken-Double-Format.mmdb"));
        this.testBrokenDatabase(this.testReader);
    }

    private void testBrokenDatabase(Reader reader) {
        InvalidDatabaseException ex = assertThrows(
                InvalidDatabaseException.class,
                () -> reader.get(InetAddress.getByName("2001:220::"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's data section contains bad data"));
    }

    @Test
    public void testBrokenSearchTreePointerFile()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenSearchTreePointer(this.testReader);
    }

    @Test
    public void testBrokenSearchTreePointerStream()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenSearchTreePointer(this.testReader);
    }

    private void testBrokenSearchTreePointer(Reader reader) {
        InvalidDatabaseException ex = assertThrows(InvalidDatabaseException.class,
                () -> reader.get(InetAddress.getByName("1.1.1.32"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's search tree is corrupt"));
    }

    @Test
    public void testBrokenDataPointerFile()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenDataPointer(this.testReader);
    }

    @Test
    public void testBrokenDataPointerStream()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenDataPointer(this.testReader);
    }

    private void testBrokenDataPointer(Reader reader) {
        InvalidDatabaseException ex = assertThrows(InvalidDatabaseException.class,
                () -> reader.get(InetAddress.getByName("1.1.1.16"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's data section contains bad data"));
    }

    @Test
    public void testClosedReaderThrowsException()
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        Reader reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));

        reader.close();
        ClosedDatabaseException ex = assertThrows(ClosedDatabaseException.class,
                () -> reader.get(InetAddress.getByName("1.1.1.16"), Map.class));
        assertEquals("The MaxMind DB has been closed.", ex.getMessage());
    }

    private void testMetadata(Reader reader, int ipVersion, long recordSize) {

        Metadata metadata = reader.getMetadata();

        assertEquals("major version", 2, metadata.getBinaryFormatMajorVersion());
        assertEquals(0, metadata.getBinaryFormatMinorVersion());
        assertEquals(ipVersion, metadata.getIpVersion());
        assertEquals("Test", metadata.getDatabaseType());

        String[] languages = new String[2];
        languages[0] = "en";
        languages[1] = "zh";

        assertEquals(languages, metadata.getLanguages());

        Map<String, String> description = new HashMap<>();
        description.put("en", "Test Database");
        description.put("zh", "Test Database Chinese");

        assertEquals(description, metadata.getDescription());
        assertEquals(recordSize, metadata.getRecordSize());

        Calendar cal = Calendar.getInstance();
        cal.set(2014, Calendar.JANUARY, 1);

        assertTrue(metadata.getBuildDate().compareTo(cal.getTime()) > 0);
    }

    private void testIpV4(Reader reader, File file)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {

        for (int i = 0; i <= 5; i++) {
            String address = "1.1.1." + (int) Math.pow(2, i);
            Map<String, String> data = new HashMap<>();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        Map<String, String> pairs = new HashMap<>();
        pairs.put("1.1.1.3", "1.1.1.2");
        pairs.put("1.1.1.5", "1.1.1.4");
        pairs.put("1.1.1.7", "1.1.1.4");
        pairs.put("1.1.1.9", "1.1.1.8");
        pairs.put("1.1.1.15", "1.1.1.8");
        pairs.put("1.1.1.17", "1.1.1.16");
        pairs.put("1.1.1.31", "1.1.1.16");
        for (String address : pairs.keySet()) {
            Map<String, String> data = new HashMap<>();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123"}) {
            assertNull(reader.get(InetAddress.getByName(ip), Map.class));
        }
    }

    // XXX - logic could be combined with above
    private void testIpV6(Reader reader, File file)
            throws IOException,
                   InstantiationException,
                   IllegalAccessException,
                   InvocationTargetException {
        String[] subnets = new String[]{"::1:ffff:ffff", "::2:0:0",
                "::2:0:40", "::2:0:50", "::2:0:58"};

        for (String address : subnets) {
            Map<String, String> data = new HashMap<>();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        Map<String, String> pairs = new HashMap<>();
        pairs.put("::2:0:1", "::2:0:0");
        pairs.put("::2:0:33", "::2:0:0");
        pairs.put("::2:0:39", "::2:0:0");
        pairs.put("::2:0:41", "::2:0:40");
        pairs.put("::2:0:49", "::2:0:40");
        pairs.put("::2:0:52", "::2:0:50");
        pairs.put("::2:0:57", "::2:0:50");
        pairs.put("::2:0:59", "::2:0:58");

        for (String address : pairs.keySet()) {
            Map<String, String> data = new HashMap<>();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123", "89fa::"}) {
            assertNull(reader.get(InetAddress.getByName(ip), Map.class));
        }
    }

    static File getFile(String name) {
        return new File(ReaderTest.class.getResource("/maxmind-db/test-data/" + name).getFile());
    }

    static InputStream getStream(String name) {
        return ReaderTest.class.getResourceAsStream("/maxmind-db/test-data/" + name);
    }

}
