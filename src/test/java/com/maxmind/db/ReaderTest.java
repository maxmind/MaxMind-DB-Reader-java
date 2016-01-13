package com.maxmind.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class ReaderTest {
    private final ObjectMapper om = new ObjectMapper();

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
    public void test() throws IOException {
        for (long recordSize : new long[]{24, 28, 32}) {
            for (int ipVersion : new int[]{4, 6}) {
                File file = getFile("MaxMind-DB-test-ipv" + ipVersion + "-" + recordSize + ".mmdb");
                Reader reader = new Reader(file);
                try {
                    this.testMetadata(reader, ipVersion, recordSize);
                    if (ipVersion == 4) {
                        this.testIpV4(reader, file);
                    } else {
                        this.testIpV6(reader, file);
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }

    @Test
    public void testNoIpV4SearchTreeFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-no-ipv4-search-tree.mmdb"));
        this.testNoIpV4SearchTree(this.testReader);
    }

    @Test
    public void testNoIpV4SearchTreeStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-no-ipv4-search-tree.mmdb"));
        this.testNoIpV4SearchTree(this.testReader);
    }

    private void testNoIpV4SearchTree(Reader reader) throws IOException {

        assertEquals("::0/64", reader.get(InetAddress.getByName("1.1.1.1"))
                .textValue());
        assertEquals("::0/64", reader.get(InetAddress.getByName("192.1.1.1"))
                .textValue());
    }

    @Test
    public void testDecodingTypesFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(this.testReader);
    }

    @Test
    public void testDecodingTypesStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(this.testReader);
    }

    private void testDecodingTypes(Reader reader) throws IOException {
        JsonNode record = reader.get(InetAddress.getByName("::1.1.1.0"));

        assertEquals(true, record.get("boolean").booleanValue());

        assertArrayEquals(new byte[]{0, 0, 0, (byte) 42}, record
                .get("bytes").binaryValue());

        assertEquals("unicode! ☯ - ♫", record.get("utf8_string").textValue());

        assertTrue(record.get("array").isArray());
        JsonNode array = record.get("array");
        assertEquals(3, array.size());
        assertEquals(3, array.size());
        assertEquals(1, array.get(0).intValue());
        assertEquals(2, array.get(1).intValue());
        assertEquals(3, array.get(2).intValue());

        assertTrue(record.get("map").isObject());
        assertEquals(1, record.get("map").size());

        JsonNode mapX = record.get("map").get("mapX");
        assertEquals(2, mapX.size());

        JsonNode arrayX = mapX.get("arrayX");
        assertEquals(3, arrayX.size());
        assertEquals(7, arrayX.get(0).intValue());
        assertEquals(8, arrayX.get(1).intValue());
        assertEquals(9, arrayX.get(2).intValue());

        assertEquals("hello", mapX.get("utf8_stringX").textValue());

        assertEquals(42.123456, record.get("double").doubleValue(), 0.000000001);
        assertEquals(1.1, record.get("float").floatValue(), 0.000001);
        assertEquals(-268435456, record.get("int32").intValue());
        assertEquals(100, record.get("uint16").intValue());
        assertEquals(268435456, record.get("uint32").intValue());
        assertEquals(new BigInteger("1152921504606846976"), record
                .get("uint64").bigIntegerValue());
        assertEquals(new BigInteger("1329227995784915872903807060280344576"),
                record.get("uint128").bigIntegerValue());
    }

    @Test
    public void testZerosFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testZeros(this.testReader);
    }

    @Test
    public void testZerosStream() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testZeros(this.testReader);
    }

    private void testZeros(Reader reader) throws IOException {
        JsonNode record = reader.get(InetAddress.getByName("::"));

        assertEquals(false, record.get("boolean").booleanValue());

        assertArrayEquals(new byte[0], record.get("bytes").binaryValue());

        assertEquals("", record.get("utf8_string").textValue());

        assertTrue(record.get("array").isArray());
        assertEquals(0, record.get("array").size());

        assertTrue(record.get("map").isObject());
        assertEquals(0, record.get("map").size());

        assertEquals(0, record.get("double").doubleValue(), 0.000000001);
        assertEquals(0, record.get("float").floatValue(), 0.000001);
        assertEquals(0, record.get("int32").intValue());
        assertEquals(0, record.get("uint16").intValue());
        assertEquals(0, record.get("uint32").intValue());
        assertEquals(BigInteger.ZERO, record.get("uint64").bigIntegerValue());
        assertEquals(BigInteger.ZERO, record.get("uint128").bigIntegerValue());
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testBrokenDatabaseFile() throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test-Broken-Double-Format.mmdb"));
        this.testBrokenDatabase(this.testReader);
    }

    @Test
    public void testBrokenDatabaseStream() throws IOException {
        this.testReader = new Reader(getStream("GeoIP2-City-Test-Broken-Double-Format.mmdb"));
        this.testBrokenDatabase(this.testReader);
    }

    private void testBrokenDatabase(Reader reader) throws IOException {

        this.thrown.expect(InvalidDatabaseException.class);
        this.thrown
                .expectMessage(containsString("The MaxMind DB file's data section contains bad data"));

        reader.get(InetAddress.getByName("2001:220::"));
    }

    @Test
    public void testBrokenSearchTreePointerFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenSearchTreePointer(this.testReader);
    }

    @Test
    public void testBrokenSearchTreePointerStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenSearchTreePointer(this.testReader);
    }

    private void testBrokenSearchTreePointer(Reader reader)
            throws IOException {

        this.thrown.expect(InvalidDatabaseException.class);
        this.thrown
                .expectMessage(containsString("The MaxMind DB file's search tree is corrupt"));

        reader.get(InetAddress.getByName("1.1.1.32"));
    }

    @Test
    public void testBrokenDataPointerFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenDataPointer(this.testReader);
    }

    @Test
    public void testBrokenDataPointerStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenDataPointer(this.testReader);
    }

    private void testBrokenDataPointer(Reader reader) throws IOException {

        this.thrown.expect(InvalidDatabaseException.class);
        this.thrown
                .expectMessage(containsString("The MaxMind DB file's data section contains bad data"));

        reader.get(InetAddress.getByName("1.1.1.16"));
    }

    @Test
    public void testObjectNodeMutation() throws IOException {
        Reader reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        ObjectNode record = (ObjectNode) reader.get(InetAddress.getByName("::1.1.1.0"));

        thrown.expect(UnsupportedOperationException.class);
        record.put("Test", "value");
    }

    @Test
    public void testArrayNodeMutation() throws IOException {
        Reader reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        ObjectNode record = (ObjectNode) reader.get(InetAddress.getByName("::1.1.1.0"));

        thrown.expect(UnsupportedOperationException.class);
        ((ArrayNode) record.get("array")).add(1);
    }

    @Test
    public void testClosedReaderThrowsException() throws IOException {
        Reader reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));

        this.thrown.expect(ClosedDatabaseException.class);
        this.thrown.expectMessage("The MaxMind DB has been closed.");

        reader.close();
        reader.get(InetAddress.getByName("1.1.1.16"));
    }

    private void testMetadata(Reader reader, int ipVersion, long recordSize) {

        Metadata metadata = reader.getMetadata();

        assertEquals("major version", 2, metadata.getBinaryFormatMajorVersion());
        assertEquals(0, metadata.getBinaryFormatMinorVersion());
        assertEquals(ipVersion, metadata.getIpVersion());
        assertEquals("Test", metadata.getDatabaseType());

        List<String> languages = new ArrayList<String>(Arrays.asList("en", "zh"));

        assertEquals(languages, metadata.getLanguages());

        Map<String, String> description = new HashMap<String, String>();
        description.put("en", "Test Database");
        description.put("zh", "Test Database Chinese");

        assertEquals(description, metadata.getDescription());
        assertEquals(recordSize, metadata.getRecordSize());

        Calendar cal = Calendar.getInstance();
        cal.set(2014, Calendar.JANUARY, 1);

        assertTrue(metadata.getBuildDate().compareTo(cal.getTime()) > 0);
    }

    private void testIpV4(Reader reader, File file) throws IOException {

        for (int i = 0; i <= 5; i++) {
            String address = "1.1.1." + (int) Math.pow(2, i);
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
        }

        Map<String, String> pairs = new HashMap<String, String>();
        pairs.put("1.1.1.3", "1.1.1.2");
        pairs.put("1.1.1.5", "1.1.1.4");
        pairs.put("1.1.1.7", "1.1.1.4");
        pairs.put("1.1.1.9", "1.1.1.8");
        pairs.put("1.1.1.15", "1.1.1.8");
        pairs.put("1.1.1.17", "1.1.1.16");
        pairs.put("1.1.1.31", "1.1.1.16");
        for (String address : pairs.keySet()) {
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123"}) {
            assertNull(reader.get(InetAddress.getByName(ip)));
        }
    }

    // XXX - logic could be combined with above
    private void testIpV6(Reader reader, File file) throws IOException {
        String[] subnets = new String[]{"::1:ffff:ffff", "::2:0:0",
                "::2:0:40", "::2:0:50", "::2:0:58"};

        for (String address : subnets) {
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
        }

        Map<String, String> pairs = new HashMap<String, String>();
        pairs.put("::2:0:1", "::2:0:0");
        pairs.put("::2:0:33", "::2:0:0");
        pairs.put("::2:0:39", "::2:0:0");
        pairs.put("::2:0:41", "::2:0:40");
        pairs.put("::2:0:49", "::2:0:40");
        pairs.put("::2:0:52", "::2:0:50");
        pairs.put("::2:0:57", "::2:0:50");
        pairs.put("::2:0:59", "::2:0:58");

        for (String address : pairs.keySet()) {
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123", "89fa::"}) {
            assertNull(reader.get(InetAddress.getByName(ip)));
        }
    }

    static File getFile(String name) {
        return new File(ReaderTest.class.getResource("/maxmind-db/test-data/" + name).getFile());
    }

    static InputStream getStream(String name) {
        return ReaderTest.class.getResourceAsStream("/maxmind-db/test-data/" + name);
    }

}
