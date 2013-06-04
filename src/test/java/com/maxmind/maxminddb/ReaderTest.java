package com.maxmind.maxminddb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ReaderTest {

    @Test
    public void test() throws MaxMindDbException, IOException {
        for (long recordSize : new long[] { 24 }) { // 24, 28, 32 }) {
            for (int ipVersion : new int[] { 4, 6 }) {
                String fileName = "test-data/Test-IPv" + ipVersion + "-"
                        + recordSize + ".mmdb";
                Reader reader = new Reader(new File(fileName));
                this.testMetadata(reader, ipVersion, recordSize);

                if (ipVersion == 4) {
                    this.testIpV4(reader, fileName);
                } else {
                    this.testIpV6(reader, fileName);
                }

            }
        }
    }

    private void testMetadata(Reader reader, Integer ipVersion, Long recordSize) {

        Metadata metadata = reader.getMetadata();

        assertEquals("major version", Integer.valueOf(2),
                metadata.binaryFormatMajorVersion);
        assertEquals(Integer.valueOf(0), metadata.binaryFormatMinorVersion);
        assertEquals(ipVersion, metadata.ipVersion);
        assertEquals("Test", metadata.databaseType);
        assertArrayEquals(new String[] { "en", "zh" }, metadata.languages);

        Map<String, Object> description = new HashMap<String, Object>();
        description.put("en", "Test Database");
        description.put("zh", "Test Database Chinese");

        assertEquals(description, metadata.description);
        assertEquals(recordSize, metadata.recordSize);

    }

    private void testIpV4(Reader reader, String fileName)
            throws UnknownHostException, MaxMindDbException, IOException {

        for (int i = 0; i <= 5; i++) {
            String address = "1.1.1." + (int) Math.pow(2, i);
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + fileName, data,
                    reader.get(InetAddress.getByName(address)));
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
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + fileName, data,
                    reader.get(InetAddress.getByName(address)));
        }

        for (String ip : new String[] { "1.1.1.33", "255.254.253.123" }) {
            assertNull(reader.get(InetAddress.getByName(ip)));
        }
    }

    // XXX - logic could be combined with above
    private void testIpV6(Reader reader, String fileName)
            throws UnknownHostException, MaxMindDbException, IOException {
        String[] subnets = new String[] { "::1:ffff:ffff", "::2:0:0",
                "::2:0:40", "::2:0:50", "::2:0:58" };

        for (String address : subnets) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + fileName, data,
                    reader.get(InetAddress.getByName(address)));
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
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + fileName, data,
                    reader.get(InetAddress.getByName(address)));
        }

        for (String ip : new String[] { "1.1.1.33", "255.254.253.123", "89fa::" }) {
            assertNull(reader.get(InetAddress.getByName(ip)));
        }

    }
}
