package com.maxmind.db;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static junit.framework.TestCase.assertEquals;

public class NetworkTest {
    @Test
    public void testIPv6() throws UnknownHostException {
        Network network = new Network(
                InetAddress.getByName("2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
                28
        );

        assertEquals("2001:db0:0:0:0:0:0:0", network.getNetworkAddress().getHostAddress());
        assertEquals(28, network.getPrefixLength());
        assertEquals("2001:db0:0:0:0:0:0:0/28", network.toString());
    }

    @Test
    public void TestIPv4() throws UnknownHostException {
        Network network = new Network(
                InetAddress.getByName("192.168.213.111"),
                31
        );

        assertEquals("192.168.213.110", network.getNetworkAddress().getHostAddress());
        assertEquals(31, network.getPrefixLength());
        assertEquals("192.168.213.110/31", network.toString());
    }

}
