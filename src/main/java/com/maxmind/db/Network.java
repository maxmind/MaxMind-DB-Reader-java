package com.maxmind.db;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * {@code Network} represents an IP network.
 *
 * @param ipAddress    An IP address in the network. This does not have to be
 *                     the first address in the network.
 * @param prefixLength The prefix length for the network. This is the number of
 *                     leading 1 bits in the subnet mask, sometimes also known as
 *                     netmask length.
 */
public record Network(InetAddress ipAddress, int prefixLength) {
    /**
     * @return The first address in the network.
     */
    public InetAddress networkAddress() {
        byte[] ipBytes = ipAddress.getAddress();
        byte[] networkBytes = new byte[ipBytes.length];
        int curPrefix = prefixLength;
        for (int i = 0; i < ipBytes.length && curPrefix > 0; i++) {
            byte b = ipBytes[i];
            if (curPrefix < 8) {
                int shiftN = 8 - curPrefix;
                b = (byte) ((b >> shiftN) << shiftN);
            }
            networkBytes[i] = b;
            curPrefix -= 8;
        }

        try {
            return InetAddress.getByAddress(networkBytes);
        } catch (UnknownHostException e) {
            throw new RuntimeException(
                "Illegal network address byte length of " + networkBytes.length);
        }
    }


    /**
     * @return A string representation of the network in CIDR notation, e.g.,
     *         {@code 1.2.3.0/24} or {@code 2001::/8}.
     */
    public String toString() {
        return networkAddress().getHostAddress() + "/" + prefixLength;
    }
}
