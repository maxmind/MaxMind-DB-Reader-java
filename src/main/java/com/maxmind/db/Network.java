package com.maxmind.db;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * <code>Network</code> represents an IP network.
 */
public final class Network {
    private final InetAddress ipAddress;
    private final int prefixLength;
    private InetAddress networkAddress = null;

    /**
     * Construct a <code>Network</code>
     *
     * @param ipAddress    An IP address in the network. This does not have to be
     *                     the first address in the network.
     * @param prefixLength The prefix length for the network.
     */
    public Network(InetAddress ipAddress, int prefixLength) {
        this.ipAddress = ipAddress;
        this.prefixLength = prefixLength;
    }

    /**
     * @return The first address in the network.
     */
    public InetAddress getNetworkAddress() {
        if (networkAddress != null) {
            return networkAddress;
        }
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
            networkAddress = InetAddress.getByAddress(networkBytes);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Illegal network address byte length of " + networkBytes.length);
        }
        return networkAddress;
    }

    /**
     * @return The prefix length is the number of leading 1 bits in the subnet
     * mask. Sometimes also known as netmask length.
     */
    public int getPrefixLength() {
        return prefixLength;
    }

    /***
     * @return A string representation of the network in CIDR notation, e.g.,
     * <code>1.2.3.0/24</code> or <code>2001::/8</code>.
     */
    public String toString() {
        return getNetworkAddress().getHostAddress() + "/" + prefixLength;
    }
}