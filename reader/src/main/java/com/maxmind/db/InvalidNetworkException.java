package com.maxmind.db;

import java.net.InetAddress;

/**
 * This is a custom exception that is thrown when the user attempts to use an
 * IPv6 address in an IPv4-only database.
 */
public class InvalidNetworkException extends Exception {
    /**
     * @param ip the IP address that was used
     */
    public InvalidNetworkException(InetAddress ip) {
        super("you attempted to use an IPv6 network in an IPv4-only database: " + ip.toString());
    }
}
