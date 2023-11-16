package com.maxmind.db;

import java.net.InetAddress;

public class InvalidNetworkException extends Exception {
    public InvalidNetworkException(InetAddress ip) {
        super("you attempted to use an IPv6 network in an IPv4-only database: " + ip.toString());
    }
}
