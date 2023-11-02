package com.maxmind.db;

import java.net.InetAddress;

public class BadVersionException extends Exception {
    public BadVersionException(InetAddress ip) {
        super("you attempted to use an IPv6 network in an IPv4-only database: " + ip.toString());
    }
}
