package com.maxmind.maxminddb;

import java.net.InetAddress;
import java.nio.ByteBuffer;

final class Log {

    static void debug(String string, ByteBuffer buffer) {
        debug(string, buffer.array());
    }

    static void debug(String string, byte[] buffer) {
        String binary = "";
        for (byte b : buffer) {
            binary += Integer.toBinaryString(b & 0xFF) + " ";
        }
        Log.debug(string, binary);

    }

    static void debug(String name, InetAddress address) {
        Log.debug(name, address.toString());
    }

    static void debug(String name, long offset) {
        Log.debug(name, String.valueOf(offset));
    }

    static void debug(String name, int value) {
        Log.debug(name, String.valueOf(value));
    }

    static void debugBinary(String name, int value) {
        Log.debug(name, Integer.toBinaryString(value));
    }

    static void debug(String name, String value) {
        debug(name + ": " + value);
    }

    static void debugNewLine() {
        System.out.println();
    }

    static void debug(String message) {
        System.out.println(message);
    }

}
