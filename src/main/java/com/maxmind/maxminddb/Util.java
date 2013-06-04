package com.maxmind.maxminddb;

import java.nio.ByteBuffer;

class Util {

    public Util() {
        // TODO Auto-generated constructor stub
    }

    static int decodeInteger(byte[] bytes) {
        int i = 0;
        for (byte b : bytes) {
            i = (i << 8) | (b & 0xFF);
        }
        return i;
    }

    static long decodeLong(byte[] bytes) {
        long i = 0;
        for (byte b : bytes) {
            i = (i << 8) | (b & 0xFF);
        }
        return i;
    }

    static byte[] getByteArray(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

}
