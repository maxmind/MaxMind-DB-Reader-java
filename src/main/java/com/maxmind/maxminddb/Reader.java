package com.maxmind.maxminddb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.channels.FileChannel;

public class Reader {
    private static final boolean DEBUG = true;
    private Decoder decoder;
    private long nodeCount;
    private final long dataSourceSize;
    private static byte METADATE_START_MARKER[] = { (byte) 0xab, (byte) 0xcd,
            (byte) 0xef, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.', 'c', 'o', 'm' };

    public Reader(File dataSource) {

        RandomAccessFile raf = new RandomAccessFile(dataSource, "r");
        FileChannel fc = raf.getChannel();
        // XXX - we will want
        // MappedByteBuffer in = fc.map(FileChannel.MapMode.READ_ONLY, 0,
        // fc.size());

    }

    // FIXME - figure out what we are returning
    Object dataForAddress(InetAddress address) throws MaxMindDbException,
            IOException {

        long pointer = this.findAddressInTree(address);

        if (pointer == 0) {
            return null;
        }

        return this.resolveDataPointer(pointer);
    }

    long findAddressInTree(InetAddress address) throws MaxMindDbException {
        byte[] rawAddress = address.getAddress();

        if (DEBUG) {
            Log.debugNewLine();
            Log.debug("IP address", address);
            Log.debug("IP address", rawAddress);
            Log.debugNewLine();
        }

        // The first node of the tree is always node 0, at the beginning of the
        // value
        long nodeNum = 0;

        for (int i = 0; i < rawAddress.length * 8; i++) {
            byte b = rawAddress[i / 8];
            int bit = 1 & (b >> 7 - i);
            int[] nodes = this.readNode(nodeNum);

            int record = nodes[bit];

            if (DEBUG) {
                Log.debug("Bit #", i);
                Log.debug("Bit value", bit);
                Log.debug("Record", bit == 1 ? "right" : "left");
                Log.debug("Record value", record);
            }

            if (record == this.nodeCount) {
                if (DEBUG) {
                    Log.debug("Record is empty");
                }
                return 0;
            }

            if (record >= this.nodeCount) {
                if (DEBUG) {
                    Log.debug("Record is a data pointer");
                }
                return record;
            }

            if (DEBUG) {
                Log.debug("Record is a node number");
            }

            nodeNum = record;
        }

        // XXX - Can we get down here?
        throw new MaxMindDbException("Something bad happened");
    }

    private int[] readNode(long nodeNum) {
        throw new AssertionError("not implemented");

    }

    private Object resolveDataPointer(long pointer) throws MaxMindDbException,
            IOException {
        long resolved = (pointer - this.nodeCount) + this.searchTreeSize();

        if (DEBUG) {
            long nodeCount = this.nodeCount;
            long treeSize = this.searchTreeSize();

            Log.debug("Resolved data pointer", "( " + pointer + " - "
                    + nodeCount + " ) + " + treeSize + " = " + resolved);

        }

        // We only want the data from the decoder, not the offset where it was
        // found.
        return this.decoder.decode(resolved).getObject();
    }

    private long searchTreeSize() {
        throw new AssertionError("not implemented");
    }
}