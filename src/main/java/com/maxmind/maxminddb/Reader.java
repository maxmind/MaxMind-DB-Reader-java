package com.maxmind.maxminddb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;

public class Reader {
    private static int DATA_SECTION_SEPARATOR_SIZE = 16;
    private static byte METADATE_START_MARKER[] = { (byte) 0xAB, (byte) 0xCD,
            (byte) 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.', 'c', 'o', 'm' };

    private static final boolean DEBUG = false;
    private final Decoder decoder;
    private final Metadata metadata;
    private final long dataSectionEnd;
    private final FileChannel fc;

    public Reader(File database) throws MaxMindDbException, IOException {

        RandomAccessFile raf = new RandomAccessFile(database, "r");
        this.fc = raf.getChannel();
        // XXX - we will want
        // MappedByteBuffer in = fc.map(FileChannel.MapMode.READ_ONLY, 0,
        // fc.size());

        /*
         * We need to make sure that whatever chunk we read will have the
         * metadata in it. The description metadata key is a hash of
         * descriptions, one per language. The description could be something
         * verbose like "GeoIP 2.0 City Database, Multilingual - English,
         * Chinese (Taiwan), Chinese (China), French, German, Portuguese" (but
         * with c. 20 languages). That comes out to about 250 bytes _per key_.
         * Multiply that by 20 languages, and the description alon ecould use up
         * about 5k. The other keys in the metadata are very, very tiny.
         * 
         * Given all this, reading 20k seems fairly future-proof. We'd have to
         * have extremely long descriptions or descriptions in 80 languages
         * before this became too long.
         */
        long start = this.findMetadataStart();

        if (start < 0) {
            throw new MaxMindDbException(
                    "Could not find a MaxMind DB metadata marker in this file ("
                            + database.getName()
                            + "). Is this a valid MaxMind DB file?");
        }

        // XXX - right?
        this.dataSectionEnd = start - METADATE_START_MARKER.length;

        Decoder metadataDecoder = new Decoder(this.fc, 0);

        this.metadata = new Metadata((Map<String, Object>) metadataDecoder
                .decode(start).getObject());

        this.decoder = new Decoder(this.fc, this.metadata.searchTreeSize
                + DATA_SECTION_SEPARATOR_SIZE);

        if (DEBUG) {
            Log.debug(this.metadata.toString());
        }
    }

    // FIXME - figure out what we are returning
    public Object get(InetAddress address) throws MaxMindDbException,
            IOException {

        long pointer = this.findAddressInTree(address);

        if (pointer == 0) {
            return null;
        }

        this.fc.position(pointer);
        return this.resolveDataPointer(pointer);
    }

    long findAddressInTree(InetAddress address) throws MaxMindDbException,
            IOException {
        byte[] rawAddress = address.getAddress();

        // XXX sort of wasteful
        if (rawAddress.length == 4 && this.metadata.ipVersion == 6) {
            byte[] newAddress = new byte[16];
            System.arraycopy(rawAddress, 0, newAddress, 12, rawAddress.length);
            rawAddress = newAddress;
        }

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
            int b = 0xFF & rawAddress[i / 8];
            int bit = 1 & (b >> 7 - (i % 8));
            long[] nodes = this.readNode(nodeNum);

            long record = nodes[bit];

            if (DEBUG) {
                Log.debug("Nodes", Arrays.toString(nodes));
                Log.debug("Bit #", i);
                Log.debug("Bit value", bit);
                Log.debug("Record", bit == 1 ? "right" : "left");
                // Log.debug("Node count", this.metadata.nodeCount);
                Log.debug("Record value", record);
            }

            if (record == this.metadata.nodeCount) {
                if (DEBUG) {
                    Log.debug("Record is empty");
                }
                return 0;
            }

            if (record >= this.metadata.nodeCount) {
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

    private long[] readNode(long nodeNumber) throws IOException,
            MaxMindDbException {
        ByteBuffer buffer = ByteBuffer
                .wrap(new byte[this.metadata.nodeByteSize]);
        this.fc.position(nodeNumber * this.metadata.nodeByteSize);

        this.fc.read(buffer);

        if (DEBUG) {
            Log.debug("Node bytes", buffer);
        }
        return this.splitNodeIntoRecords(buffer);
    }

    private long[] splitNodeIntoRecords(ByteBuffer bytes)
            throws MaxMindDbException {
        long[] nodes = new long[2];
        switch (this.metadata.recordSize.intValue()) {
            case 24:
                nodes[0] = Util.decodeLong(Arrays.copyOfRange(bytes.array(), 0,
                        3));
                nodes[1] = Util.decodeLong(Arrays.copyOfRange(bytes.array(), 3,
                        6));
                return nodes;
            case 28:
                nodes[0] = Util.decodeLong(Arrays.copyOfRange(bytes.array(), 0,
                        3));
                nodes[1] = Util.decodeLong(Arrays.copyOfRange(bytes.array(), 4,
                        7));
                nodes[0] = ((0xF0 & bytes.get(3)) << 20) | nodes[0];
                nodes[1] = ((0x0F & bytes.get(3)) << 24) | nodes[1];
                return nodes;
            case 32:
                nodes[0] = Util.decodeLong(Arrays.copyOfRange(bytes.array(), 0,
                        4));
                nodes[1] = Util.decodeLong(Arrays.copyOfRange(bytes.array(), 4,
                        8));
                return nodes;
            default:
                throw new MaxMindDbException("Unknown record size: "
                        + this.metadata.recordSize);
        }
    }

    private Object resolveDataPointer(long pointer) throws MaxMindDbException,
            IOException {
        long resolved = (pointer - this.metadata.nodeCount)
                + this.metadata.searchTreeSize;

        if (DEBUG) {
            long treeSize = this.metadata.searchTreeSize;

            Log.debug("Resolved data pointer", "( " + pointer + " - "
                    + this.metadata.nodeCount + " ) + " + treeSize + " = "
                    + resolved);

        }

        // We only want the data from the decoder, not the offset where it was
        // found.
        return this.decoder.decode(resolved).getObject();
    }

    /*
     * And here I though searching a file was a solved problem.
     * 
     * This is an extremely naive but reasonably readable implementation. There
     * are much faster algorithms (e.g., Boyer-Moore) for this if speed is ever
     * an issue, but I suspect it won't be.
     */
    private long findMetadataStart() throws IOException {
        long fileSize = this.fc.size();

        FILE: for (long i = 0; i < fileSize - METADATE_START_MARKER.length + 1; i++) {
            for (int j = 0; j < METADATE_START_MARKER.length; j++) {
                ByteBuffer b = ByteBuffer.wrap(new byte[1]);
                this.fc.read(b, fileSize - i - j - 1);
                if (b.get(0) != METADATE_START_MARKER[METADATE_START_MARKER.length
                        - j - 1]) {
                    continue FILE;
                }
            }
            return fileSize - i;
        }
        return -1;
    }

    public Metadata getMetadata() {
        return this.metadata;
    }
}