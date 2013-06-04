package com.maxmind.maxminddb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.fasterxml.jackson.databind.JsonNode;

public class Reader {
    private static final int DATA_SECTION_SEPARATOR_SIZE = 16;
    private static final byte[] METADATA_START_MARKER = { (byte) 0xAB,
            (byte) 0xCD, (byte) 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.',
            'c', 'o', 'm' };

    private final boolean DEBUG;
    private final Decoder decoder;
    private final Metadata metadata;
    private final FileChannel fc;
    private final RandomAccessFile raf;

    public Reader(File database) throws MaxMindDbException, IOException {
        this.DEBUG = System.getenv().get("MAXMIND_DB_READER_DEBUG") != null;
        this.raf = new RandomAccessFile(database, "r");
        this.fc = this.raf.getChannel();
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

        Decoder metadataDecoder = new Decoder(this.fc, 0);

        this.metadata = new Metadata(metadataDecoder.decode(start).getObject());

        this.decoder = new Decoder(this.fc, this.metadata.searchTreeSize
                + DATA_SECTION_SEPARATOR_SIZE);

        if (this.DEBUG) {
            Log.debug(this.metadata.toString());
        }
    }

    public JsonNode get(InetAddress address) throws MaxMindDbException,
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

        if (this.DEBUG) {
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

            if (this.DEBUG) {
                Log.debug("Nodes", Arrays.toString(nodes));
                Log.debug("Bit #", i);
                Log.debug("Bit value", bit);
                Log.debug("Record", bit == 1 ? "right" : "left");
                // Log.debug("Node count", this.metadata.nodeCount);
                Log.debug("Record value", record);
            }

            if (record == this.metadata.nodeCount) {
                if (this.DEBUG) {
                    Log.debug("Record is empty");
                }
                return 0;
            }

            if (record >= this.metadata.nodeCount) {
                if (this.DEBUG) {
                    Log.debug("Record is a data pointer");
                }
                return record;
            }

            if (this.DEBUG) {
                Log.debug("Record is a node number");
            }

            nodeNum = record;
        }

        throw new MaxMindDbException("Something bad happened");
    }

    private long[] readNode(long nodeNumber) throws IOException,
            MaxMindDbException {
        ByteBuffer buffer = ByteBuffer
                .wrap(new byte[this.metadata.nodeByteSize]);
        this.fc.position(nodeNumber * this.metadata.nodeByteSize);

        this.fc.read(buffer);

        if (this.DEBUG) {
            Log.debug("Node bytes", buffer);
        }
        return this.splitNodeIntoRecords(buffer);
    }

    private long[] splitNodeIntoRecords(ByteBuffer bytes)
            throws MaxMindDbException {
        long[] nodes = new long[2];
        switch (this.metadata.recordSize) {
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

    private JsonNode resolveDataPointer(long pointer)
            throws MaxMindDbException, IOException {
        long resolved = (pointer - this.metadata.nodeCount)
                + this.metadata.searchTreeSize;

        if (this.DEBUG) {
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

        FILE: for (long i = 0; i < fileSize - METADATA_START_MARKER.length + 1; i++) {
            for (int j = 0; j < METADATA_START_MARKER.length; j++) {
                ByteBuffer b = ByteBuffer.wrap(new byte[1]);
                this.fc.read(b, fileSize - i - j - 1);
                if (b.get(0) != METADATA_START_MARKER[METADATA_START_MARKER.length
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

    public void close() throws IOException {
        if (this.fc != null) {
            this.fc.close();
        }
        if (this.raf != null) {
            this.raf.close();
        }
    }
}