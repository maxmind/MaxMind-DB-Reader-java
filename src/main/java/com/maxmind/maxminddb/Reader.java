package com.maxmind.maxminddb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

public class Reader {
    private static final boolean DEBUG = true;
    private Decoder decoder;
    private long nodeCount;
    private final long dataSectionEnd;
    private static byte METADATE_START_MARKER[] = { (byte) 0xAB, (byte) 0xCD,
            (byte) 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.', 'c', 'o', 'm' };
    private final FileChannel fc;

    public Reader(File dataSource) throws MaxMindDbException, IOException {

        RandomAccessFile raf = new RandomAccessFile(dataSource, "r");
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
                            + dataSource.getName()
                            + "). Is this a valid MaxMind DB file?");
        }

        // XXX - right?
        this.dataSectionEnd = start - METADATE_START_MARKER.length;

        Decoder decoder = new Decoder(this.fc, 0);

        // FIXME - pretty ugly that I am setting the position outside of the
        // decoder. Move this all into
        // the decoder and make sure it is thread safe
        this.fc.position(start);
        Metadata metadata = new Metadata((Map<String, Object>) decoder
                .decode(0).getObject());
        if (DEBUG) {
            Log.debug(metadata.toString());
        }
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
            long treeSize = this.searchTreeSize();

            Log.debug("Resolved data pointer", "( " + pointer + " - "
                    + this.nodeCount + " ) + " + treeSize + " = " + resolved);

        }

        // We only want the data from the decoder, not the offset where it was
        // found.
        return this.decoder.decode(resolved).getObject();
    }

    private long searchTreeSize() {
        throw new AssertionError("not implemented");
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
        System.out.println(fileSize);

        FILE: for (long i = 0; i < fileSize - METADATE_START_MARKER.length + 1; i++) {
            for (int j = 0; j < METADATE_START_MARKER.length; j++) {
                ByteBuffer b = ByteBuffer.wrap(new byte[1]);
                this.fc.read(b, fileSize - i - j - 1);
                System.out.println(b.get(0));
                if (b.get(0) != METADATE_START_MARKER[METADATE_START_MARKER.length
                        - j - 1]) {
                    continue FILE;
                }
            }
            return fileSize - i;
        }
        return -1;
    }
}