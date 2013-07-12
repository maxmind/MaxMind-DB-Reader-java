package com.maxmind.maxminddb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Instances of this class provide a reader for the MaxMind DB format. IP
 * addresses can be looked up using the <code>get</code> method.
 */
public final class MaxMindDbReader implements Closeable {
    private static final int DATA_SECTION_SEPARATOR_SIZE = 16;
    private static final byte[] METADATA_START_MARKER = { (byte) 0xAB,
            (byte) 0xCD, (byte) 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.',
            'c', 'o', 'm' };

    private final static boolean DEBUG = System.getenv().get(
            "MAXMIND_DB_READER_DEBUG") != null;
    private final Decoder decoder;
    private final Metadata metadata;
    private final ThreadBuffer threadBuffer;

    /**
     * The file mode to use when opening a MaxMind DB.
     */
    public enum FileMode {
        /**
         * The default file mode. This maps the database to virtual memory. This
         * often provides similar performance to loading the database into real
         * memory without the overhead.
         */
        MEMORY_MAPPED,
        /**
         * Loads the database into memory when the reader is constructed.
         */
        MEMORY
    }

    /**
     * Constructs a Reader for the MaxMind DB format. The file passed to it must
     * be a valid MaxMind DB file such as a GeoIP2 database file.
     *
     * @param database
     *            the MaxMind DB file to use.
     * @throws IOException
     *             if there is an error opening or reading from the file.
     */
    public MaxMindDbReader(File database) throws IOException {
        this(database, FileMode.MEMORY_MAPPED);
    }

    /**
     * Constructs a Reader for the MaxMind DB format. The file passed to it must
     * be a valid MaxMind DB file such as a GeoIP2 database file.
     *
     * @param database
     *            the MaxMind DB file to use.
     * @param fileMode
     *            the mode to open the file with.
     * @throws IOException
     *             if there is an error opening or reading from the file.
     */
    public MaxMindDbReader(File database, FileMode fileMode) throws IOException {
        this.threadBuffer = new ThreadBuffer(database, fileMode);
        int start = this.findMetadataStart(database.getName());

        Decoder metadataDecoder = new Decoder(this.threadBuffer, 0);
        this.metadata = new Metadata(metadataDecoder.decode(start).getNode());
        this.decoder = new Decoder(this.threadBuffer,
                this.metadata.searchTreeSize + DATA_SECTION_SEPARATOR_SIZE);

        if (MaxMindDbReader.DEBUG) {
            Log.debug(this.metadata.toString());
        }
    }

    /**
     * Looks up the <code>address</code> in the MaxMind DB.
     *
     * @param ipAddress
     *            the IP address to look up.
     * @return the record for the IP address.
     * @throws IOException
     *             if a file I/O error occurs.
     */
    public JsonNode get(InetAddress ipAddress) throws IOException {
        int pointer = this.findAddressInTree(ipAddress);
        if (pointer == 0) {
            return null;
        }
        return this.resolveDataPointer(pointer);
    }

    private int findAddressInTree(InetAddress address)
            throws InvalidDatabaseException {
        byte[] rawAddress = address.getAddress();

        if (MaxMindDbReader.DEBUG) {
            Log.debugNewLine();
            Log.debug("IP address", address);
            Log.debug("IP address", rawAddress);
            Log.debugNewLine();
        }

        boolean isIp4AddressInIp6Db = rawAddress.length == 4
                && this.metadata.ipVersion == 6;
        int ipStartBit = isIp4AddressInIp6Db ? 96 : 0;

        // The first node of the tree is always node 0, at the beginning of the
        // value
        int nodeNum = 0;

        for (int i = 0; i < rawAddress.length * 8 + ipStartBit; i++) {
            int bit = 0;
            if (i >= ipStartBit) {
                int b = 0xFF & rawAddress[(i - ipStartBit) / 8];
                bit = 1 & (b >> 7 - (i % 8));
            }
            int record = this.readNode(nodeNum, bit);

            if (MaxMindDbReader.DEBUG) {
                Log.debug("Bit #", i);
                Log.debug("Bit value", bit);
                Log.debug("Record", bit == 1 ? "right" : "left");
                Log.debug("Record value", record);
            }

            if (record == this.metadata.nodeCount) {
                if (MaxMindDbReader.DEBUG) {
                    Log.debug("Record is empty");
                }
                return 0;
            } else if (record > this.metadata.nodeCount) {
                if (MaxMindDbReader.DEBUG) {
                    Log.debug("Record is a data pointer");
                }
                return record;
            }

            if (MaxMindDbReader.DEBUG) {
                Log.debug("Record is a node number");
            }

            nodeNum = record;
        }
        throw new InvalidDatabaseException("Something bad happened");
    }

    private int readNode(int nodeNumber, int index)
            throws InvalidDatabaseException {
        ByteBuffer buffer = this.threadBuffer.get();
        int baseOffset = nodeNumber * this.metadata.nodeByteSize;

        switch (this.metadata.recordSize) {
            case 24:
                buffer.position(baseOffset + index * 3);
                return Decoder.decodeInteger(buffer, 0, 3);
            case 28:
                int middle = buffer.get(baseOffset + 3);

                if (index == 0) {
                    middle = (0xF0 & middle) >>> 4;
                } else {
                    middle = 0x0F & middle;
                }
                buffer.position(baseOffset + index * 4);
                return Decoder.decodeInteger(buffer, middle, 3);
            case 32:
                buffer.position(baseOffset + index * 4);
                return Decoder.decodeInteger(buffer, 0, 4);
            default:
                throw new InvalidDatabaseException("Unknown record size: "
                        + this.metadata.recordSize);
        }
    }

    private JsonNode resolveDataPointer(int pointer) throws IOException {
        int resolved = (pointer - this.metadata.nodeCount)
                + this.metadata.searchTreeSize;

        if (MaxMindDbReader.DEBUG) {
            int treeSize = this.metadata.searchTreeSize;
            Log.debug("Resolved data pointer", "( " + pointer + " - "
                    + this.metadata.nodeCount + " ) + " + treeSize + " = "
                    + resolved);
        }

        // We only want the data from the decoder, not the offset where it was
        // found.
        return this.decoder.decode(resolved).getNode();
    }

    /*
     * Apparently searching a file for a sequence is not a solved problem in
     * Java. This searches from the end of the file for metadata start.
     *
     * This is an extremely naive but reasonably readable implementation. There
     * are much faster algorithms (e.g., Boyer-Moore) for this if speed is ever
     * an issue, but I suspect it won't be.
     */
    private int findMetadataStart(String databaseName) throws InvalidDatabaseException {
        ByteBuffer buffer = this.threadBuffer.get();
        int fileSize = buffer.capacity();

        FILE: for (int i = 0; i < fileSize - METADATA_START_MARKER.length + 1; i++) {
            for (int j = 0; j < METADATA_START_MARKER.length; j++) {
                byte b = buffer.get(fileSize - i - j - 1);
                if (b != METADATA_START_MARKER[METADATA_START_MARKER.length - j
                        - 1]) {
                    continue FILE;
                }
            }
            return fileSize - i;
        }
        throw new InvalidDatabaseException(
               "Could not find a MaxMind DB metadata marker in this file ("
                        + databaseName
                        + "). Is this a valid MaxMind DB file?");
    }

    Metadata getMetadata() {
        return this.metadata;
    }

    /**
     * Closes the MaxMind DB and returns resources to the system.
     *
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        this.threadBuffer.close();
    }
}
