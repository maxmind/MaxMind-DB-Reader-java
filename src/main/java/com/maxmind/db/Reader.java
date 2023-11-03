package com.maxmind.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Instances of this class provide a reader for the MaxMind DB format. IP
 * addresses can be looked up using the <code>get</code> method.
 */
public final class Reader implements Closeable {
    private static final int IPV4_LEN = 4;
    private static final int IPV6_LEN = 6;
    private static final int DATA_SECTION_SEPARATOR_SIZE = 16;
    private static final byte[] METADATA_START_MARKER = {(byte) 0xAB,
        (byte) 0xCD, (byte) 0xEF, 'M', 'a', 'x', 'M', 'i', 'n', 'd', '.',
        'c', 'o', 'm'};

    private final int ipV4Start;
    private final Metadata metadata;
    private final AtomicReference<BufferHolder> bufferHolderReference;
    private final NodeCache cache;
    private final ConcurrentHashMap<Class, CachedConstructor> constructors;

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
     * Constructs a Reader for the MaxMind DB format, with no caching. The file
     * passed to it must be a valid MaxMind DB file such as a GeoIP2 database
     * file.
     *
     * @param database the MaxMind DB file to use.
     * @throws IOException if there is an error opening or reading from the file.
     */
    public Reader(File database) throws IOException {
        this(database, NoCache.getInstance());
    }

    /**
     * Constructs a Reader for the MaxMind DB format, with the specified backing
     * cache. The file passed to it must be a valid MaxMind DB file such as a
     * GeoIP2 database file.
     *
     * @param database the MaxMind DB file to use.
     * @param cache    backing cache instance
     * @throws IOException if there is an error opening or reading from the file.
     */
    public Reader(File database, NodeCache cache) throws IOException {
        this(database, FileMode.MEMORY_MAPPED, cache);
    }

    /**
     * Constructs a Reader with no caching, as if in mode
     * {@link FileMode#MEMORY}, without using a <code>File</code> instance.
     *
     * @param source the InputStream that contains the MaxMind DB file.
     * @throws IOException if there is an error reading from the Stream.
     */
    public Reader(InputStream source) throws IOException {
        this(source, NoCache.getInstance());
    }

    /**
     * Constructs a Reader with the specified backing cache, as if in mode
     * {@link FileMode#MEMORY}, without using a <code>File</code> instance.
     *
     * @param source the InputStream that contains the MaxMind DB file.
     * @param cache  backing cache instance
     * @throws IOException if there is an error reading from the Stream.
     */
    public Reader(InputStream source, NodeCache cache) throws IOException {
        this(new BufferHolder(source), "<InputStream>", cache);
    }

    /**
     * Constructs a Reader for the MaxMind DB format, with no caching. The file
     * passed to it must be a valid MaxMind DB file such as a GeoIP2 database
     * file.
     *
     * @param database the MaxMind DB file to use.
     * @param fileMode the mode to open the file with.
     * @throws IOException if there is an error opening or reading from the file.
     */
    public Reader(File database, FileMode fileMode) throws IOException {
        this(database, fileMode, NoCache.getInstance());
    }

    /**
     * Constructs a Reader for the MaxMind DB format, with the specified backing
     * cache. The file passed to it must be a valid MaxMind DB file such as a
     * GeoIP2 database file.
     *
     * @param database the MaxMind DB file to use.
     * @param fileMode the mode to open the file with.
     * @param cache    backing cache instance
     * @throws IOException if there is an error opening or reading from the file.
     */
    public Reader(File database, FileMode fileMode, NodeCache cache) throws IOException {
        this(new BufferHolder(database, fileMode), database.getName(), cache);
    }

    private Reader(BufferHolder bufferHolder, String name, NodeCache cache) throws IOException {
        this.bufferHolderReference = new AtomicReference<>(
            bufferHolder);

        if (cache == null) {
            throw new NullPointerException("Cache cannot be null");
        }
        this.cache = cache;

        ByteBuffer buffer = bufferHolder.get();
        int start = this.findMetadataStart(buffer, name);

        Decoder metadataDecoder = new Decoder(this.cache, buffer, start);
        this.metadata = metadataDecoder.decode(start, Metadata.class);

        this.ipV4Start = this.findIpV4StartNode(buffer);

        this.constructors = new ConcurrentHashMap<>();
    }

    /**
     * Looks up <code>ipAddress</code> in the MaxMind DB.
     *
     * @param <T>       the type to populate.
     * @param ipAddress the IP address to look up.
     * @param cls       the class of object to populate.
     * @return the object.
     * @throws IOException if a file I/O error occurs.
     */
    public <T> T get(InetAddress ipAddress, Class<T> cls) throws IOException {
        return getRecord(ipAddress, cls).getData();
    }

    protected int getIpv4Start() {
        return this.ipV4Start;
    }

    /**
     * Looks up <code>ipAddress</code> in the MaxMind DB.
     *
     * @param <T>       the type to populate.
     * @param ipAddress the IP address to look up.
     * @param cls       the class of object to populate.
     * @return the record for the IP address. If there is no data for the
     * address, the non-null {@link DatabaseRecord} will still be returned.
     * @throws IOException if a file I/O error occurs.
     */
    public <T> DatabaseRecord<T> getRecord(InetAddress ipAddress, Class<T> cls)
        throws IOException {
        ByteBuffer buffer = this.getBufferHolder().get();

        byte[] rawAddress = ipAddress.getAddress();

        int bitLength = rawAddress.length * 8;
        int record = this.startNode(bitLength);
        int nodeCount = this.metadata.getNodeCount();

        int pl = 0;
        for (; pl < bitLength && record < nodeCount; pl++) {
            int b = 0xFF & rawAddress[pl / 8];
            int bit = 1 & (b >> 7 - (pl % 8));
            record = this.readNode(buffer, record, bit);
        }

        T dataRecord = null;
        if (record > nodeCount) {
            // record is a data pointer
            try {
                dataRecord = this.resolveDataPointer(buffer, record, cls);
            } catch (DeserializationException exception) {
                throw new DeserializationException(
                    "Error getting record for IP " + ipAddress + " -  " + exception.getMessage(),
                    exception);
            }
        }
        return new DatabaseRecord<>(dataRecord, ipAddress, pl);
    }

    /**
     * Creates a Networks iterator and skips aliased networks.
     * Please note that a MaxMind DB may map IPv4 networks into several locations
     * in an IPv6 database. This iterator will iterate over all of these locations
     * separately. To only iterate over the IPv4 networks once, use the
     * SkipAliasedNetworks option.
     * 
     * @return Networks The Networks iterator.
     * @throws InvalidNetworkException Exception for using an IPv6 network in ipv4-only database.
     * @throws ClosedDatabaseException Exception for a closed databased.
     * @throws InvalidDatabaseException Exception for an invalid database.
     */
    public Networks networks() throws 
        InvalidNetworkException, ClosedDatabaseException, InvalidDatabaseException {
        return this.networks(true);
    }

    /**
     * Creates a Networks iterator.
     * Please note that a MaxMind DB may map IPv4 networks into several locations
     * in an IPv6 database. This iterator will iterate over all of these locations
     * separately. To only iterate over the IPv4 networks once, use the
     * SkipAliasedNetworks option.
     * 
     * @param skipAliasedNetworks Enable skipping aliased networks.
     * @return Networks The Networks iterator.
     * @throws InvalidNetworkException Exception for using an IPv6 network in ipv4-only database.
     * @throws ClosedDatabaseException Exception for a closed databased.
     * @throws InvalidDatabaseException Exception for an invalid database.
     */
    public Networks networks(boolean skipAliasedNetworks) throws 
        InvalidNetworkException, ClosedDatabaseException, InvalidDatabaseException {
        try {
            InetAddress ipv4 = InetAddress.getByAddress(new byte[4]);
            InetAddress ipv6 = InetAddress.getByAddress(new byte[16]);
            Network ipAllV4 = new Network(ipv4, 0); // Mask 32.
            Network ipAllV6 = new Network(ipv6, 0); // Mask 128.

            if (this.getMetadata().getIpVersion() == 6) {
                return this.networksWithIn(ipAllV6, skipAliasedNetworks);
            }
            return this.networksWithIn(ipAllV4, skipAliasedNetworks);
        } catch (UnknownHostException e) {
            /* This is returned by getByAddress. This should never happen
            as the ipv4 and ipv6 are constants set by us. */
            return null;
        }
    }

    protected BufferHolder getBufferHolder() throws ClosedDatabaseException {
        BufferHolder bufferHolder = this.bufferHolderReference.get();
        if (bufferHolder == null) {
            throw new ClosedDatabaseException();
        }
        return bufferHolder;
    }

    private int startNode(int bitLength) {
        // Check if we are looking up an IPv4 address in an IPv6 tree. If this
        // is the case, we can skip over the first 96 nodes.
        if (this.metadata.getIpVersion() == 6 && bitLength == 32) {
            return this.ipV4Start;
        }
        // The first node of the tree is always node 0, at the beginning of the
        // value
        return 0;
    }

    private int findIpV4StartNode(ByteBuffer buffer)
        throws InvalidDatabaseException {
        if (this.metadata.getIpVersion() == 4) {
            return 0;
        }

        int node = 0;
        for (int i = 0; i < 96 && node < this.metadata.getNodeCount(); i++) {
            node = this.readNode(buffer, node, 0);
        }
        return node;
    }

    /**
     * Returns an iterator within the specified network.
     * Please note that a MaxMind DB may map IPv4 networks into several locations
     * in an IPv6 database. This iterator will iterate over all of these locations
     * separately. To only iterate over the IPv4 networks once, use the
     * SkipAliasedNetworks option.
     * @param <T> Represents the data type(e.g., Map, HastMap, etc.).
     * @param network Specifies the network to be iterated.
     * @param skipAliasedNetworks Boolean for skipping aliased networks.
     * @return Networks
     * @throws InvalidNetworkException Exception for using an IPv6 network in ipv4-only database.
     * @throws ClosedDatabaseException Exception for a closed databased.
     * @throws InvalidDatabaseException Exception for an invalid database.
     */
    public <T> Networks<T> networksWithIn(Network network, boolean skipAliasedNetworks) 
        throws InvalidNetworkException, ClosedDatabaseException, InvalidDatabaseException {
        InetAddress networkAddress = network.getNetworkAddress();
        if (this.metadata.getIpVersion() == 4 && networkAddress instanceof Inet6Address) {
            throw new InvalidNetworkException(networkAddress);
        }

        byte[] ipBytes = networkAddress.getAddress();
        int prefixLength = network.getPrefixLength();

        if (this.metadata.getIpVersion() == 6 && ipBytes.length == IPV4_LEN) {
            if (skipAliasedNetworks) {
                ipBytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3] };
            } else {
                // Convert it to the IP address (in 16-byte from) of the IPv4 address.
                ipBytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    -1, -1, // -1 is for 0xff.
                    ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3]};
            }
            prefixLength += 96;
        }

        int[] traverseResult = this.traverseTree(ipBytes, 0, prefixLength);
        int node = traverseResult[0];
        int prefix = traverseResult[1];

        Networks<T> networks = new Networks<T>(this, skipAliasedNetworks,
            new Networks.NetworkNode[]{ new Networks.NetworkNode(ipBytes, prefix, node) });

        return networks;
    }

    /**
     * Returns the node number and the prefix for the network.
     * @param ip The ip address to travese.
     * @param node The number of the node.
     * @param bitCount The prefix.
     * @return int[]
     */
    public int[] traverseTree(byte[] ip, int node, int bitCount)
        throws ClosedDatabaseException, InvalidDatabaseException {
        int nodeCount = this.metadata.getNodeCount();
        int i = 0;

        ByteBuffer buffer = this.getBufferHolder().get();

        for (; i < bitCount && node < nodeCount; i++) {
            int bit = 1 & (ip[i >> 3] >> (7 - (i % 8)));

            // bit:0 -> left record.
            // bit:1 -> right record.
            node = this.readNode(buffer, node, bit);
        }

        return new int[]{node, i};
    }

    protected int readNode(ByteBuffer buffer, int nodeNumber, int index)
            throws InvalidDatabaseException {
        // index is the index of the record within the node, which
        // can either be 0 or 1.
        int baseOffset = nodeNumber * this.metadata.getNodeByteSize();

        switch (this.metadata.getRecordSize()) {
            case 24:
                // For a 24 bit record, each record is 3 bytes.
                buffer.position(baseOffset + index * 3);
                return Decoder.decodeInteger(buffer, 0, 3);
            case 28:
                int middle = buffer.get(baseOffset + 3);

                if (index == 0) {
                    // We get the most significant from the first half
                    // of the byte. It belongs to the first record.
                    middle = (0xF0 & middle) >>> 4;
                } else {
                    // We get the most significant byte of the second record.
                    middle = 0x0F & middle;
                }
                buffer.position(baseOffset + index * 4);
                return Decoder.decodeInteger(buffer, middle, 3);
            case 32:
                buffer.position(baseOffset + index * 4);
                return Decoder.decodeInteger(buffer, 0, 4);
            default:
                throw new InvalidDatabaseException("Unknown record size: "
                    + this.metadata.getRecordSize());
        }
    }

    protected <T> T resolveDataPointer(
        ByteBuffer buffer,
        int pointer,
        Class<T> cls
    ) throws IOException {
        int resolved = (pointer - this.metadata.getNodeCount())
            + this.metadata.getSearchTreeSize();

        if (resolved >= buffer.capacity()) {
            throw new InvalidDatabaseException(
                "The MaxMind DB file's search tree is corrupt: "
                    + "contains pointer larger than the database.");
        }

        // We only want the data from the decoder, not the offset where it was
        // found.
        Decoder decoder = new Decoder(
            this.cache,
            buffer,
            this.metadata.getSearchTreeSize() + DATA_SECTION_SEPARATOR_SIZE,
            this.constructors
        );
        return decoder.decode(resolved, cls);
    }

    /*
     * Apparently searching a file for a sequence is not a solved problem in
     * Java. This searches from the end of the file for metadata start.
     *
     * This is an extremely naive but reasonably readable implementation. There
     * are much faster algorithms (e.g., Boyer-Moore) for this if speed is ever
     * an issue, but I suspect it won't be.
     */
    private int findMetadataStart(ByteBuffer buffer, String databaseName)
        throws InvalidDatabaseException {
        int fileSize = buffer.capacity();

        FILE:
        for (int i = 0; i < fileSize - METADATA_START_MARKER.length + 1; i++) {
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
                + databaseName + "). Is this a valid MaxMind DB file?");
    }

    /**
     * @return the metadata for the MaxMind DB file.
     */
    public Metadata getMetadata() {
        return this.metadata;
    }

    /**
     * <p>
     * Closes the database.
     * </p>
     * <p>
     * If you are using <code>FileMode.MEMORY_MAPPED</code>, this will
     * <em>not</em> unmap the underlying file due to a limitation in Java's
     * <code>MappedByteBuffer</code>. It will however set the reference to
     * the buffer to <code>null</code>, allowing the garbage collector to
     * collect it.
     * </p>
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        this.bufferHolderReference.set(null);
    }
}
