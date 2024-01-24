package com.maxmind.db;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Stack;

/**
 * Instances of this class provide an iterator over the networks in a database.
 * The iterator will return a {@link DatabaseRecord} for each network.
 *
 * @param <T> The type of data returned by the iterator.
 */
public final class Networks<T> implements Iterator<DatabaseRecord<T>> {
    private final Reader reader;
    private final Stack<NetworkNode> nodes;
    private NetworkNode lastNode;
    private final boolean includeAliasedNetworks;
    private final ByteBuffer buffer; /* Stores the buffer for Next() calls */
    private final Class<T> typeParameterClass;
    
    /**
     * Constructs a Networks instance.
     *
     * @param reader The reader object.
     * @param includeAliasedNetworks The boolean to include aliased networks.
     * @param typeParameterClass The type of data returned by the iterator.
     * @throws ClosedDatabaseException Exception for a closed database.
     */
    Networks(Reader reader, boolean includeAliasedNetworks, Class<T> typeParameterClass) 
        throws ClosedDatabaseException {
        this(reader, includeAliasedNetworks, new NetworkNode[]{}, typeParameterClass);
    }

    /**
     * Constructs a Networks instance.
     *
     * @param reader The reader object.
     * @param includeAliasedNetworks The boolean to include aliased networks.
     * @param nodes The initial nodes array to start Networks iterator with.
     * @param typeParameterClass The type of data returned by the iterator.
     * @throws ClosedDatabaseException Exception for a closed database.
     */
    Networks(
            Reader reader,
            boolean includeAliasedNetworks,
            NetworkNode[] nodes,
            Class<T> typeParameterClass)
        throws ClosedDatabaseException {
        this.reader = reader;
        this.includeAliasedNetworks = includeAliasedNetworks;
        this.buffer = reader.getBufferHolder().get();
        this.nodes = new Stack<>();
        this.typeParameterClass = typeParameterClass;
        for (NetworkNode node : nodes) {
            this.nodes.push(node);
        }
    }

    /**
     * Constructs a Networks instance with includeAliasedNetworks set to false by default.
     *
     * @param reader The reader object.
     * @param typeParameterClass The type of data returned by the iterator.
     */
    Networks(Reader reader, Class<T> typeParameterClass) throws ClosedDatabaseException {
        this(reader, false, typeParameterClass);
    }

    /**
     * Returns the next DataRecord.
     *
     * @return The next DataRecord.
     * @throws NetworksIterationException An exception when iterating over the networks.
     */
    @Override
    public DatabaseRecord<T> next() {
        try {
            T data = this.reader.resolveDataPointer(
                this.buffer, this.lastNode.pointer, this.typeParameterClass);

            byte[] ip = this.lastNode.ip;
            int prefixLength = this.lastNode.prefix;

            // We do this because uses of includeAliasedNetworks will get IPv4 networks
            // from the ::FFFF:0:0/96. We want to return the IPv4 form of the address
            // in that case.
            if (!this.includeAliasedNetworks && isInIpv4Subtree(ip)) {
                ip = Arrays.copyOfRange(ip, 12, ip.length);
                prefixLength -= 96;
            }

            // If the ip is in ipv6 form, drop the prefix manually
            // as InetAddress converts it to ipv4.
            InetAddress ipAddr = InetAddress.getByAddress(ip);
            if (ipAddr instanceof Inet4Address && ip.length > 4 && prefixLength > 96) {
                prefixLength -= 96;
            }

            return new DatabaseRecord<>(data, ipAddr, prefixLength);
        } catch (IOException e) {
            throw new NetworksIterationException(e);
        }
    }

    private boolean isInIpv4Subtree(byte[] ip) {
        if (ip.length != 16) {
            return false;
        }
        for (int i = 0; i < 12; i++) {
            if (ip[i] != 0) {
                return false;
            }
        }
        return true;
    }
    
    /**
    * hasNext prepares the next network for reading with the Network method. It
    * returns true if there is another network to be processed and false if there
    * are no more networks.
    *
    * @return boolean True if there is another network to be processed.
    * @throws NetworksIterationException Exception while iterating over the networks.
    */
    @Override
    public boolean hasNext()  {
        while (!this.nodes.isEmpty()) {
            NetworkNode node = this.nodes.pop();

            // Next until we don't have data.
            while (node.pointer != this.reader.getMetadata().getNodeCount()) {
                // This skips IPv4 aliases without hardcoding the networks that the writer
                // currently aliases.
                if (!this.includeAliasedNetworks && this.reader.getIpv4Start() != 0
                        && node.pointer == this.reader.getIpv4Start()
                        && !isInIpv4Subtree(node.ip)) {
                    break;
                }

                if (node.pointer > this.reader.getMetadata().getNodeCount()) {
                    this.lastNode = node;
                    return true;
                }

                byte[] ipRight = Arrays.copyOf(node.ip, node.ip.length);
                if (ipRight.length <= (node.prefix >> 3)) {
                    throw new NetworksIterationException("Invalid search tree");
                }

                ipRight[node.prefix >> 3] |= 1 << (7 - (node.prefix % 8));

                try {
                    int rightPointer = this.reader.readNode(this.buffer, node.pointer, 1);
                    node.prefix++;

                    this.nodes.push(new NetworkNode(ipRight, node.prefix, rightPointer));
                    node.pointer = this.reader.readNode(this.buffer, node.pointer, 0);
                } catch (InvalidDatabaseException e) {
                    throw new NetworksIterationException(e);
                }
            }
        }
        return false;
    }

    static class NetworkNode {
        public byte[] ip;
        public int prefix;
        public int pointer;

        /**
         * Constructs a network node for internal use.
         *
         * @param ip The ip address of the node.
         * @param prefix The prefix of the node.
         * @param pointer The node number
         */
        NetworkNode(byte[] ip, int prefix, int pointer) {
            this.ip = ip;
            this.prefix = prefix;
            this.pointer = pointer;
        }
    }

}
