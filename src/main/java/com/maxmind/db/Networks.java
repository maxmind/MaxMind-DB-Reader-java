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
public class Networks<T> implements Iterator<DatabaseRecord<T>> {
    private final Reader reader;
    private final Stack<NetworkNode> nodes;
    private NetworkNode lastNode;
    private final boolean skipAliasedNetworks;
    private final ByteBuffer buffer; /* Stores the buffer for Next() calls */
    private Class<T> typeParameterClass;
    
    /**
     * Constructs a Networks instance.
     * @param reader The reader object.
     * @param skipAliasedNetworks The boolean to skip aliased networks.
     * @throws ClosedDatabaseException Exception for a closed database.
     */
    Networks(Reader reader, boolean skipAliasedNetworks) 
        throws ClosedDatabaseException {
        this(reader, skipAliasedNetworks, new NetworkNode[]{});
    }

    /**
     * Constructs a Networks instance.
     * @param reader The reader object.
     * @param skipAliasedNetworks The boolean to skip aliased networks.
     * @param nodes The initial nodes array to start Networks iterator with.
     * @throws ClosedDatabaseException Exception for a closed database.
     */
    Networks(Reader reader, boolean skipAliasedNetworks, NetworkNode[] nodes) 
        throws ClosedDatabaseException {
        this.reader = reader;
        this.skipAliasedNetworks = skipAliasedNetworks;
        this.buffer = reader.getBufferHolder().get();
        this.nodes = new Stack<NetworkNode>();
        for (NetworkNode node : nodes) {
            this.nodes.push(node);
        }
    }

    /**
     * Constructs a Networks instance with skipAliasedNetworks set to false by default.
     * @param reader The reader object.
     */
    Networks(Reader reader) throws ClosedDatabaseException {
        this(reader, false);
    }

    /**
     * Sets the Class for the data type in DataRecord.
     * @param cls The class object. ( For example, Map.class )
     */
    public void setDataClass(Class<T> cls) {
        this.typeParameterClass = cls;
    }

    /**
     * Returns the next DataRecord. You need to set the class using
     * prepareForClass before calling next. 
     * For example,
     *  networks.prepareForClass(Map.Class);
     *  Map test = networks.next();
     */
    @Override
    public DatabaseRecord<T> next() {
        try {
            T data = this.reader.resolveDataPointer(
                this.buffer, this.lastNode.pointer, this.typeParameterClass);

            byte[] ip = this.lastNode.ip;
            int prefixLength = this.lastNode.prefix;

            // We do this because uses of SkipAliasedNetworks expect the IPv4 networks
            // to be returned as IPv4 networks. If we are not skipping aliased
            // networks, then the user will get IPv4 networks from the ::FFFF:0:0/96
            // network.
            if (this.skipAliasedNetworks && isInIpv4Subtree(ip)) {
                ip = Arrays.copyOfRange(ip, 12, ip.length);
                prefixLength -= 96;
            }

            // If the ip is in ipv6 form, drop the prefix manually
            // as InetAddress converts it to ipv4.
            InetAddress ipAddr = InetAddress.getByAddress(ip);
            if (ipAddr instanceof Inet4Address && ip.length > 4 && prefixLength > 96) {
                prefixLength -= 96;
            }

            return new DatabaseRecord<T>(data, InetAddress.getByAddress(ip), prefixLength);
        } catch (IOException e) {
            throw new NetworksIterationException(e.getMessage());
        }
    }

    public boolean isInIpv4Subtree(byte[] ip) {
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
    
    /*
    * Next prepares the next network for reading with the Network method. It
    * returns true if there is another network to be processed and false if there
    * are no more networks or if there is an error.
    */
    @Override
    public boolean hasNext()  {
        while (!this.nodes.isEmpty()) {
            NetworkNode node = this.nodes.pop();

            // Next until we don't have data.
            while (node.pointer != this.reader.getMetadata().getNodeCount()) {
                // This skips IPv4 aliases without hardcoding the networks that the writer
                // currently aliases.
                if (this.skipAliasedNetworks && this.reader.getIpv4Start() != 0
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
                    throw new NetworksIterationException(e.getMessage());
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
         * @param ip The ip address of the node.
         * @param prefix The prefix of the node.
         * @param pointer The node number
         */
        public NetworkNode(byte[] ip, int prefix, int pointer) {
            this.ip = ip;
            this.prefix = prefix;
            this.pointer = pointer;
        }
    }

}
