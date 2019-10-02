package com.maxmind.db;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.InetAddress;

/**
 * Record represents the data and metadata associated with a database lookup.
 */
public final class Record {
    private final JsonNode data;
    private final Network network;

    /**
     * Create a new record.
     *
     * @param data the data for the record in the database.
     * @param ipAddress the IP address used in the lookup.
     * @param prefixLength the network prefix length associated with the record in the database.
     */
    public Record( JsonNode data, InetAddress ipAddress, int prefixLength) {
        this.data = data;
        this.network = new Network(ipAddress, prefixLength);
    }

    /**
     * @return the data for the record in the database. The record  will be
     * <code>null</code> if there was no data for the address in the database.
     */
    public JsonNode getData() {
        return data;
    }

    /**
     * @return the network associated with the record in the database. This is
     * the largest network where all of the IPs in the network have the same
     * data.
     */
    public Network getNetwork() {
        return network;
    }
}
