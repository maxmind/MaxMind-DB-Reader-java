package com.maxmind.db;

import java.net.InetAddress;

/**
 * DatabaseRecord represents the data and metadata associated with a database
 * lookup.
 *
 * @param <T> the type to deserialize the returned value to
 * @param data the data for the record in the database. The record will be
 *             {@code null} if there was no data for the address in the
 *             database.
 * @param network the network associated with the record in the database. This is
 *                the largest network where all of the IPs in the network have the same
 *                data.
 */
public record DatabaseRecord<T>(T data, Network network) {
    /**
     * Create a new record.
     *
     * @param data         the data for the record in the database.
     * @param ipAddress    the IP address used in the lookup.
     * @param prefixLength the network prefix length associated with the record in the database.
     */
    public DatabaseRecord(T data, InetAddress ipAddress, int prefixLength) {
        this(data, new Network(ipAddress, prefixLength));
    }
}
