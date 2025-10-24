package com.maxmind.db;

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
public record DatabaseRecord<T>(T data, Network network) {}
