package com.maxmind.db;

/**
 * This class represents an error encountered while iterating over the networks.
 * The most likely causes are corrupt data in the database, or a bug in the reader code.
 * 
 * This exception extends RuntimeException because it is thrown by the iterator
 * methods in {@link Networks}.
 * 
 * @see Networks
 */
public class NetworksIterationException extends RuntimeException { 
    NetworksIterationException(String message) {
        super(message);
    }

    NetworksIterationException(Throwable cause) {
        super(cause);
    }
}
