package com.maxmind.db;

/**
 * Signals that the value could not be deserialized into the type.
 */
public class DeserializationException extends RuntimeException {
    DeserializationException() {
        super("Database value cannot be deserialized into the type.");
    }

    DeserializationException(String message) {
        super(message);
    }

    DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
