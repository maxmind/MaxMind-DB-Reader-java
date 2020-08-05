package com.maxmind.db;

import java.io.IOException;

/**
 * Signals that the value could not be deserialized into the type.
 */
public class DeserializationException extends IOException {
    private static final long serialVersionUID = 1L;

    DeserializationException() {
        super("Database value cannot be deserialized into the type.");
    }
}
