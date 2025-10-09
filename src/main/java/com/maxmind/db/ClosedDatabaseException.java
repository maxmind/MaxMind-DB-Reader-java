package com.maxmind.db;

import java.io.IOException;

/**
 * Signals that the underlying database has been closed.
 */
public class ClosedDatabaseException extends IOException {
    ClosedDatabaseException() {
        super("The MaxMind DB has been closed.");
    }
}
