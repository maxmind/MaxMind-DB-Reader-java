package com.maxmind.db;

import java.io.IOException;

/**
 * Signals that no annotated parameter was found. You should annotate
 * parameters of the constructor class with the MaxMindDbParameter annotation.
 */
public class ParameterNotFoundException extends IOException {
    private static final long serialVersionUID = 1L;

    ParameterNotFoundException(String message) {
        super(message);
    }
}
