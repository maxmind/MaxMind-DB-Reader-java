package com.maxmind.db;

/**
 * Signals that no annotated parameter was found. You should annotate
 * parameters of the constructor class with the MaxMindDbParameter annotation.
 */
public class ParameterNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ParameterNotFoundException(String message) {
        super(message);
    }
}
