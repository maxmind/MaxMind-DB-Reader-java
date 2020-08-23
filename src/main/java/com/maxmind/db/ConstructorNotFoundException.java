package com.maxmind.db;

/**
 * Signals that no annotated constructor was found. You should annotate a
 * constructor in the class with the MaxMindDbConstructor annotation.
 */
public class ConstructorNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ConstructorNotFoundException(String message) {
        super(message);
    }
}
