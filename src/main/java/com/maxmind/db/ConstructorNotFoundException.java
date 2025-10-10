package com.maxmind.db;

/**
 * Signals that no annotated constructor was found. You should annotate a
 * constructor in the class with the MaxMindDbConstructor annotation.
 */
public class ConstructorNotFoundException extends RuntimeException {
    ConstructorNotFoundException(String message) {
        super(message);
    }
}
