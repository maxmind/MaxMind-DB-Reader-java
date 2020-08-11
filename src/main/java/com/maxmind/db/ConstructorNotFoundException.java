package com.maxmind.db;

import java.io.IOException;

/**
 * Signals that no annotated constructor was found. You should annotate a
 * constructor in the class with the MaxMindDbConstructor annotation.
 */
public class ConstructorNotFoundException extends IOException {
    private static final long serialVersionUID = 1L;

    ConstructorNotFoundException(String message) {
        super(message);
    }
}
