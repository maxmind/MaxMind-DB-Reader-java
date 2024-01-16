package com.maxmind.db;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Interface for a MaxMind DB parameter. This is used to mark a parameter that
 * should be used to create an instance of a class when decoding a MaxMind DB
 * file.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface MaxMindDbParameter {
    String name();
}
