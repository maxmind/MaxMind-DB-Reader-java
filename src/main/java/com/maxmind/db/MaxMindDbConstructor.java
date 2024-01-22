package com.maxmind.db;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * {@code MaxMindDbConstructor} is an annotation that can be used to mark a constructor
 * that should be used to create an instance of a class when decoding a MaxMind
 * DB file.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface MaxMindDbConstructor {
}
