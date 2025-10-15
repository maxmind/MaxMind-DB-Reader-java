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
    /**
     * @return the name of the parameter in the MaxMind DB file
     */
    String name();

    /**
     * Whether to use a default when the value is missing in the database.
     */
    boolean useDefault() default false;

    /**
     * The default value as a string. Parsed according to the Java parameter
     * type (e.g., "0", "false"). If empty and {@code useDefault} is true,
     * the Java type's default is used (0, false, 0.0, and "" for String).
     */
    String defaultValue() default "";
}
