package com.maxmind.db;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code MaxMindDbCreator} is an annotation that can be used to mark a static factory
 * method or constructor that should be used to create an instance of a class from a
 * decoded value when decoding a MaxMind DB file.
 *
 * <p>This is similar to Jackson's {@code @JsonCreator} annotation and is useful for
 * types that need custom deserialization logic, such as enums with non-standard
 * string representations.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * public enum ConnectionType {
 *     DIALUP("Dialup"),
 *     CABLE_DSL("Cable/DSL");
 *
 *     private final String name;
 *
 *     ConnectionType(String name) {
 *         this.name = name;
 *     }
 *
 *     {@literal @}MaxMindDbCreator
 *     public static ConnectionType fromString(String s) {
 *         return switch (s) {
 *             case "Dialup" -&gt; DIALUP;
 *             case "Cable/DSL" -&gt; CABLE_DSL;
 *             default -&gt; null;
 *         };
 *     }
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR})
public @interface MaxMindDbCreator {
}
