package com.maxmind.db;

/**
 * {@code DecodedValue} is a wrapper for the decoded value and the number of bytes used
 * to decode it.
 *
 * @param value the decoded value
 */
record DecodedValue(Object value) {}
