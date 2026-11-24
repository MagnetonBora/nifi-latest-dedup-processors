package com.example.nifi.dedup;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Converts a record field value into an {@link Instant}.
 * <p>
 * Immutable and thread-safe: a single instance is created per processor schedule
 * and shared by all concurrent {@code onTrigger} invocations.
 * <ul>
 *     <li>{@link Instant}, {@link java.sql.Timestamp}, {@link Date}, {@link OffsetDateTime},
 *     {@link ZonedDateTime} are converted directly;</li>
 *     <li>{@link LocalDateTime} is interpreted as UTC;</li>
 *     <li>integral numbers are interpreted as epoch milliseconds;</li>
 *     <li>strings are parsed with the configured pattern (ISO-8601 with offset by default);
 *     a pattern without zone/offset is interpreted as UTC.</li>
 * </ul>
 */
final class TimestampConverter {

    private final DateTimeFormatter formatter;
    private final String formatDescription;

    private TimestampConverter(final DateTimeFormatter formatter, final String formatDescription) {
        this.formatter = formatter;
        this.formatDescription = formatDescription;
    }

    /**
     * @param pattern {@link DateTimeFormatter} pattern, or {@code null}/blank for ISO-8601 with offset
     * @throws IllegalArgumentException if the pattern is invalid
     */
    static TimestampConverter forPattern(final String pattern) {
        if (pattern == null || pattern.isBlank()) {
            return new TimestampConverter(DateTimeFormatter.ISO_OFFSET_DATE_TIME, "ISO-8601");
        }
        return new TimestampConverter(DateTimeFormatter.ofPattern(pattern).withZone(ZoneOffset.UTC), pattern);
    }

    /**
     * @param value non-null field value
     * @throws IllegalArgumentException if the value has an unsupported type or cannot be parsed
     */
    Instant toInstant(final Object value) {
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof Date date) {
            // java.sql.Timestamp overrides toInstant() and keeps nanoseconds
            return date.toInstant();
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toInstant();
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.toInstant(ZoneOffset.UTC);
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return Instant.ofEpochMilli(((Number) value).longValue());
        }
        if (value instanceof BigInteger bigInteger) {
            try {
                return Instant.ofEpochMilli(bigInteger.longValueExact());
            } catch (final ArithmeticException e) {
                throw new IllegalArgumentException("numeric timestamp " + value + " is out of range", e);
            }
        }
        if (value instanceof CharSequence text) {
            try {
                return formatter.parse(text, Instant::from);
            } catch (final DateTimeParseException e) {
                throw new IllegalArgumentException(
                        "value '" + text + "' does not match timestamp format " + formatDescription, e);
            }
        }
        throw new IllegalArgumentException("unsupported timestamp type " + value.getClass().getName());
    }
}
