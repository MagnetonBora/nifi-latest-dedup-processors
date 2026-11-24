package com.example.nifi.dedup;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimestampConverterTest {

    private static final Instant EXPECTED = Instant.parse("2026-11-23T10:00:00Z");

    private final TimestampConverter iso = TimestampConverter.forPattern(null);

    @Test
    void temporalTypes() {
        assertEquals(EXPECTED, iso.toInstant(EXPECTED));
        assertEquals(EXPECTED, iso.toInstant(Timestamp.from(EXPECTED)));
        assertEquals(EXPECTED, iso.toInstant(Date.from(EXPECTED)));
        assertEquals(EXPECTED, iso.toInstant(OffsetDateTime.parse("2026-11-23T12:00:00+02:00")));
        assertEquals(EXPECTED, iso.toInstant(EXPECTED.atZone(ZoneId.of("Europe/Kyiv"))));
        assertEquals(EXPECTED, iso.toInstant(LocalDateTime.parse("2026-11-23T10:00:00")));
    }

    @Test
    void sqlTimestampKeepsNanos() {
        final Instant withNanos = EXPECTED.plusNanos(123_456_789);
        assertEquals(withNanos, iso.toInstant(Timestamp.from(withNanos)));
    }

    @Test
    void integralNumbersAreEpochMillis() {
        final long millis = EXPECTED.toEpochMilli();
        assertEquals(EXPECTED, iso.toInstant(millis));
        assertEquals(EXPECTED, iso.toInstant(BigInteger.valueOf(millis)));
        assertEquals(Instant.ofEpochMilli(5), iso.toInstant(5));
    }

    @Test
    void isoStrings() {
        assertEquals(EXPECTED, iso.toInstant("2026-11-23T10:00:00Z"));
        assertEquals(EXPECTED, iso.toInstant("2026-11-23T12:00:00+02:00"));
        assertEquals(EXPECTED.plusMillis(250), iso.toInstant("2026-11-23T10:00:00.250Z"));
        assertEquals(EXPECTED, TimestampConverter.forPattern("  ").toInstant("2026-11-23T10:00:00Z"));
    }

    @Test
    void customPatternWithoutZoneIsUtc() {
        final TimestampConverter converter = TimestampConverter.forPattern("yyyy-MM-dd HH:mm:ss");
        assertEquals(EXPECTED, converter.toInstant("2026-11-23 10:00:00"));
    }

    @Test
    void customPatternWithOffsetUsesParsedOffset() {
        final TimestampConverter converter = TimestampConverter.forPattern("dd.MM.yyyy HH:mm XXX");
        assertEquals(EXPECTED, converter.toInstant("23.11.2026 12:00 +02:00"));
    }

    @Test
    void invalidValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> iso.toInstant("not-a-date"));
        assertThrows(IllegalArgumentException.class, () -> iso.toInstant("2026-11-23T10:00:00")); // no offset
        assertThrows(IllegalArgumentException.class, () -> iso.toInstant(true));
        assertThrows(IllegalArgumentException.class, () -> iso.toInstant(1.5d));
        assertThrows(IllegalArgumentException.class, () -> iso.toInstant(BigDecimal.ONE));
        assertThrows(IllegalArgumentException.class, () -> iso.toInstant(Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> iso.toInstant(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
        assertThrows(IllegalArgumentException.class,
                () -> TimestampConverter.forPattern("yyyy-MM-dd").toInstant("2026-11-23")); // no time part
    }

    @Test
    void invalidPatternIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> TimestampConverter.forPattern("yyyy-MM-dd'T"));
    }
}
