package com.example.nifi.dedup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatestRecordDeduplicatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TestRunner runner;
    private JsonTreeReader reader;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(LatestRecordDeduplicator.class);

        reader = new JsonTreeReader();
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, JsonRecordSetWriter.OUTPUT_GROUPING, JsonRecordSetWriter.OUTPUT_ONELINE);
        runner.enableControllerService(writer);

        runner.setProperty(LatestRecordDeduplicator.RECORD_READER, "reader");
        runner.setProperty(LatestRecordDeduplicator.RECORD_WRITER, "writer");
        runner.setProperty(LatestRecordDeduplicator.PARTITION_BY, "/id");
        runner.setProperty(LatestRecordDeduplicator.TIMESTAMP_FIELD, "/timestamp");
    }

    // 1
    @Test
    void keepsLatestRecordPerKey() {
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"Alice\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 2, \"name\": \"Bob\", \"timestamp\": \"2026-11-23T10:05:00Z\"}",
                "{\"id\": 1, \"name\": \"Alice Updated\", \"timestamp\": \"2026-11-23T11:00:00Z\"}",
                "{\"id\": 3, \"name\": \"Charlie\", \"timestamp\": \"2026-11-23T12:00:00Z\"}",
                "{\"id\": 2, \"name\": \"Bob Updated\", \"timestamp\": \"2026-11-23T09:00:00Z\"}"));
        runner.run();

        final MockFlowFile out = assertSingleSuccess();
        assertRecords(out,
                "{\"id\": 1, \"name\": \"Alice Updated\", \"timestamp\": \"2026-11-23T11:00:00Z\"}",
                "{\"id\": 2, \"name\": \"Bob\", \"timestamp\": \"2026-11-23T10:05:00Z\"}",
                "{\"id\": 3, \"name\": \"Charlie\", \"timestamp\": \"2026-11-23T12:00:00Z\"}");
        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "application/json");
    }

    // 2
    @Test
    void allKeysUniqueKeepsAllRecordsInOrder() {
        final String[] input = {
                "{\"id\": 4, \"name\": \"D\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 2, \"name\": \"B\", \"timestamp\": \"2026-11-23T09:00:00Z\"}",
                "{\"id\": 3, \"name\": \"C\", \"timestamp\": \"2026-11-23T08:00:00Z\"}",
                "{\"id\": 1, \"name\": \"A\", \"timestamp\": \"2026-11-23T07:00:00Z\"}"};
        runner.enqueue(lines(input));
        runner.run();

        final MockFlowFile out = assertSingleSuccess();
        assertRecords(out, input);
        out.assertAttributeEquals("record.count", "4");
    }

    // 3
    @Test
    void sameKeyForAllRecordsKeepsOnlyLatest() {
        runner.enqueue(lines(
                "{\"id\": 7, \"name\": \"v1\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 7, \"name\": \"v2\", \"timestamp\": \"2026-11-23T11:00:00Z\"}",
                "{\"id\": 7, \"name\": \"v3\", \"timestamp\": \"2026-11-23T15:00:00Z\"}",
                "{\"id\": 7, \"name\": \"v4\", \"timestamp\": \"2026-11-23T12:00:00Z\"}",
                "{\"id\": 7, \"name\": \"v5\", \"timestamp\": \"2026-11-23T09:00:00Z\"}"));
        runner.run();

        final MockFlowFile out = assertSingleSuccess();
        assertRecords(out, "{\"id\": 7, \"name\": \"v3\", \"timestamp\": \"2026-11-23T15:00:00Z\"}");
        out.assertAttributeEquals("record.count", "1");
    }

    // 4
    @Test
    void equalTimestampsKeepFirstRecord() {
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"first\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 1, \"name\": \"second\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 1, \"name\": \"third\", \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
        runner.run();

        assertRecords(assertSingleSuccess(),
                "{\"id\": 1, \"name\": \"first\", \"timestamp\": \"2026-11-23T10:00:00Z\"}");
    }

    // 5
    @Test
    void missingKeyRoutesToFailure() {
        assertFailureUnchanged(lines(
                "{\"id\": 1, \"name\": \"ok\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"name\": \"no id\", \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
    }

    // 6
    @Test
    void nullKeyRoutesToFailure() {
        assertFailureUnchanged(lines("{\"id\": null, \"name\": \"x\", \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
    }

    // 7
    @Test
    void missingTimestampRoutesToFailure() {
        assertFailureUnchanged(lines("{\"id\": 1, \"name\": \"x\"}"));
    }

    @Test
    void nullTimestampRoutesToFailure() {
        assertFailureUnchanged(lines("{\"id\": 1, \"name\": \"x\", \"timestamp\": null}"));
    }

    // 8
    @Test
    void invalidTimestampRoutesToFailure() {
        assertFailureUnchanged(lines("{\"id\": 1, \"name\": \"x\", \"timestamp\": \"not-a-date\"}"));
    }

    // 9
    @Test
    void emptyFlowFileProducesEmptyResult() {
        runner.enqueue(new byte[0]);
        runner.run();

        final MockFlowFile out = assertSingleSuccess();
        out.assertContentEquals("");
        out.assertAttributeEquals("record.count", "0");
    }

    // 10
    @Test
    void singleRecordIsReturnedUnchanged() {
        final String record = "{\"id\": 1, \"name\": \"Alice\", \"timestamp\": \"2026-11-23T10:00:00Z\"}";
        runner.enqueue(record);
        runner.run();

        final MockFlowFile out = assertSingleSuccess();
        assertRecords(out, record);
        out.assertAttributeEquals("record.count", "1");
    }

    // 11
    @Test
    void flowFilesAreProcessedIndependently() {
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"ff1-new\", \"timestamp\": \"2026-11-23T11:00:00Z\"}",
                "{\"id\": 1, \"name\": \"ff1-old\", \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
        // Older timestamp than in the first FlowFile: must still win inside its own FlowFile.
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"ff2-only\", \"timestamp\": \"2020-01-01T00:00:00Z\"}"));
        runner.run(2);

        runner.assertAllFlowFilesTransferred(LatestRecordDeduplicator.REL_SUCCESS, 2);
        final List<MockFlowFile> out = runner.getFlowFilesForRelationship(LatestRecordDeduplicator.REL_SUCCESS);
        assertRecords(out.get(0), "{\"id\": 1, \"name\": \"ff1-new\", \"timestamp\": \"2026-11-23T11:00:00Z\"}");
        assertRecords(out.get(1), "{\"id\": 1, \"name\": \"ff2-only\", \"timestamp\": \"2020-01-01T00:00:00Z\"}");
    }

    @Test
    void concurrentFlowFilesDoNotShareState() {
        final int flowFiles = 50;
        for (int f = 0; f < flowFiles; f++) {
            final List<String> records = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                records.add(record(k, "ff" + f + "-k" + k + "-old", Instant.parse("2026-11-23T10:00:00Z").plusSeconds(f)));
                records.add(record(k, "ff" + f + "-k" + k + "-new", Instant.parse("2026-11-23T11:00:00Z").plusSeconds(f)));
            }
            runner.enqueue(lines(records.toArray(String[]::new)), Map.of("ff.index", String.valueOf(f)));
        }
        runner.setThreadCount(4);
        runner.run(flowFiles);

        runner.assertAllFlowFilesTransferred(LatestRecordDeduplicator.REL_SUCCESS, flowFiles);
        for (final MockFlowFile out : runner.getFlowFilesForRelationship(LatestRecordDeduplicator.REL_SUCCESS)) {
            final String f = out.getAttribute("ff.index");
            final List<JsonNode> records = parse(out.getContent());
            assertEquals(10, records.size());
            for (int k = 0; k < 10; k++) {
                assertEquals(k, records.get(k).get("id").asInt());
                assertEquals("ff" + f + "-k" + k + "-new", records.get(k).get("name").asText());
            }
        }
    }

    // 12
    @Test
    void largeInput() {
        final int recordCount = 200_000;
        final int keyCount = 1_000;
        final Random random = new Random(42);
        final Instant base = Instant.parse("2026-01-01T00:00:00Z");

        final StringBuilder content = new StringBuilder(recordCount * 80);
        final Map<Integer, Instant> expectedTs = new LinkedHashMap<>();
        final Map<Integer, String> expectedName = new HashMap<>();
        for (int i = 0; i < recordCount; i++) {
            final int key = random.nextInt(keyCount);
            final Instant ts = base.plusSeconds(random.nextInt(100_000)); // ties are possible
            final String name = "r" + i;
            content.append(record(key, name, ts)).append('\n');

            final Instant current = expectedTs.get(key);
            if (current == null || ts.isAfter(current)) {
                expectedTs.put(key, ts);
                expectedName.put(key, name);
            }
        }

        runner.enqueue(content.toString());
        runner.run();

        final MockFlowFile out = assertSingleSuccess();
        out.assertAttributeEquals("record.count", String.valueOf(expectedTs.size()));
        final List<JsonNode> records = parse(out.getContent());
        assertEquals(expectedTs.size(), records.size());

        final List<Integer> expectedOrder = new ArrayList<>(expectedTs.keySet());
        for (int i = 0; i < records.size(); i++) {
            final JsonNode node = records.get(i);
            final int key = node.get("id").asInt();
            assertEquals(expectedOrder.get(i), key, "output order must follow first appearance of key");
            assertEquals(expectedName.get(key), node.get("name").asText());
            assertEquals(expectedTs.get(key).toString(), node.get("timestamp").asText());
        }
    }

    // --- Additional scenarios covering design decisions ---

    @Test
    void customTimestampFormatWithoutZoneIsUtc() {
        runner.setProperty(LatestRecordDeduplicator.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss");
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"old\", \"timestamp\": \"2026-11-23 10:00:00\"}",
                "{\"id\": 1, \"name\": \"new\", \"timestamp\": \"2026-11-23 10:00:01\"}"));
        runner.run();

        assertRecords(assertSingleSuccess(), "{\"id\": 1, \"name\": \"new\", \"timestamp\": \"2026-11-23 10:00:01\"}");
    }

    @Test
    void isoTimestampsAreComparedAsInstantsNotStrings() {
        // "10:00+02:00" is 08:00Z: lexicographically larger, but earlier in time.
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"earlier\", \"timestamp\": \"2026-11-23T10:00:00+02:00\"}",
                "{\"id\": 1, \"name\": \"later\", \"timestamp\": \"2026-11-23T09:00:00Z\"}"));
        runner.run();

        assertEquals("later", parse(assertSingleSuccess().getContent()).getFirst().get("name").asText());
    }

    @Test
    void numericTimestampIsEpochMillis() {
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"new\", \"timestamp\": 1700000000001}",
                "{\"id\": 1, \"name\": \"old\", \"timestamp\": 1700000000000}"));
        runner.run();

        assertRecords(assertSingleSuccess(), "{\"id\": 1, \"name\": \"new\", \"timestamp\": 1700000000001}");
    }

    @Test
    void timestampTypedByReader() {
        runner.disableControllerService(reader);
        runner.setProperty(reader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd'T'HH:mm:ssXXX");
        runner.enableControllerService(reader);

        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"earlier\", \"timestamp\": \"2026-11-23T10:00:00+02:00\"}",
                "{\"id\": 1, \"name\": \"later\", \"timestamp\": \"2026-11-23T09:00:00+00:00\"}",
                "{\"id\": 2, \"name\": \"only\", \"timestamp\": \"2026-11-23T09:00:00+00:00\"}"));
        runner.run();

        final List<JsonNode> records = parse(assertSingleSuccess().getContent());
        assertEquals(2, records.size());
        assertEquals("later", records.get(0).get("name").asText());
        assertEquals("only", records.get(1).get("name").asText());
    }

    @Test
    void stringAndNumericKeysWithSameTextAreSameKey() {
        runner.enqueue(lines(
                "{\"id\": 1, \"name\": \"numeric\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": \"1\", \"name\": \"string\", \"timestamp\": \"2026-11-23T11:00:00Z\"}"));
        runner.run();

        final List<JsonNode> records = parse(assertSingleSuccess().getContent());
        assertEquals(1, records.size());
        assertEquals("string", records.getFirst().get("name").asText());
    }

    @Test
    void nestedKeyByRecordPath() {
        runner.setProperty(LatestRecordDeduplicator.PARTITION_BY, "/customer/id");
        runner.enqueue(lines(
                "{\"customer\": {\"id\": \"a\"}, \"name\": \"old\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"customer\": {\"id\": \"a\"}, \"name\": \"new\", \"timestamp\": \"2026-11-23T11:00:00Z\"}"));
        runner.run();

        assertEquals("new", parse(assertSingleSuccess().getContent()).getFirst().get("name").asText());
    }

    @Test
    void nonScalarKeyRoutesToFailure() {
        runner.setProperty(LatestRecordDeduplicator.PARTITION_BY, "/customer");
        assertFailureUnchanged(lines(
                "{\"customer\": {\"id\": \"a\"}, \"name\": \"x\", \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
    }

    @Test
    void recordPathSelectingMultipleValuesRoutesToFailure() {
        runner.setProperty(LatestRecordDeduplicator.PARTITION_BY, "/tags[*]");
        assertFailureUnchanged(lines(
                "{\"id\": 1, \"tags\": [\"a\", \"b\"], \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
    }

    @Test
    void errorAfterValidRecordsProducesNoPartialResult() {
        assertFailureUnchanged(lines(
                "{\"id\": 1, \"name\": \"a\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 2, \"name\": \"b\", \"timestamp\": \"2026-11-23T10:00:00Z\"}",
                "{\"id\": 3, \"name\": \"c\", \"timestamp\": \"bad\"}",
                "{\"id\": 4, \"name\": \"d\", \"timestamp\": \"2026-11-23T10:00:00Z\"}"));
    }

    @Test
    void malformedJsonRoutesToFailure() {
        assertFailureUnchanged("{\"id\": 1, \"timestamp\": ");
    }

    @Test
    void invalidConfigurationIsRejected() {
        runner.setProperty(LatestRecordDeduplicator.TIMESTAMP_FORMAT, "yyyy-MM-dd'T");
        runner.assertNotValid();

        runner.removeProperty(LatestRecordDeduplicator.TIMESTAMP_FORMAT);
        runner.assertValid();

        runner.setProperty(LatestRecordDeduplicator.PARTITION_BY, "/id[");
        runner.assertNotValid();

        runner.removeProperty(LatestRecordDeduplicator.PARTITION_BY);
        runner.assertNotValid();
    }

    // --- helpers ---

    private static String record(final int id, final String name, final Instant ts) {
        return "{\"id\": " + id + ", \"name\": \"" + name + "\", \"timestamp\": \"" + ts + "\"}";
    }

    private static String lines(final String... lines) {
        return String.join("\n", lines) + "\n";
    }

    private MockFlowFile assertSingleSuccess() {
        runner.assertAllFlowFilesTransferred(LatestRecordDeduplicator.REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(LatestRecordDeduplicator.REL_SUCCESS).getFirst();
    }

    private void assertFailureUnchanged(final String input) {
        runner.enqueue(input);
        runner.run();

        runner.assertAllFlowFilesTransferred(LatestRecordDeduplicator.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(LatestRecordDeduplicator.REL_FAILURE).getFirst();
        failed.assertContentEquals(input);
        failed.assertAttributeNotExists("record.count");
        assertTrue(runner.getLogger().getErrorMessages().size() > 0, "failure must be logged");
    }

    private static void assertRecords(final MockFlowFile flowFile, final String... expected) {
        final List<JsonNode> actual = parse(flowFile.getContent());
        final List<JsonNode> expectedNodes = parse(String.join("\n", expected));
        assertEquals(expectedNodes, actual);
    }

    private static List<JsonNode> parse(final String content) {
        return content.lines()
                .filter(line -> !line.isBlank())
                .map(line -> {
                    try {
                        return MAPPER.readTree(line);
                    } catch (final Exception e) {
                        throw new AssertionError("Invalid JSON line: " + line, e);
                    }
                })
                .toList();
    }
}
