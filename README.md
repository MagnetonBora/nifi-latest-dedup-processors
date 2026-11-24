# LatestRecordDeduplicator: a custom Apache NiFi 2.12 processor

A custom NiFi processor that removes duplicate records inside a single FlowFile. For every key it keeps only the record with the **latest timestamp**.

Input (NDJSON):

```json
{"id": 1, "name": "Alice",         "timestamp": "2026-11-23T10:00:00Z"}
{"id": 2, "name": "Bob",           "timestamp": "2026-11-23T10:05:00Z"}
{"id": 1, "name": "Alice Updated", "timestamp": "2026-11-23T11:00:00Z"}
{"id": 3, "name": "Charlie",       "timestamp": "2026-11-23T12:00:00Z"}
{"id": 2, "name": "Bob Updated",   "timestamp": "2026-11-23T09:00:00Z"}
```

Output:

```json
{"id": 1, "name": "Alice Updated", "timestamp": "2026-11-23T11:00:00Z"}
{"id": 2, "name": "Bob",           "timestamp": "2026-11-23T10:05:00Z"}
{"id": 3, "name": "Charlie",       "timestamp": "2026-11-23T12:00:00Z"}
```

## What was built

| Item | Description |
|---|---|
| [`LatestRecordDeduplicator`](nifi-latest-dedup-processors/src/main/java/com/example/nifi/dedup/LatestRecordDeduplicator.java) | The processor. Built on the NiFi Record API, so it works with any Record Reader and Record Writer (JSON, CSV, Avro, …). |
| [`TimestampConverter`](nifi-latest-dedup-processors/src/main/java/com/example/nifi/dedup/TimestampConverter.java) | Converts a field value (timestamp type, epoch millis or formatted string) into an `Instant`. |
| [Tests](nifi-latest-dedup-processors/src/test/java/com/example/nifi/dedup/) | 33 tests built on NiFi `TestRunner` with the real `JsonTreeReader` and `JsonRecordSetWriter`. They cover duplicates, edge cases, errors, 200k records and concurrent execution. |
| [`flows/latest-record-deduplicator-demo.json`](flows/latest-record-deduplicator-demo.json) | A demo flow that you can import into NiFi. |
| [`docs/TECHNICAL_SPECIFICATION.md`](docs/TECHNICAL_SPECIFICATION.md) | The full design document (in Russian): architecture, algorithm and design decisions. |

### How it works

1. **Read phase.** Records are streamed through the Record Reader. For each record the processor reads the key and the timestamp. A `LinkedHashMap<key, record>` keeps the latest record per key. The algorithm runs in O(n) time. Memory usage grows with the number of *distinct* keys, not with the total number of records, and the FlowFile content is never loaded into memory as a whole.
2. **Write phase.** The surviving records are written through the Record Writer, replacing the FlowFile content.

Because nothing is written until every record has been read, a bad record never produces a partial result. The whole FlowFile goes to `failure` unchanged.

### Behaviour

| Situation | Result |
|---|---|
| Several records with the same key | the one with the latest timestamp is kept |
| Same key **and** same timestamp | the **first** record encountered is kept |
| Output order | order in which each key first appeared |
| Empty FlowFile | `success`, empty content, `record.count=0` |
| Missing or `null` key or timestamp, unparseable timestamp, malformed input | `failure`, original FlowFile unchanged, error logged as a bulletin |
| Keys `1` and `"1"` | treated as the same key (keys are compared by their string form) |

Each FlowFile is processed independently. No state is shared between FlowFiles or threads, so the processor is safe to run with several Concurrent Tasks.

### Properties

| Property | Required | Example | Notes |
|---|---|---|---|
| Record Reader | yes | `JsonTreeReader` | any `RecordReaderFactory` |
| Record Writer | yes | `JsonRecordSetWriter` | any `RecordSetWriterFactory`; for NDJSON output set *Output Grouping = One Line Per Object* |
| Partition By | yes | `/id` | RecordPath to the key; must select exactly one scalar, non-null value |
| Timestamp Field | yes | `/timestamp` | RecordPath to the timestamp |
| Timestamp Format | no | `yyyy-MM-dd HH:mm:ss` | `java.time` pattern for **string** timestamps. Empty means ISO-8601 with offset (`2026-11-23T10:00:00Z`). A pattern without a zone is read as UTC. |

Supported timestamp values: timestamp or date types produced by the reader, integers (read as epoch milliseconds) and strings.

### Relationships and attributes

- `success`: the same FlowFile with deduplicated content. Sets `record.count` (number of output records) and `mime.type` (from the writer).
- `failure`: the original FlowFile, content and attributes untouched.

## Building

The result is a NAR file, the NiFi plugin archive.

### Option 1: in Docker (no Java needed)

Requirements: Docker and `make`.

```bash
make
```

The NAR is written to `dist/nifi-latest-dedup-nar-1.0.0-SNAPSHOT.nar`.

The build runs inside `eclipse-temurin:21-jdk` and uses the bundled Maven Wrapper. Maven dependencies are cached in `./.cache`. The first build takes a few minutes; later builds take about 15 seconds.

| Command | What it does |
|---|---|
| `make` | build and test, then copy the NAR to `dist/` |
| `make test` | run tests only |
| `make MVN_ARGS=-DskipTests` | build without tests |
| `make clean` | remove build output |
| `make distclean` | also remove the Maven cache |

### Option 2: locally, without Docker

Requirements: **JDK 21 or newer**. You don't need to install Maven: the wrapper `./mvnw` downloads Maven 3.9.16, which is the minimum version the NiFi 2.12 parent POM accepts.

```bash
export JAVA_HOME=/path/to/jdk-21   # e.g. /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
./mvnw clean install
```

The NAR is written to `nifi-latest-dedup-nar/target/nifi-latest-dedup-nar-1.0.0-SNAPSHOT.nar`.

> The build fails early if `JAVA_HOME` points to JDK 17 or if you run a system `mvn` older than 3.9.16. Both checks come from the NiFi parent POM. Use `./mvnw` and JDK 21+.

## Installing into NiFi

NiFi 2.x loads NARs from its auto-load directory **without a restart**.

### NiFi running locally (from the binary distribution)

```bash
cp dist/nifi-latest-dedup-nar-*.nar $NIFI_HOME/extensions/
```

### NiFi running in Docker (`apache/nifi:2.12.0`)

The official image uses `/opt/nifi/nifi-current/nar_extensions` as the auto-load directory, not `extensions`.

```bash
docker cp dist/nifi-latest-dedup-nar-1.0.0-SNAPSHOT.nar <container>:/opt/nifi/nifi-current/nar_extensions/
```

After about 30 seconds the processor shows up in **Add Processor** as `LatestRecordDeduplicator` (search for "dedup").

To install a new build, replace the NAR file. If the UI keeps showing the old version, restart NiFi.

## Demo flow

[`flows/latest-record-deduplicator-demo.json`](flows/latest-record-deduplicator-demo.json) is a process group that was exported from NiFi 2.12.0 and tested there.

```
 Generate NDJSON with duplicates ──▶ Keep latest record per id ──success──▶ Log deduplicated output
 (GenerateFlowFile, every 30 s)      (LatestRecordDeduplicator)             (LogAttribute, info)
                                                │
                                                └──failure──▶ Log failed input
                                                              (LogAttribute, warn)
```

GenerateFlowFile emits 7 records with 3 ids. The records cover the interesting cases: a newer update, an older (stale) update and a duplicate with the same timestamp:

```json
{"id": 1, "name": "Alice",   "status": "created",          "timestamp": "2026-11-23T10:00:00Z"}
{"id": 2, "name": "Bob",     "status": "created",          "timestamp": "2026-11-23T10:05:00Z"}
{"id": 1, "name": "Alice",   "status": "updated",          "timestamp": "2026-11-23T11:00:00Z"}
{"id": 3, "name": "Charlie", "status": "created",          "timestamp": "2026-11-23T12:00:00Z"}
{"id": 2, "name": "Bob",     "status": "stale (older)",    "timestamp": "2026-11-23T09:00:00Z"}
{"id": 1, "name": "Alice",   "status": "final (latest)",   "timestamp": "2026-11-23T12:30:00Z"}
{"id": 3, "name": "Charlie", "status": "same ts, dropped", "timestamp": "2026-11-23T12:00:00Z"}
```

`mime.type` is `application/json` everywhere. GenerateFlowFile sets it on the input, and the deduplicator copies it from `JsonRecordSetWriter`.

`Log deduplicated output` writes the result to `logs/nifi-app.log` with the prefix `dedup-demo`:

```
--------------------dedup-demo--------------------
FlowFile Attribute Map Content
Key: 'filename'
	Value: 'b5f4aa2c-a93b-41e7-b184-177416334cd6'
Key: 'mime.type'
	Value: 'application/json'
Key: 'record.count'
	Value: '3'
--------------------dedup-demo--------------------
{"id":1,"name":"Alice","status":"final (latest)","timestamp":"2026-11-23T12:30:00Z"}
{"id":2,"name":"Bob","status":"created","timestamp":"2026-11-23T10:05:00Z"}
{"id":3,"name":"Charlie","status":"created","timestamp":"2026-11-23T12:00:00Z"}
```

If a FlowFile fails, `Log failed input` logs the **original, unchanged** content at WARN level with the prefix `dedup-demo-failure`. The reason is logged separately as an ERROR by the deduplicator itself and also shows up as a bulletin in the UI. For example, with a `null` id in the second record:

```
ERROR ... LatestRecordDeduplicator ... Failed to deduplicate records of ... Record 2: field selected by Partition By [/id] is missing or null
WARN  ... LogAttribute ...
----------------dedup-demo-failure----------------
{"id": 1, "name": "ok", "timestamp": "2026-11-23T10:00:00Z"}
{"id": null, "name": "broken", "timestamp": "2026-11-23T11:00:00Z"}
```

The sample data contains no invalid records. To try the failure path, edit **Custom Text** in the generator.

Both loggers auto-terminate `success`, so FlowFiles are dropped once they have been logged.

### Importing and running it

1. Install the NAR first (see above). Otherwise the processor appears as a "ghost" component.
2. In the NiFi UI, drag a **Process Group** onto the canvas. Click the **upload** icon next to the name field, choose `flows/latest-record-deduplicator-demo.json`, then click **Add**.
3. Open the group. Right-click on the canvas, choose **Enable all controller services**, then **Start**. Imported controller services are disabled, so the deduplicator shows as invalid until you enable them. This is expected.
4. Look at the log. GenerateFlowFile fires right away and then every 30 seconds. Stop the group when you are done.

   ```bash
   docker logs -f <nifi-container> 2>&1 | grep -A20 dedup-demo   # NiFi in Docker (also matches dedup-demo-failure)
   tail -f $NIFI_HOME/logs/nifi-app.log | grep -A20 dedup-demo    # local NiFi
   ```
5. To see the input with duplicates, stop only `Keep latest record per id`. Then right-click the `input (with duplicates)` connection, choose **List queue**, and use the eye icon to view the content.

## Project layout

```
├── Makefile                          # Docker build
├── mvnw, .mvn/                       # Maven Wrapper (Maven 3.9.16)
├── pom.xml                           # parent: org.apache.nifi:nifi-extension-bundles:2.12.0
├── nifi-latest-dedup-processors/     # processor code and tests (jar)
├── nifi-latest-dedup-nar/            # NAR packaging; parent NAR: nifi-standard-services-api-nar
├── flows/                            # demo flow definition
└── docs/                             # technical specification
```

The processor is registered via `META-INF/services/org.apache.nifi.processor.Processor`. The NAR does **not** bundle `nifi-api`, `nifi-record` or the record serialization service API. These come from the parent NAR, which lets the processor work with the standard Record Reader and Writer controller services.
