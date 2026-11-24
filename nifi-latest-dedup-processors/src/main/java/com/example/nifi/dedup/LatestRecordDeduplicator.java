package com.example.nifi.dedup;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"record", "deduplicate", "dedup", "latest", "timestamp", "partition", "window"})
@CapabilityDescription("Deduplicates the records of a single FlowFile, keeping for every distinct value of "
        + "'Partition By' only the record with the latest timestamp (equivalent to SQL "
        + "ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC) = 1). When several records share "
        + "the same key and timestamp, the first one encountered is kept. Output records keep the order in "
        + "which their keys first appeared. Each FlowFile is processed independently; no state is kept "
        + "between FlowFiles. Any invalid record routes the whole, unmodified FlowFile to failure.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records written (distinct keys)"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the Record Writer")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "One record per distinct key is "
        + "held in memory while the FlowFile is processed. FlowFiles with a very large number of distinct keys "
        + "may require a large heap.")
public class LatestRecordDeduplicator extends AbstractProcessor {

    static final String RECORD_COUNT_ATTRIBUTE = "record.count";

    static final Validator TIMESTAMP_FORMAT_VALIDATOR = (subject, input, context) -> {
        try {
            DateTimeFormatter.ofPattern(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        } catch (final IllegalArgumentException e) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("not a valid java.time.format.DateTimeFormatter pattern: " + e.getMessage())
                    .build();
        }
    };

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Controller Service used to read the incoming records")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Controller Service used to write the deduplicated records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor PARTITION_BY = new PropertyDescriptor.Builder()
            .name("Partition By")
            .description("RecordPath selecting the deduplication key, e.g. /id. Must select exactly one scalar, "
                    + "non-null value per record. Values are compared by their string form, so 1 and \"1\" are "
                    + "the same key.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .build();

    static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("Timestamp Field")
            .description("RecordPath selecting the timestamp, e.g. /timestamp. Supported values: timestamp/date "
                    + "types, integral numbers (epoch milliseconds) and strings parsed with 'Timestamp Format'.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .build();

    static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
            .name("Timestamp Format")
            .description("java.time DateTimeFormatter pattern used to parse string timestamps. If not set, "
                    + "ISO-8601 with offset is expected (e.g. 2026-11-23T10:00:00Z). A pattern without zone or "
                    + "offset is interpreted as UTC. Ignored for non-string values.")
            .required(false)
            .addValidator(TIMESTAMP_FORMAT_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with its content replaced by the deduplicated records")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The original, unmodified FlowFile if it could not be read, validated or written")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            List.of(RECORD_READER, RECORD_WRITER, PARTITION_BY, TIMESTAMP_FIELD, TIMESTAMP_FORMAT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    // Immutable configuration, assigned once per schedule before any onTrigger call.
    private volatile RecordPath partitionPath;
    private volatile RecordPath timestampPath;
    private volatile TimestampConverter timestampConverter;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        partitionPath = RecordPath.compile(context.getProperty(PARTITION_BY).getValue());
        timestampPath = RecordPath.compile(context.getProperty(TIMESTAMP_FIELD).getValue());
        timestampConverter = TimestampConverter.forPattern(context.getProperty(TIMESTAMP_FORMAT).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Some readers (e.g. Avro) cannot open an empty stream; an empty input is an empty result.
        if (flowFile.getSize() == 0) {
            session.transfer(session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE, "0"), REL_SUCCESS);
            return;
        }

        final RecordReaderFactory readerFactory =
                context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory =
                context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        try {
            // Phase 1: read everything; nothing is written yet, so a failure here leaves no partial result.
            final ReadResult readResult = readLatestRecords(session, flowFile, readerFactory, writerFactory);

            // Phase 2: write the surviving records. If this throws, the session discards the new content
            // and 'flowFile' still refers to the original content.
            final FlowFile output = writeRecords(session, flowFile, writerFactory, readResult);
            session.transfer(output, REL_SUCCESS);
        } catch (final ProcessException e) {
            getLogger().error("Failed to deduplicate records of {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private ReadResult readLatestRecords(final ProcessSession session, final FlowFile flowFile,
                                         final RecordReaderFactory readerFactory,
                                         final RecordSetWriterFactory writerFactory) {
        final AtomicReference<ReadResult> result = new AtomicReference<>();

        session.read(flowFile, in -> {
            try (RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                final RecordSchema writeSchema = writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema());

                final Map<String, LatestEntry> latest = new LinkedHashMap<>();
                long index = 0;
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    index++;
                    final String key = extractKey(record, index);
                    final Instant timestamp = extractTimestamp(record, index);

                    final LatestEntry current = latest.get(key);
                    // Strictly later wins: on equal timestamps the first record is kept.
                    if (current == null || timestamp.isAfter(current.timestamp())) {
                        latest.put(key, new LatestEntry(timestamp, record));
                    }
                }
                result.set(new ReadResult(writeSchema, latest));
            } catch (final MalformedRecordException | SchemaNotFoundException e) {
                throw new ProcessException("Failed to read records: " + e.getMessage(), e);
            }
        });

        return result.get();
    }

    private FlowFile writeRecords(final ProcessSession session, final FlowFile flowFile,
                                  final RecordSetWriterFactory writerFactory, final ReadResult readResult) {
        final AtomicReference<WriteResult> writeResult = new AtomicReference<>();
        final AtomicReference<String> mimeType = new AtomicReference<>();

        final FlowFile written = session.write(flowFile, out -> {
            try (RecordSetWriter writer =
                         writerFactory.createWriter(getLogger(), readResult.writeSchema(), out, flowFile)) {
                writer.beginRecordSet();
                for (final LatestEntry entry : readResult.latest().values()) {
                    writer.write(entry.record());
                }
                writeResult.set(writer.finishRecordSet());
                mimeType.set(writer.getMimeType());
            } catch (final SchemaNotFoundException e) {
                throw new ProcessException("Failed to create record writer: " + e.getMessage(), e);
            }
        });

        final Map<String, String> attributes = new HashMap<>(writeResult.get().getAttributes());
        attributes.put(RECORD_COUNT_ATTRIBUTE, String.valueOf(writeResult.get().getRecordCount()));
        attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType.get());
        return session.putAllAttributes(written, attributes);
    }

    private String extractKey(final Record record, final long index) {
        final Object value = selectSingleValue(partitionPath, record, PARTITION_BY, index);
        if (value instanceof Record || value instanceof Map || value instanceof Collection
                || value.getClass().isArray()) {
            throw new DeduplicationException(String.format("Record %d: value selected by %s [%s] must be scalar, got %s",
                    index, PARTITION_BY.getName(), partitionPath.getPath(), value.getClass().getSimpleName()));
        }
        return String.valueOf(value);
    }

    private Instant extractTimestamp(final Record record, final long index) {
        final Object value = selectSingleValue(timestampPath, record, TIMESTAMP_FIELD, index);
        try {
            return timestampConverter.toInstant(value);
        } catch (final IllegalArgumentException e) {
            throw new DeduplicationException(String.format("Record %d: invalid timestamp selected by %s [%s]: %s",
                    index, TIMESTAMP_FIELD.getName(), timestampPath.getPath(), e.getMessage()), e);
        }
    }

    private static Object selectSingleValue(final RecordPath path, final Record record,
                                            final PropertyDescriptor property, final long index) {
        final List<FieldValue> selected = path.evaluate(record).getSelectedFields().toList();
        if (selected.isEmpty()) {
            throw new DeduplicationException(String.format("Record %d: no field selected by %s [%s]",
                    index, property.getName(), path.getPath()));
        }
        if (selected.size() > 1) {
            throw new DeduplicationException(String.format("Record %d: %s [%s] selected %d values, expected exactly one",
                    index, property.getName(), path.getPath(), selected.size()));
        }
        final Object value = selected.getFirst().getValue();
        if (value == null) {
            throw new DeduplicationException(String.format("Record %d: field selected by %s [%s] is missing or null",
                    index, property.getName(), path.getPath()));
        }
        return value;
    }

    private record LatestEntry(Instant timestamp, Record record) {
    }

    private record ReadResult(RecordSchema writeSchema, Map<String, LatestEntry> latest) {
    }
}
