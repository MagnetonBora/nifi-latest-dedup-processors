package com.example.nifi.dedup;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * Data error detected while deduplicating a FlowFile: missing or null key/timestamp,
 * unsupported value type or unparseable timestamp. Routes the FlowFile to failure.
 */
public class DeduplicationException extends ProcessException {

    public DeduplicationException(final String message) {
        super(message);
    }

    public DeduplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
