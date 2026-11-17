package com.prithhvi.taskqueue.queue;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonRawValue;

/** A row from the jobs table. Payload is kept as raw JSON text. */
public record Job(
        long id,
        String type,
        @JsonRawValue String payload,  // send it back as JSON, not as an escaped string
        String status,
        int attempts,
        int maxAttempts,
        String lockedBy,
        String lastError,
        OffsetDateTime createdAt,
        OffsetDateTime finishedAt
) {}
