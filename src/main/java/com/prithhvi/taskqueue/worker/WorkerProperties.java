package com.prithhvi.taskqueue.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds the taskqueue.workers.* settings from application.yml. */
@ConfigurationProperties(prefix = "taskqueue.workers")
public record WorkerProperties(
        boolean enabled,
        int count,
        long pollIntervalMs,
        int batchSize,          // jobs claimed per query
        long retryBaseMs,       // first retry delay; doubles each attempt
        long retryMaxMs,        // cap on the retry delay
        long leaseMs,           // RUNNING longer than this = worker presumed dead
        long reaperIntervalMs   // how often to look for expired leases
) {}
