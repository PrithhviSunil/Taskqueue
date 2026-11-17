package com.prithhvi.taskqueue.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds the taskqueue.workers.* settings from application.yml. */
@ConfigurationProperties(prefix = "taskqueue.workers")
public record WorkerProperties(boolean enabled, int count, long pollIntervalMs) {}
