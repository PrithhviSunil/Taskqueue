package com.prithhvi.taskqueue.worker;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.prithhvi.taskqueue.queue.Job;
import com.prithhvi.taskqueue.queue.JobRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Starts N worker threads when the app starts and stops them cleanly when it shuts down.
 * Each thread runs the same loop: claim a job, run it, mark it done or failed, repeat.
 */
@Component
public class WorkerPool implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final JobRepository repo;
    private final JobHandlers handlers;
    private final WorkerProperties props;
    private final JsonMapper json;

    private ExecutorService executor;
    private volatile boolean running = false;

    public WorkerPool(JobRepository repo, JobHandlers handlers, WorkerProperties props, JsonMapper json) {
        this.repo = repo;
        this.handlers = handlers;
        this.props = props;
        this.json = json;
    }

    @Override
    public void start() {
        if (!props.enabled()) {
            log.info("Workers disabled; running API only");
            return;
        }
        running = true;
        executor = Executors.newFixedThreadPool(props.count());
        // e.g. "12345@laptop" — unique per process, so ids stay unique across machines.
        String processId = ManagementFactory.getRuntimeMXBean().getName();
        for (int i = 0; i < props.count(); i++) {
            String workerId = processId + "-w" + i;
            executor.submit(() -> loop(workerId));
        }
        log.info("Started {} workers", props.count());
    }

    private void loop(String workerId) {
        while (running) {
            try {
                Optional<Job> next = repo.claimNext(workerId);
                if (next.isEmpty()) {
                    // Queue is empty: back off briefly instead of hammering the database.
                    Thread.sleep(props.pollIntervalMs());
                    continue;
                }
                execute(next.get(), workerId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // A database hiccup shouldn't kill the worker thread; log and keep going.
                log.error("Worker {} loop error", workerId, e);
                sleepQuietly(1000);
            }
        }
    }

    private void execute(Job job, String workerId) throws InterruptedException {
        try {
            handlers.get(job.type()).run(json.readTree(job.payload()));
            repo.complete(job.id(), workerId);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            repo.fail(job.id(), workerId, e.toString());
        }
    }

    @Override
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                // Let in-flight jobs finish; anything still RUNNING after this is what
                // day 2's crash recovery will pick back up.
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("Workers stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
