package com.prithhvi.taskqueue.worker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.prithhvi.taskqueue.queue.JobRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Periodically puts back jobs whose worker died mid-run (see JobRepository.reapExpired).
 * It's one UPDATE statement, so it's safe for several app instances to run it at once.
 */
@Component
public class Reaper implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Reaper.class);

    private final JobRepository repo;
    private final WorkerProperties props;
    private ScheduledExecutorService scheduler;

    public Reaper(JobRepository repo, WorkerProperties props) {
        this.repo = repo;
        this.props = props;
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "reaper"));
        scheduler.scheduleWithFixedDelay(this::reap,
                props.reaperIntervalMs(), props.reaperIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void reap() {
        try {
            int n = repo.reapExpired(props.leaseMs());
            if (n > 0) {
                log.warn("Requeued {} job(s) whose lease expired", n);
            }
        } catch (Exception e) {
            log.error("Reaper failed", e);
        }
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return scheduler != null && !scheduler.isShutdown();
    }
}
