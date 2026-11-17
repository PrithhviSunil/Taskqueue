package com.prithhvi.taskqueue;


import static org.assertj.core.api.Assertions.assertThat;
import org.springframework.jdbc.core.JdbcTemplate;
import com.prithhvi.taskqueue.queue.Job;
import com.prithhvi.taskqueue.queue.JobRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(properties = "taskqueue.workers.enabled=false")
class JobRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16");

    @Autowired
    JobRepository repo;
    
    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanTable() {
        jdbc.update("TRUNCATE jobs");
    }

    @Test
    void claimedJobCanBeCompleted() {
        long id = repo.enqueue("noop", "{}", 5);

        Job job = repo.claimNext("worker-1").orElseThrow();
        assertThat(job.id()).isEqualTo(id);
        assertThat(job.status()).isEqualTo("RUNNING");
        assertThat(job.attempts()).isEqualTo(1);

        assertThat(repo.complete(id, "worker-1")).isTrue();

        Job reloaded = repo.findById(id).orElseThrow();
        assertThat(reloaded.status()).isEqualTo("DONE");
    }

    @Test 
    void twoJobsCanBeClaimedAtTheSameTime() {
        long first = repo.enqueue("noop", "{}", 5);
        long second = repo.enqueue("noop", "{}", 5);

        Job a = repo.claimNext("worker-A").orElseThrow();
        Job b = repo.claimNext("worker-B").orElseThrow();

        assertThat(a.id()).isNotEqualTo(b.id());
        assertThat(java.util.List.of(a.id(), b.id())).containsExactlyInAnyOrder(first, second);

        assertThat(repo.claimNext("worker-C")).isEmpty();        
    }

    @Test
    void failedJobRetriesThenGoesDead() {
        long id = repo.enqueue("fail", "{}", 2);

        // Attempt 1 fails: back to PENDING, but not eligible until its backoff is over.
        repo.claimNext("worker-1").orElseThrow();
        assertThat(repo.fail(id, "worker-1", "boom", 60_000)).isTrue();

        Job afterFirst = repo.findById(id).orElseThrow();
        assertThat(afterFirst.status()).isEqualTo("PENDING");
        assertThat(afterFirst.attempts()).isEqualTo(1);
        assertThat(afterFirst.lastError()).isEqualTo("boom");
        assertThat(repo.claimNext("worker-2")).isEmpty();

        // Fast-forward the clock instead of sleeping for a minute.
        jdbc.update("UPDATE jobs SET run_at = now() WHERE id = ?", id);

        // Attempt 2 (the last one) fails: DEAD, and never picked up again.
        Job retried = repo.claimNext("worker-2").orElseThrow();
        assertThat(retried.attempts()).isEqualTo(2);
        assertThat(repo.fail(id, "worker-2", "boom again", 0)).isTrue();

        Job dead = repo.findById(id).orElseThrow();
        assertThat(dead.status()).isEqualTo("DEAD");
        assertThat(dead.lastError()).isEqualTo("boom again");
        assertThat(repo.claimNext("worker-3")).isEmpty();
    }

    @Test
    void reaperRecoversJobFromDeadWorker() {
        long id = repo.enqueue("noop", "{}", 5);
        repo.claimNext("worker-1").orElseThrow();

        // Pretend worker-1 claimed this a minute ago and then died.
        jdbc.update("UPDATE jobs SET locked_at = now() - interval '1 minute' WHERE id = ?", id);

        assertThat(repo.reapExpired(30_000)).isEqualTo(1);

        Job requeued = repo.findById(id).orElseThrow();
        assertThat(requeued.status()).isEqualTo("PENDING");
        assertThat(requeued.lockedBy()).isNull();

        Job reclaimed = repo.claimNext("worker-2").orElseThrow();
        assertThat(reclaimed.id()).isEqualTo(id);
        assertThat(reclaimed.attempts()).isEqualTo(2);

        // The "dead" worker comes back late: it no longer owns the job, so it can't finish it.
        assertThat(repo.complete(id, "worker-1")).isFalse();
        assertThat(repo.complete(id, "worker-2")).isTrue();
    }

    @Test
    void reaperLeavesHealthyJobsAlone() {
        long id = repo.enqueue("noop", "{}", 5);
        repo.claimNext("worker-1").orElseThrow();

        // Claimed just now, well within its 30s lease.
        assertThat(repo.reapExpired(30_000)).isZero();
        assertThat(repo.findById(id).orElseThrow().status()).isEqualTo("RUNNING");
    }
}
