package com.prithhvi.taskqueue.queue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * All SQL for the queue lives here. Each method is a single statement, so each one
 * is atomic on its own; no explicit transactions are needed yet.
 */
@Repository
public class JobRepository {

    private static final RowMapper<Job> JOB_ROW = (rs, i) -> new Job(
            rs.getLong("id"),
            rs.getString("type"),
            rs.getString("payload"),
            rs.getString("status"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getString("locked_by"),
            rs.getString("last_error"),
            rs.getObject("created_at", java.time.OffsetDateTime.class),
            rs.getObject("finished_at", java.time.OffsetDateTime.class));

    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Adds a PENDING job and returns its id. */
    public long enqueue(String type, String payloadJson, int maxAttempts) {
        return jdbc.queryForObject("""
                INSERT INTO jobs (type, payload, max_attempts)
                VALUES (?, ?::jsonb, ?)
                RETURNING id
                """, Long.class, type, payloadJson, maxAttempts);
    }

    public Optional<Job> findById(long id) {
        List<Job> rows = jdbc.query("SELECT * FROM jobs WHERE id = ?", JOB_ROW, id);
        return rows.stream().findFirst();
    }

    /**
     * Atomically takes the oldest due PENDING job and marks it RUNNING for this worker.
     *
     * FOR UPDATE locks the row we pick. SKIP LOCKED means: if another worker has already
     * locked a row, don't wait for it, just move on to the next one. So N workers running
     * this at the same moment each get a different job, and none of them block each other.
     */
    public Optional<Job> claimNext(String workerId) {
        List<Job> rows = jdbc.query("""
                UPDATE jobs
                SET status = 'RUNNING',
                    locked_by = ?,
                    locked_at = now(),
                    attempts = attempts + 1
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE status = 'PENDING' AND run_at <= now()
                    ORDER BY run_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *
                """, JOB_ROW, workerId);
        return rows.stream().findFirst();
    }

    /**
     * Marks a job DONE. The locked_by check means a worker can only complete a job it
     * still owns. (Matters on day 2, when stuck jobs get handed to someone else.)
     */
    public boolean complete(long id, String workerId) {
        return jdbc.update("""
                UPDATE jobs
                SET status = 'DONE', finished_at = now(), locked_by = NULL
                WHERE id = ? AND locked_by = ? AND status = 'RUNNING'
                """, id, workerId) == 1;
    }

    /** Day 1: any failure is final. Day 2 replaces this with retry + backoff + dead-letter. */
    public boolean fail(long id, String workerId, String error) {
        return jdbc.update("""
                UPDATE jobs
                SET status = 'FAILED', finished_at = now(), locked_by = NULL, last_error = ?
                WHERE id = ? AND locked_by = ? AND status = 'RUNNING'
                """, error, id, workerId) == 1;
    }

    /**
     * Measures a batch of jobs (ids fromId..toId), computed from the database's own
     * timestamps so the numbers don't depend on how the client measures time.
     *   throughput  = jobs finished / (last finish - first claim)
     *   latency     = finished_at - created_at (time from enqueue until done)
     */
    public Map<String, Object> window(long fromId, long toId) {
        return jdbc.queryForMap("""
                SELECT count(*)                                         AS total,
                       count(*) FILTER (WHERE status = 'DONE')          AS done,
                       count(*) FILTER (WHERE status = 'DONE' AND attempts > 1) AS claimed_more_than_once,
                       extract(epoch FROM max(finished_at) - min(locked_at)) AS processing_seconds,
                       percentile_cont(0.50) WITHIN GROUP (
                           ORDER BY extract(epoch FROM finished_at - created_at) * 1000)
                           FILTER (WHERE status = 'DONE')               AS p50_latency_ms,
                       percentile_cont(0.99) WITHIN GROUP (
                           ORDER BY extract(epoch FROM finished_at - created_at) * 1000)
                           FILTER (WHERE status = 'DONE')               AS p99_latency_ms
                FROM jobs
                WHERE id BETWEEN ? AND ?
                """, fromId, toId);
    }

    /** Job counts per status, plus a correctness check used by the benchmark. */
    public Map<String, Long> stats() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String s : List.of("PENDING", "RUNNING", "DONE", "FAILED", "DEAD")) {
            out.put(s, 0L);
        }
        jdbc.query("SELECT status, count(*) AS n FROM jobs GROUP BY status",
                rs -> { out.put(rs.getString("status"), rs.getLong("n")); });
        // With no retries yet, a DONE job should have been claimed exactly once.
        // Anything above 1 would mean two workers ran the same job.
        out.put("claimedMoreThanOnce", jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE status = 'DONE' AND attempts > 1", Long.class));
        return out;
    }
}
