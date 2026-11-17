package com.prithhvi.taskqueue.worker;

import java.util.concurrent.ThreadLocalRandom;

/**
 * How long to wait before retrying a failed job.
 *
 * Exponential: 1s, 2s, 4s, 8s... so a struggling downstream service gets more breathing
 * room each time instead of being hammered.
 *
 * Jitter: the actual delay is a random point between half and all of that. If 1,000 jobs
 * fail at once because a service went down, they won't all retry at the same instant
 * and knock it over again (the "thundering herd" problem).
 */
public final class Backoff {

    private Backoff() {}

    public static long delayMs(int attempt, long baseMs, long maxMs) {
        // Cap the exponent so 2^n can't overflow on a job with many attempts.
        int exp = Math.min(Math.max(attempt - 1, 0), 30);
        long ceiling = Math.min(maxMs, baseMs * (1L << exp));
        long half = ceiling / 2;
        return half + ThreadLocalRandom.current().nextLong(half + 1);
    }
}
