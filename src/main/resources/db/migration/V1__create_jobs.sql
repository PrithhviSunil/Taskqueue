-- One row = one piece of background work.
CREATE TABLE jobs (
    id            BIGSERIAL PRIMARY KEY,
    type          TEXT        NOT NULL,                  -- which handler runs it, e.g. 'sleep'
    payload       JSONB       NOT NULL DEFAULT '{}',     -- the handler's input
    status        TEXT        NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'RUNNING', 'DONE', 'FAILED', 'DEAD')),
    attempts      INT         NOT NULL DEFAULT 0,        -- bumped every time a worker claims it
    max_attempts  INT         NOT NULL DEFAULT 5,        -- used by retries (day 2)
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),    -- not eligible before this (retry backoff)
    locked_by     TEXT,                                  -- which worker is running it
    locked_at     TIMESTAMPTZ,                           -- when it was claimed (crash recovery, day 2)
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

-- Workers only ever look for PENDING jobs that are due, oldest first.
-- A partial index keeps that lookup small even when millions of DONE rows pile up.
CREATE INDEX jobs_pending_idx ON jobs (run_at, id) WHERE status = 'PENDING';
