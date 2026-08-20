-- A Session owns one isolated worktree, so at most one nonterminal Run (or
-- write-ahead request reservation) may own it at a time.  This intentionally
-- is not a foreign key: reservations exist before react_agent_runs does.
ALTER TABLE react_agent_sessions
    ADD COLUMN IF NOT EXISTS active_run_id TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_sessions_active_run_nonempty'
            AND conrelid = 'react_agent_sessions'::regclass
    ) THEN
        ALTER TABLE react_agent_sessions
            ADD CONSTRAINT react_agent_sessions_active_run_nonempty
            CHECK (active_run_id IS NULL OR active_run_id <> '');
    END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS react_agent_sessions_active_run_unique
    ON react_agent_sessions (active_run_id)
    WHERE active_run_id IS NOT NULL;

-- Upgrade conservatively.  A missing Run row is a valid interrupted
-- reservation and must keep the Session busy.  When legacy data has multiple
-- candidates, the newest claim wins; every other Run is thereby barred from
-- Resume until an operator resolves the pre-existing ambiguity.
WITH candidates AS (
    SELECT run.session_id, run.run_id, run.created_at
    FROM react_agent_runs AS run
    WHERE NOT run.terminal AND run.session_id IS NOT NULL
    UNION ALL
    SELECT request.session_id, request.run_id, request.created_at
    FROM react_agent_requests AS request
    LEFT JOIN react_agent_runs AS run ON run.run_id = request.run_id
    WHERE run.run_id IS NULL
), ranked AS (
    SELECT
        session_id,
        run_id,
        row_number() OVER (
            PARTITION BY session_id ORDER BY created_at DESC, run_id DESC
        ) AS claim_rank
    FROM candidates
)
UPDATE react_agent_sessions AS session
SET active_run_id = ranked.run_id,
    updated_at = clock_timestamp()
FROM ranked
WHERE session.session_id = ranked.session_id
    AND session.active_run_id IS NULL
    AND ranked.claim_rank = 1;
