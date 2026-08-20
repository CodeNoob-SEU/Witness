ALTER TABLE react_agent_runs
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT REFERENCES react_agent_runs(run_id),
    ADD COLUMN IF NOT EXISTS fork_sequence BIGINT,
    ADD COLUMN IF NOT EXISTS workspace_tree TEXT;

CREATE INDEX IF NOT EXISTS react_agent_runs_session_created_idx
    ON react_agent_runs (session_id, created_at DESC, run_id);
