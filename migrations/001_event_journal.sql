CREATE TABLE IF NOT EXISTS react_agent_sessions (
    session_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS react_agent_runs (
    run_id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES react_agent_sessions(session_id),
    agent_revision TEXT,
    tool_manifest_hash TEXT,
    head_sequence BIGINT NOT NULL,
    head_hash CHAR(64) NOT NULL,
    terminal BOOLEAN NOT NULL DEFAULT FALSE,
    idempotency_key TEXT,
    request_hash CHAR(64),
    fence BIGINT NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (session_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS react_agent_run_events (
    run_id TEXT NOT NULL REFERENCES react_agent_runs(run_id),
    sequence BIGINT NOT NULL,
    event_id UUID NOT NULL,
    operation_id TEXT NOT NULL,
    operation_payload_hash CHAR(64) NOT NULL,
    schema_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    privacy_class TEXT NOT NULL,
    occurred_at DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    step INTEGER,
    call_key TEXT,
    session_id TEXT,
    execution_id TEXT,
    agent_revision TEXT,
    tool_manifest_hash TEXT,
    public_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    private_payload JSONB,
    safe_checkpoint BOOLEAN NOT NULL DEFAULT FALSE,
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    model_calls_delta INTEGER NOT NULL DEFAULT 0,
    tool_calls_delta INTEGER NOT NULL DEFAULT 0,
    tool_executions_delta INTEGER NOT NULL DEFAULT 0,
    previous_hash CHAR(64) NOT NULL,
    event_hash CHAR(64) NOT NULL,
    PRIMARY KEY (run_id, sequence),
    UNIQUE (run_id, operation_id),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS react_agent_run_events_session_idx
    ON react_agent_run_events (session_id, recorded_at, run_id, sequence);

CREATE TABLE IF NOT EXISTS react_agent_run_snapshots (
    run_id TEXT PRIMARY KEY REFERENCES react_agent_runs(run_id),
    last_sequence BIGINT NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS react_agent_session_snapshots (
    session_id TEXT PRIMARY KEY REFERENCES react_agent_sessions(session_id),
    version BIGINT NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE react_agent_run_events IS
    'Append-only Agent facts. The runtime role must not receive UPDATE or DELETE grants.';
