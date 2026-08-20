-- Content-free OpenTelemetry projection used only to link a new execution
-- trace to the execution that it resumed. This table is not a fact source:
-- missing rows or write failures must never affect Runtime correctness.
CREATE TABLE IF NOT EXISTS react_agent_execution_trace_references (
    run_id TEXT NOT NULL REFERENCES react_agent_runs(run_id),
    execution_id TEXT NOT NULL,
    trace_id CHAR(32) NOT NULL,
    span_id CHAR(16) NOT NULL,
    trace_flags SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, execution_id),
    CHECK (execution_id <> ''),
    CHECK (
        trace_id ~ '^[0-9a-f]{32}$'
        AND trace_id <> repeat('0', 32)
    ),
    CHECK (
        span_id ~ '^[0-9a-f]{16}$'
        AND span_id <> repeat('0', 16)
    ),
    CHECK (trace_flags IN (0, 1))
);

COMMENT ON TABLE react_agent_execution_trace_references IS
    'Best-effort content-free OTel execution roots; never a Runtime fact source.';
