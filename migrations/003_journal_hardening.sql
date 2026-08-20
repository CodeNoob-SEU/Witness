-- Durable HTTP request idempotency is separate from runs because a request
-- must reserve its run id before the first run event can be committed.
CREATE TABLE IF NOT EXISTS react_agent_requests (
    session_id TEXT NOT NULL REFERENCES react_agent_sessions(session_id),
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, idempotency_key),
    UNIQUE (run_id),
    CHECK (session_id <> ''),
    CHECK (idempotency_key <> ''),
    CHECK (request_hash <> ''),
    CHECK (run_id <> '')
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_runs_head_sequence_positive'
            AND conrelid = 'react_agent_runs'::regclass
    ) THEN
        ALTER TABLE react_agent_runs
            ADD CONSTRAINT react_agent_runs_head_sequence_positive
            CHECK (head_sequence >= 1);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_runs_fence_nonnegative'
            AND conrelid = 'react_agent_runs'::regclass
    ) THEN
        ALTER TABLE react_agent_runs
            ADD CONSTRAINT react_agent_runs_fence_nonnegative
            CHECK (fence >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_runs_lease_complete'
            AND conrelid = 'react_agent_runs'::regclass
    ) THEN
        ALTER TABLE react_agent_runs
            ADD CONSTRAINT react_agent_runs_lease_complete
            CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_runs_lineage_complete'
            AND conrelid = 'react_agent_runs'::regclass
    ) THEN
        ALTER TABLE react_agent_runs
            ADD CONSTRAINT react_agent_runs_lineage_complete
            CHECK ((parent_run_id IS NULL) = (fork_sequence IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_runs_lineage_valid'
            AND conrelid = 'react_agent_runs'::regclass
    ) THEN
        ALTER TABLE react_agent_runs
            ADD CONSTRAINT react_agent_runs_lineage_valid
            CHECK (
                (fork_sequence IS NULL OR fork_sequence >= 1)
                AND (parent_run_id IS NULL OR parent_run_id <> run_id)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_run_events_values_valid'
            AND conrelid = 'react_agent_run_events'::regclass
    ) THEN
        ALTER TABLE react_agent_run_events
            ADD CONSTRAINT react_agent_run_events_values_valid
            CHECK (
                sequence >= 1
                AND schema_version >= 1
                AND input_tokens >= 0
                AND output_tokens >= 0
                AND total_tokens >= 0
                AND model_calls_delta >= 0
                AND tool_calls_delta >= 0
                AND tool_executions_delta >= 0
                AND previous_hash ~ '^[0-9a-f]{64}$'
                AND event_hash ~ '^[0-9a-f]{64}$'
                AND operation_payload_hash ~ '^[0-9a-f]{64}$'
                AND jsonb_typeof(public_payload) = 'object'
                AND (
                    private_payload IS NULL
                    OR jsonb_typeof(private_payload) = 'object'
                )
            );
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION react_agent_validate_event_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    current_run react_agent_runs%ROWTYPE;
BEGIN
    SELECT * INTO current_run
    FROM react_agent_runs
    WHERE run_id = NEW.run_id
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'run does not exist: %', NEW.run_id
            USING ERRCODE = '23503';
    END IF;
    IF NEW.session_id IS DISTINCT FROM current_run.session_id
        OR NEW.agent_revision IS DISTINCT FROM current_run.agent_revision
        OR NEW.tool_manifest_hash IS DISTINCT FROM current_run.tool_manifest_hash THEN
        RAISE EXCEPTION 'event metadata differs from immutable run metadata'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.sequence = 1 THEN
        IF NEW.event_type <> 'run_started'
            OR NEW.previous_hash <> repeat('0', 64)
            OR current_run.head_sequence <> 1
            OR current_run.head_hash <> NEW.event_hash
            OR EXISTS (
                SELECT 1 FROM react_agent_run_events WHERE run_id = NEW.run_id
            ) THEN
            RAISE EXCEPTION 'invalid first event for run %', NEW.run_id
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF current_run.terminal
            OR NEW.sequence <> current_run.head_sequence + 1
            OR NEW.previous_hash <> current_run.head_hash THEN
            RAISE EXCEPTION 'event does not extend current run head for %', NEW.run_id
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS react_agent_validate_event_insert_trigger
    ON react_agent_run_events;
CREATE TRIGGER react_agent_validate_event_insert_trigger
BEFORE INSERT ON react_agent_run_events
FOR EACH ROW EXECUTE FUNCTION react_agent_validate_event_insert();

CREATE OR REPLACE FUNCTION react_agent_reject_event_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'react_agent_run_events is append-only'
        USING ERRCODE = '55000';
END
$$;

DROP TRIGGER IF EXISTS react_agent_reject_event_mutation_trigger
    ON react_agent_run_events;
CREATE TRIGGER react_agent_reject_event_mutation_trigger
BEFORE UPDATE OR DELETE ON react_agent_run_events
FOR EACH ROW EXECUTE FUNCTION react_agent_reject_event_mutation();

DROP TRIGGER IF EXISTS react_agent_reject_event_truncate_trigger
    ON react_agent_run_events;
CREATE TRIGGER react_agent_reject_event_truncate_trigger
BEFORE TRUNCATE ON react_agent_run_events
FOR EACH STATEMENT EXECUTE FUNCTION react_agent_reject_event_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON react_agent_run_events FROM PUBLIC;
