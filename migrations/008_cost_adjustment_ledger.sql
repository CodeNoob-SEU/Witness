-- Provider invoices and operator corrections may arrive after run_completed.
-- Keep them in an independent append-only ledger so the terminal run journal
-- remains closed and its sequence/hash chain never changes.
CREATE TABLE IF NOT EXISTS react_agent_cost_adjustments (
    run_id TEXT NOT NULL REFERENCES react_agent_runs(run_id),
    ledger_sequence BIGINT NOT NULL,
    record_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    previous_record_id TEXT NOT NULL,
    operation_payload_hash CHAR(64) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    public_payload JSONB NOT NULL,
    PRIMARY KEY (run_id, ledger_sequence),
    UNIQUE (run_id, record_id),
    UNIQUE (run_id, operation_id),
    UNIQUE (run_id, previous_record_id),
    CHECK (ledger_sequence >= 1),
    CHECK (record_id <> ''),
    CHECK (operation_id <> ''),
    CHECK (previous_record_id <> ''),
    CHECK (record_id <> previous_record_id),
    CHECK (operation_payload_hash ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(public_payload) = 'object')
);

CREATE INDEX IF NOT EXISTS react_agent_cost_adjustments_run_recorded_idx
    ON react_agent_cost_adjustments (run_id, recorded_at, ledger_sequence);

CREATE OR REPLACE FUNCTION react_agent_validate_cost_adjustment_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.public_payload->>'record_id' IS DISTINCT FROM NEW.record_id
        OR NEW.public_payload->>'adjustment_operation_id' IS DISTINCT FROM NEW.operation_id
        OR NEW.public_payload->>'adjusts_record_id' IS DISTINCT FROM NEW.previous_record_id
        OR NEW.public_payload->>'kind' IS DISTINCT FROM 'adjustment'
        OR NEW.public_payload->>'source' IS DISTINCT FROM 'manual_adjustment'
        OR (NEW.public_payload->>'ledger_sequence')::BIGINT IS DISTINCT FROM NEW.ledger_sequence
        OR NEW.public_payload->>'revised_total_micros' IS NULL
        OR (NEW.public_payload->>'revised_total_micros')::BIGINT < 0
        OR (NEW.public_payload->>'operation_total_micros')::BIGINT
            IS DISTINCT FROM (NEW.public_payload->>'revised_total_micros')::BIGINT
        OR NEW.public_payload->>'amount_micros' IS NULL
        OR NEW.public_payload->>'currency' IS NULL
        OR (NEW.public_payload->>'currency') !~ '^[A-Z]{3}$'
        OR length(COALESCE(NEW.public_payload->>'note', '')) > 2000 THEN
        RAISE EXCEPTION 'cost adjustment projection differs from immutable columns'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS react_agent_validate_cost_adjustment_insert_trigger
    ON react_agent_cost_adjustments;
CREATE TRIGGER react_agent_validate_cost_adjustment_insert_trigger
BEFORE INSERT ON react_agent_cost_adjustments
FOR EACH ROW EXECUTE FUNCTION react_agent_validate_cost_adjustment_insert();

CREATE OR REPLACE FUNCTION react_agent_reject_cost_adjustment_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'react_agent_cost_adjustments is append-only'
        USING ERRCODE = '55000';
END
$$;

DROP TRIGGER IF EXISTS react_agent_reject_cost_adjustment_mutation_trigger
    ON react_agent_cost_adjustments;
CREATE TRIGGER react_agent_reject_cost_adjustment_mutation_trigger
BEFORE UPDATE OR DELETE ON react_agent_cost_adjustments
FOR EACH ROW EXECUTE FUNCTION react_agent_reject_cost_adjustment_mutation();

DROP TRIGGER IF EXISTS react_agent_reject_cost_adjustment_truncate_trigger
    ON react_agent_cost_adjustments;
CREATE TRIGGER react_agent_reject_cost_adjustment_truncate_trigger
BEFORE TRUNCATE ON react_agent_cost_adjustments
FOR EACH STATEMENT EXECUTE FUNCTION react_agent_reject_cost_adjustment_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON react_agent_cost_adjustments FROM PUBLIC;

COMMENT ON TABLE react_agent_cost_adjustments IS
    'Append-only post-hoc cost corrections; independent from terminal run event sequences.';
