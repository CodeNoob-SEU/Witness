-- Observer and audit roles should never need SELECT on the event table that
-- contains private recovery checkpoints.  This security-barrier view exposes
-- only the immutable envelope, public payload, counters, and hash-chain data.
CREATE OR REPLACE VIEW react_agent_public_run_events
WITH (security_barrier = true)
AS
SELECT
    run_id,
    sequence,
    event_id,
    causation_id,
    operation_id,
    schema_version,
    event_type,
    privacy_class,
    occurred_at,
    recorded_at,
    step,
    call_key,
    session_id,
    execution_id,
    agent_revision,
    tool_manifest_hash,
    public_payload,
    safe_checkpoint,
    input_tokens,
    output_tokens,
    total_tokens,
    cached_input_tokens,
    reasoning_output_tokens,
    billable_tokens,
    model_calls_delta,
    tool_calls_delta,
    tool_executions_delta,
    previous_hash,
    event_hash
FROM react_agent_run_events;

COMMENT ON VIEW react_agent_public_run_events IS
    'Content-safe event projection for explicitly granted observer roles; excludes private_payload.';
