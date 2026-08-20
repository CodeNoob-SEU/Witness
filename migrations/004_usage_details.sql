ALTER TABLE react_agent_run_events
    ADD COLUMN IF NOT EXISTS cached_input_tokens BIGINT,
    ADD COLUMN IF NOT EXISTS reasoning_output_tokens BIGINT,
    ADD COLUMN IF NOT EXISTS billable_tokens BIGINT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_run_events_usage_details_valid'
            AND conrelid = 'react_agent_run_events'::regclass
    ) THEN
        ALTER TABLE react_agent_run_events
            ADD CONSTRAINT react_agent_run_events_usage_details_valid
            CHECK (
                (cached_input_tokens IS NULL OR (
                    cached_input_tokens >= 0
                    AND cached_input_tokens <= input_tokens
                ))
                AND (reasoning_output_tokens IS NULL OR (
                    reasoning_output_tokens >= 0
                    AND reasoning_output_tokens <= output_tokens
                ))
                AND (billable_tokens IS NULL OR billable_tokens >= 0)
            ) NOT VALID;
    END IF;
END
$$;

-- Keep the ACCESS EXCLUSIVE phase short on an existing journal; validation
-- scans old rows under the lighter lock used by VALIDATE CONSTRAINT.
ALTER TABLE react_agent_run_events
    VALIDATE CONSTRAINT react_agent_run_events_usage_details_valid;
