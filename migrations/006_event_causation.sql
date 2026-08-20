ALTER TABLE react_agent_run_events
    ADD COLUMN IF NOT EXISTS causation_id UUID;

CREATE INDEX IF NOT EXISTS react_agent_run_events_causation_idx
    ON react_agent_run_events (causation_id)
    WHERE causation_id IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_run_events_causation_not_self'
            AND conrelid = 'react_agent_run_events'::regclass
    ) THEN
        ALTER TABLE react_agent_run_events
            ADD CONSTRAINT react_agent_run_events_causation_not_self
            CHECK (causation_id IS NULL OR causation_id <> event_id)
            NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_run_events_causation_fk'
            AND conrelid = 'react_agent_run_events'::regclass
    ) THEN
        ALTER TABLE react_agent_run_events
            ADD CONSTRAINT react_agent_run_events_causation_fk
            FOREIGN KEY (causation_id)
            REFERENCES react_agent_run_events(event_id)
            NOT VALID;
    END IF;
END
$$;

-- Reruns also finish validation after an interrupted/manual partial migration.
ALTER TABLE react_agent_run_events
    VALIDATE CONSTRAINT react_agent_run_events_causation_not_self;
ALTER TABLE react_agent_run_events
    VALIDATE CONSTRAINT react_agent_run_events_causation_fk;
