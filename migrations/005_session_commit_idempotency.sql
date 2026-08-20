CREATE TABLE IF NOT EXISTS react_agent_session_commits (
    session_id TEXT NOT NULL REFERENCES react_agent_sessions(session_id),
    operation_id TEXT NOT NULL,
    expected_version BIGINT NOT NULL,
    committed_version BIGINT NOT NULL,
    transcript_hash CHAR(64) NOT NULL,
    transcript JSONB NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, operation_id),
    CONSTRAINT react_agent_session_commits_version_unique
        UNIQUE (session_id, committed_version),
    CHECK (operation_id <> ''),
    CHECK (expected_version >= 0),
    CHECK (committed_version = expected_version + 1),
    CHECK (transcript_hash ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(transcript) = 'array')
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'react_agent_session_commits_version_unique'
            AND conrelid = 'react_agent_session_commits'::regclass
    ) THEN
        ALTER TABLE react_agent_session_commits
            ADD CONSTRAINT react_agent_session_commits_version_unique
            UNIQUE (session_id, committed_version);
    END IF;
END
$$;
