\getenv writer_password HW11_WRITER_PASSWORD
\getenv reader_password HW11_READER_PASSWORD

SELECT 'CREATE DATABASE fraud_monitoring'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'fraud_monitoring')
\gexec

SELECT 'CREATE ROLE scores_writer LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'scores_writer')
\gexec

SELECT 'CREATE ROLE scores_reader LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'scores_reader')
\gexec

\connect fraud_monitoring

BEGIN;
ALTER ROLE scores_writer PASSWORD :'writer_password';
ALTER ROLE scores_reader PASSWORD :'reader_password';
REVOKE CONNECT ON DATABASE fraud_monitoring FROM PUBLIC;
GRANT CONNECT ON DATABASE fraud_monitoring TO scores_writer, scores_reader;

CREATE SCHEMA IF NOT EXISTS scoring;
CREATE TABLE IF NOT EXISTS scoring.scores (
    transaction_id text PRIMARY KEY,
    scored_at timestamptz NOT NULL,
    score double precision NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag smallint NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version text NOT NULL,
    us_state text,
    merch text,
    cat_id text
);
CREATE INDEX IF NOT EXISTS scores_recent_idx
    ON scoring.scores (scored_at DESC, transaction_id DESC);

GRANT USAGE ON SCHEMA scoring TO scores_writer, scores_reader;
GRANT INSERT, SELECT (transaction_id) ON scoring.scores TO scores_writer;
GRANT SELECT ON scoring.scores TO scores_reader;
COMMIT;
