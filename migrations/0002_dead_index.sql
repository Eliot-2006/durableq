-- Plain CREATE INDEX: migrations run in one transaction, which rules out
-- CONCURRENTLY. On a large existing jobs table, build it first by hand with
-- CREATE INDEX CONCURRENTLY jobs_dead ...; IF NOT EXISTS then makes this a no-op.
CREATE INDEX IF NOT EXISTS jobs_dead ON jobs (id) WHERE state = 'dead';
