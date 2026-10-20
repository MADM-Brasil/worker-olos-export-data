-- MANUAL. After 001/002/005. Internal claims only; does not approve source contracts.
BEGIN;
CREATE TABLE olos_export.attempts_recovery_schema_versions (
  version integer PRIMARY KEY CHECK (version=1)
);
INSERT INTO olos_export.attempts_recovery_schema_versions(version) VALUES (1);
-- Immutable, conservative claims: an uncertain RPC is never automatically retried.
-- The primary key fences the same run/action even across copied/local work directories.
CREATE TABLE olos_export.attempts_recovery_operations (
  run_id uuid NOT NULL REFERENCES olos_export.runs(id),
  operation text NOT NULL CHECK (operation IN ('log','reread')),
  claim_id uuid NOT NULL UNIQUE,
  configuration_digest text NOT NULL CHECK (configuration_digest ~ '^[a-f0-9]{64}$'),
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(run_id,operation)
);
COMMIT;
