-- MANUAL after 001..005. Append-only evidence; does not authorize remote acquisition.
BEGIN;
CREATE TABLE olos_export.local_ingestion_reconciliations (
 authorization_id uuid PRIMARY KEY,
 authorization_digest text NOT NULL CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
 capture_id uuid NOT NULL,
 case_id uuid NOT NULL REFERENCES olos_export.recovery_cases(id),
 original_run_id uuid NOT NULL REFERENCES olos_export.runs(id),
 original_batch_id uuid NOT NULL,
 manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
 original_manifest jsonb NOT NULL,
 recovery_run_id uuid NOT NULL UNIQUE REFERENCES olos_export.runs(id),
 previous_generation bigint NOT NULL,
 fencing_token bigint NOT NULL CHECK (fencing_token > previous_generation),
 proof jsonb NOT NULL,
 operator_reference text NOT NULL,
 authorized_at timestamptz NOT NULL,
 expires_at timestamptz NOT NULL,
 reconciled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(original_run_id,fencing_token)
);
CREATE INDEX local_reconciliation_case ON olos_export.local_ingestion_reconciliations(case_id);
COMMIT;
