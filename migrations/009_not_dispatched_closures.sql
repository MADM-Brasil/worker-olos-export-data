-- MANUAL after 008. Keep the core schema_versions protocol [1,2].
BEGIN;
CREATE TABLE olos_export.not_dispatched_closures (
 capture_id uuid PRIMARY KEY,
 run_id uuid NOT NULL UNIQUE REFERENCES olos_export.runs(id),
 batch_id uuid NOT NULL UNIQUE,
 stream_id uuid NOT NULL REFERENCES olos_export.streams(id),
 previous_generation bigint NOT NULL CHECK (previous_generation>0),
 released_generation bigint NOT NULL CHECK (released_generation=previous_generation+1),
 evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence)='object'),
 evidence_digest text NOT NULL CHECK (evidence_digest ~ '^[a-f0-9]{64}$'),
 closed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
COMMIT;
