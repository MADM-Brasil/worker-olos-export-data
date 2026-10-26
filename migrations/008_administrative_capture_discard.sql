-- MANUAL after 007. Does not change the core schema_versions protocol [1,2].
BEGIN;
CREATE TABLE olos_export.administrative_capture_discards (
  capture_id uuid PRIMARY KEY,
  batch_id uuid NOT NULL UNIQUE,
  original_run_id uuid NOT NULL UNIQUE REFERENCES olos_export.runs(id),
  recovery_case_id uuid NOT NULL UNIQUE REFERENCES olos_export.recovery_cases(id),
  stream_id uuid NOT NULL REFERENCES olos_export.streams(id),
  configuration_digest text NOT NULL CHECK (configuration_digest ~ '^[0-9a-f]{64}$'),
  manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
  original_manifest jsonb NOT NULL CHECK (jsonb_typeof(original_manifest)='object'),
  authorization_id uuid NOT NULL UNIQUE,
  authorization_digest text NOT NULL CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
  authorization_document jsonb NOT NULL CHECK (jsonb_typeof(authorization_document)='object'),
  expected_generation bigint NOT NULL CHECK (expected_generation>0),
  operator_reference text NOT NULL CHECK (operator_reference ~ '^[A-Za-z0-9_.:/-]{3,128}$'),
  reason text NOT NULL CHECK (reason='OPERATOR_DECLINED_HISTORICAL_IMPORT'),
  proof jsonb NOT NULL CHECK (jsonb_typeof(proof)='object'),
  proof_digest text NOT NULL CHECK (proof_digest ~ '^[0-9a-f]{64}$'),
  discarded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX administrative_discard_configuration ON olos_export.administrative_capture_discards(configuration_digest);
COMMIT;
