-- Aplicação MANUAL após 001/002. Controles internos, não DDL inferido de raw.olos_calls.
-- Ledger separado: schema_versions da ingestão permanece exatamente [1,2].
BEGIN;
CREATE TABLE olos_export.projection_schema_versions (
 version integer PRIMARY KEY,
 applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE olos_export.projection_batches (
 batch_id uuid NOT NULL,
 destination_id text NOT NULL,
 source_table text NOT NULL,
 source_namespace text NOT NULL,
 source_contract_version text NOT NULL,
 field_definitions jsonb NOT NULL CHECK (jsonb_typeof(field_definitions)='object'),
 destination_version text NOT NULL,
 mapping_version text NOT NULL,
 config_digest text NOT NULL CHECK (config_digest ~ '^[0-9a-f]{64}$'),
 snapshot_digest text NOT NULL CHECK (snapshot_digest ~ '^[0-9a-f]{64}$'),
 row_count integer NOT NULL CHECK (row_count>=0),
 captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(batch_id,destination_id),
 FOREIGN KEY(batch_id,source_table) REFERENCES olos_export.batches(id,source_table)
);
CREATE TABLE olos_export.projection_outbox (
 delivery_id uuid PRIMARY KEY,
 batch_id uuid NOT NULL,
 destination_id text NOT NULL,
 source_key text COLLATE "C" NOT NULL,
 payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
 state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','confirmed','rejected','uncertain')),
 generation bigint NOT NULL DEFAULT 0 CHECK (generation>=0),
 lease_owner uuid,
 lease_until timestamptz,
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts>=0),
 available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 attempt_id uuid,
 request_payload jsonb CHECK (request_payload IS NULL OR olos_export.safe_payload(request_payload)),
 request_digest text CHECK (request_digest IS NULL OR request_digest ~ '^[0-9a-f]{64}$'),
 last_code text CHECK (last_code IN ('PROJECTION_PENDING','PROJECTION_REJECTED','DESTINATION_BLOCKED','TARGET_NOT_COMMITTED','TARGET_UNKNOWN','CONFIRMED','RECEIPT_ABSENT','REQUEST_CHANGED')),
 confirmed_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(batch_id,destination_id,source_key),
 FOREIGN KEY(batch_id,destination_id) REFERENCES olos_export.projection_batches(batch_id,destination_id),
 CHECK ((lease_owner IS NULL)=(lease_until IS NULL)),
 CHECK ((request_payload IS NULL)=(request_digest IS NULL)),
 CHECK (state NOT IN ('uncertain','confirmed') OR (request_payload IS NOT NULL AND attempt_id IS NOT NULL)),
 CHECK (state<>'confirmed' OR confirmed_at IS NOT NULL)
);
CREATE INDEX projection_pending ON olos_export.projection_outbox(destination_id,available_at,created_at) WHERE state='pending';
CREATE TABLE olos_export.projection_events (
 id uuid PRIMARY KEY,
 delivery_id uuid NOT NULL REFERENCES olos_export.projection_outbox(delivery_id),
 generation bigint NOT NULL,
 attempt_id uuid,
 event text NOT NULL CHECK (event IN ('claimed','dispatch_authorized','pending','confirmed','rejected','uncertain','receipt_absent')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO olos_export.projection_schema_versions(version) VALUES(1);
COMMIT;
