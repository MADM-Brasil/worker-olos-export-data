-- Aplicação MANUAL no banco receptor. Requer namespace olos_export preparado pelo DBA.
-- Pode coexistir com 001/002/003; não cria raw nem concede escrita em negócio.
BEGIN;
CREATE TABLE olos_export.delivery_schema_versions (
 version integer PRIMARY KEY,
 applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE olos_export.delivery_receipts (
 destination_id text NOT NULL,
 delivery_id uuid NOT NULL,
 request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
 destination_version text NOT NULL,
 mapping_version text NOT NULL,
 state text NOT NULL CHECK (state IN ('confirmed','rejected')),
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(destination_id,delivery_id)
);
INSERT INTO olos_export.delivery_schema_versions(version) VALUES(1);
COMMIT;
