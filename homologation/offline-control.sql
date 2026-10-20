-- ONLY a newly-created, identified offline homologation container. Not a corporate migration.
BEGIN;
CREATE SCHEMA raw;
CREATE TABLE olos_export.offline_imports (
 id uuid PRIMARY KEY,
 input_digest text NOT NULL,
 mapping_digest text NOT NULL,
 run_id uuid NOT NULL,
 stream_id uuid NOT NULL,
 fencing_token bigint NOT NULL,
 batch_id uuid NOT NULL UNIQUE,
 spool_manifest jsonb,
 expected integer NOT NULL CHECK(expected BETWEEN 1 AND 10000),
 confirmed integer NOT NULL DEFAULT 0 CHECK(confirmed BETWEEN 0 AND expected),
 metrics jsonb NOT NULL,
 historical boolean NOT NULL DEFAULT true CHECK(historical),
 loaded_at timestamp NOT NULL,
 completed_at timestamptz,
 UNIQUE(input_digest,mapping_digest)
);
CREATE TABLE olos_export.offline_delivered (
 delivery_id uuid PRIMARY KEY REFERENCES olos_export.projection_outbox(delivery_id),
 import_id uuid NOT NULL REFERENCES olos_export.offline_imports(id)
);
CREATE ROLE olos_offline_homolog NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT olos_export_worker,olos_export_projector,olos_export_receiver TO olos_offline_homolog;
GRANT USAGE ON SCHEMA raw TO olos_offline_homolog;
GRANT SELECT,INSERT,UPDATE ON olos_export.offline_imports TO olos_offline_homolog;
GRANT SELECT,INSERT ON olos_export.offline_delivered TO olos_offline_homolog;
COMMIT;
