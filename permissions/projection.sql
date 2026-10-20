-- TEMPLATE MANUAL após 003; não concede nenhum direito no schema raw.
BEGIN;
CREATE ROLE olos_export_projector NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT USAGE ON SCHEMA olos_export TO olos_export_projector;
GRANT SELECT ON olos_export.projection_schema_versions,olos_export.projection_batches TO olos_export_worker,olos_export_projector;
GRANT INSERT ON olos_export.projection_batches,olos_export.projection_outbox TO olos_export_worker;
GRANT UPDATE(snapshot_digest,row_count)
 ON olos_export.projection_batches TO olos_export_worker;
GRANT SELECT ON olos_export.projection_outbox TO olos_export_worker,olos_export_projector;
GRANT UPDATE(state,generation,lease_owner,lease_until,attempts,available_at,attempt_id,request_payload,request_digest,last_code,confirmed_at)
 ON olos_export.projection_outbox TO olos_export_projector;
GRANT SELECT,INSERT ON olos_export.projection_events TO olos_export_projector;
GRANT EXECUTE ON FUNCTION olos_export.safe_payload(jsonb) TO olos_export_projector;
COMMIT;
-- CONNECT/login/segredos são provisionados manualmente pela operação.
