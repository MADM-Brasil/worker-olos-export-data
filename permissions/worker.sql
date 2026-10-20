-- TEMPLATE MANUAL. Executar como administrador após revisar 001/002.
-- Role de grupo sem login/segredo; o login exclusivo será provisionado pela operação.
-- NÃO conceder o role de owner/migrator ao worker. Não alterar ACLs do schema public.
BEGIN;
CREATE ROLE olos_export_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT USAGE ON SCHEMA olos_export TO olos_export_worker;
GRANT SELECT ON olos_export.source_contracts, olos_export.schema_versions TO olos_export_worker;
GRANT SELECT, INSERT, UPDATE ON
 olos_export.streams, olos_export.runs, olos_export.source_locks,
 olos_export.batches, olos_export.checkpoints, olos_export.recovery_cases
 TO olos_export_worker;
GRANT SELECT, INSERT ON olos_export.sync_errors, olos_export.recovery_attempts,
 olos_export.audit_events TO olos_export_worker;
GRANT SELECT, INSERT, UPDATE ON
 olos_export.attempts_raw_data, olos_export.inbound_raw_data, olos_export.pbx_billing_data,
 olos_export.users, olos_export.campaign, olos_export.disposition TO olos_export_worker;
GRANT EXECUTE ON FUNCTION olos_export.safe_payload(jsonb) TO olos_export_worker;
COMMIT;
-- Em sessão administrativa, conceder CONNECT no banco específico e associar o login:
-- GRANT CONNECT ON DATABASE hubspot_olos TO <login_exclusivo>;
-- GRANT olos_export_worker TO <login_exclusivo>;
-- Definir senha por canal protegido. Não há DELETE, TRUNCATE, CREATE, ownership ou grant option.
-- Verificar privilégios herdados/PUBLIC do banco existente sem modificar public nesta entrega.
