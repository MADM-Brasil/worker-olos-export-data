-- Aplicação MANUAL após 005, usando o role sem login já preparado em worker.sql.
-- Sem privilégios de DDL, DELETE, TRUNCATE, UPDATE de recibos ou aprovação de contratos.
BEGIN;
GRANT SELECT ON olos_export.large_batch_schema_versions TO olos_export_worker;
GRANT SELECT, INSERT ON olos_export.large_batch_receipts, olos_export.large_batch_keys TO olos_export_worker;
COMMIT;
