-- TEMPLATE MANUAL no receptor após 004. Negócio exige aprovação/grants separados.
BEGIN;
CREATE ROLE olos_export_receiver NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT USAGE ON SCHEMA olos_export TO olos_export_receiver;
GRANT SELECT ON olos_export.delivery_schema_versions TO olos_export_receiver;
GRANT SELECT,INSERT ON olos_export.delivery_receipts TO olos_export_receiver;
COMMIT;
-- Nenhuma permissão raw/Kommo/HubSpot é concedida aqui.
