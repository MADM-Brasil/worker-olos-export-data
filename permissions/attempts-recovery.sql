-- MANUAL, dedicated group after 006. No DDL/DELETE/UPDATE or public changes.
BEGIN;
GRANT SELECT ON olos_export.attempts_recovery_schema_versions TO olos_export_worker;
GRANT SELECT, INSERT ON olos_export.attempts_recovery_operations TO olos_export_worker;
COMMIT;
