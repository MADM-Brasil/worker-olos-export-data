-- MANUAL after 009. Append-only audit; no checkpoint or remote authority.
BEGIN;
GRANT SELECT, INSERT ON olos_export.not_dispatched_closures TO olos_export_worker;
COMMIT;
