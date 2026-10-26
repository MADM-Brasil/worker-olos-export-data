-- MANUAL after migration 007. No UPDATE/DELETE of reconciliation evidence.
BEGIN;
GRANT SELECT,INSERT ON olos_export.local_ingestion_reconciliations TO olos_export_worker;
GRANT SELECT ON olos_export.projection_events,olos_export.delivery_receipts TO olos_export_worker;
COMMIT;
