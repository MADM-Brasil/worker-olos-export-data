-- MANUAL after migration 008.
-- Append-only administrative discard evidence.
-- Does not authorize acquisition or modify checkpoints.
BEGIN;

GRANT SELECT, INSERT
ON olos_export.administrative_capture_discards
TO olos_export_worker;

COMMIT;
