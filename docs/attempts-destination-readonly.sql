-- Run through an already authorized PostgreSQL connection to the actual deployment.
-- No SQL Server connection, business rows, DDL, DML or reconciliation writes.
BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '10s';
SELECT current_database() AS database_name, session_user AS login,
       current_setting('is_superuser') AS superuser,
       current_setting('transaction_read_only') AS read_only;
SELECT ordinal_position,column_name,udt_name,is_nullable,datetime_precision,is_identity,is_generated
FROM information_schema.columns
WHERE table_schema='raw' AND table_name='olos_calls' ORDER BY ordinal_position;
SELECT p.contype,array_agg(a.attname ORDER BY k.ord) AS columns
FROM pg_constraint p
LEFT JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(attnum,ord) ON true
LEFT JOIN pg_attribute a ON a.attrelid=p.conrelid AND a.attnum=k.attnum
WHERE p.conrelid='raw.olos_calls'::regclass GROUP BY p.oid,p.contype;
SELECT relrowsecurity AS rls,
 (SELECT count(*) FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) AS user_triggers
FROM pg_class c WHERE c.oid='raw.olos_calls'::regclass;
SELECT has_table_privilege(current_user,'raw.olos_calls','SELECT') AS can_select,
       has_table_privilege(current_user,'raw.olos_calls','INSERT') AS can_insert;
SELECT count(*) AS total_rows FROM raw.olos_calls;
ROLLBACK;
-- Check control ledgers using their existing least-privilege roles:
-- ingest: olos_export.schema_versions=[1,2], large_batch_schema_versions=[1]
-- projector: olos_export.projection_schema_versions=[1]
-- receiver: olos_export.delivery_schema_versions=[1]
-- Each role must also pass AttemptsService.probe's grants, login and control-table checks.
