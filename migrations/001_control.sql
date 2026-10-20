-- Aplicação MANUAL por administrador; nunca importada pelo runtime.
-- Executar somente após revisão e backup/plano operacional aprovado.
BEGIN;
CREATE SCHEMA olos_export;
REVOKE ALL ON SCHEMA olos_export FROM PUBLIC;
CREATE TABLE olos_export.schema_versions (
  version integer PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Campos e chaves da Olos NÃO confirmados. Nenhuma aprovação é concedida aqui.
CREATE TABLE olos_export.source_contracts (
  source_table text NOT NULL CHECK (source_table IN ('AttemptsRawData','InboundRawData','PbxBillingData','Users','Campaign','Disposition')),
  version text NOT NULL,
  approved boolean NOT NULL DEFAULT false,
  fields jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(fields) = 'object'),
  key_fields text[] NOT NULL DEFAULT '{}',
  layout_evidence text,
  key_evidence text,
  PRIMARY KEY (source_table, version),
  CHECK (NOT approved OR (cardinality(key_fields) > 0 AND layout_evidence IS NOT NULL AND key_evidence IS NOT NULL AND length(layout_evidence) > 0 AND length(key_evidence) > 0))
);
INSERT INTO olos_export.source_contracts (source_table, version) VALUES
 ('AttemptsRawData','pending'),('InboundRawData','pending'),('PbxBillingData','pending'),
 ('Users','pending'),('Campaign','pending'),('Disposition','pending');

CREATE TABLE olos_export.streams (
  id uuid PRIMARY KEY,
  source_table text NOT NULL,
  read_type text NOT NULL CHECK (read_type IN ('current_day','previous_days','reread','reference_snapshot','investigation')),
  export_date date NOT NULL,
  last_success_at timestamptz,
  UNIQUE (source_table, read_type, export_date),
  UNIQUE (id, source_table)
);
CREATE TABLE olos_export.runs (
  id uuid PRIMARY KEY,
  stream_id uuid NOT NULL,
  source_table text NOT NULL,
  contract_version text NOT NULL,
  fencing_token bigint NOT NULL CHECK (fencing_token > 0),
  state text NOT NULL CHECK (state IN ('read_intent','persisting','succeeded','failed','needs_reconciliation')),
  pk_range jsonb, -- componentes de PK como strings; sem presumir nome/tipo/escopo
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  received_count bigint NOT NULL DEFAULT 0 CHECK (received_count >= 0),
  persisted_count bigint NOT NULL DEFAULT 0 CHECK (persisted_count >= 0),
  duplicate_count bigint NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
  rejected_count bigint NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
  FOREIGN KEY (stream_id, source_table) REFERENCES olos_export.streams(id, source_table),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  UNIQUE (id, stream_id, source_table),
  UNIQUE (id, source_table)
);
CREATE TABLE olos_export.source_locks (
  source_table text PRIMARY KEY CHECK (source_table IN ('AttemptsRawData','InboundRawData','PbxBillingData','Users','Campaign','Disposition')),
  generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
  owner_run_id uuid REFERENCES olos_export.runs(id),
  lease_until timestamptz,
  CHECK ((owner_run_id IS NULL) = (lease_until IS NULL))
);
INSERT INTO olos_export.source_locks(source_table) SELECT source_table FROM olos_export.source_contracts;
CREATE TABLE olos_export.batches (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL,
  stream_id uuid NOT NULL,
  source_table text NOT NULL,
  digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
  expected_version bigint NOT NULL CHECK (expected_version >= 0),
  previous_cursor text,
  next_cursor text,
  state text NOT NULL CHECK (state IN ('read_observed','committed','failed','needs_reconciliation')),
  received_count integer NOT NULL CHECK (received_count >= 0),
  persisted_count integer NOT NULL DEFAULT 0 CHECK (persisted_count >= 0),
  duplicate_count integer NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
  rejected_count integer NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
  observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  committed_at timestamptz,
  FOREIGN KEY (run_id, stream_id, source_table) REFERENCES olos_export.runs(id, stream_id, source_table),
  UNIQUE (id, source_table),
  UNIQUE (id, stream_id),
  CHECK (state <> 'committed' OR (received_count = persisted_count + duplicate_count + rejected_count AND rejected_count = 0 AND committed_at IS NOT NULL))
);
-- Somente um lote observado por run: evita saltos e leituras ainda não persistidas.
CREATE UNIQUE INDEX one_pending_batch_per_run ON olos_export.batches(run_id)
 WHERE state IN ('read_observed','failed','needs_reconciliation');
CREATE TABLE olos_export.checkpoints (
  stream_id uuid PRIMARY KEY REFERENCES olos_export.streams(id),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  cursor_value text,
  last_batch_id uuid,
  last_success_at timestamptz,
  FOREIGN KEY (last_batch_id, stream_id) REFERENCES olos_export.batches(id, stream_id)
);
CREATE TABLE olos_export.sync_errors (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES olos_export.runs(id),
  batch_id uuid REFERENCES olos_export.batches(id),
  code text NOT NULL CHECK (code IN ('PERSISTENCE_FAILED','COMMIT_UNKNOWN','VALIDATION_FAILED','CHECKPOINT_CONFLICT','LEASE_LOST')),
  outcome text NOT NULL CHECK (outcome IN ('not_committed','unknown')),
  received_count integer NOT NULL CHECK (received_count >= 0),
  rejected_count integer NOT NULL CHECK (rejected_count >= 0),
  occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
  -- Sem mensagem/stack/payload bruto ou parâmetros SQL.
);
CREATE TABLE olos_export.recovery_cases (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL UNIQUE REFERENCES olos_export.runs(id),
  state text NOT NULL DEFAULT 'open' CHECK (state IN ('open','resolved')),
  reason text NOT NULL CHECK (reason IN ('expired_read_intent','persistence_unknown')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  resolved_at timestamptz
);
CREATE TABLE olos_export.recovery_attempts (
  id uuid PRIMARY KEY,
  case_id uuid NOT NULL REFERENCES olos_export.recovery_cases(id),
  action text NOT NULL CHECK (action IN ('inspect_log','plan_reread','manual_review')),
  result text NOT NULL CHECK (result IN ('inconclusive','evidence_recorded','failed')),
  evidence_digest text CHECK (evidence_digest IS NULL OR evidence_digest ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE olos_export.audit_events (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES olos_export.runs(id),
  batch_id uuid UNIQUE REFERENCES olos_export.batches(id),
  event text NOT NULL CHECK (event IN ('batch_committed','reconciliation_required')),
  received_count integer NOT NULL CHECK (received_count >= 0),
  persisted_count integer NOT NULL CHECK (persisted_count >= 0),
  duplicate_count integer NOT NULL CHECK (duplicate_count >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX runs_stream_state ON olos_export.runs(stream_id, state);
CREATE INDEX batches_run ON olos_export.batches(run_id);
INSERT INTO olos_export.schema_versions(version) VALUES (1);
COMMIT;
