-- MANUAL, após 001/002. Somente controles internos; não aprova contratos nem altera public.
-- Ledger separado preserva a compatibilidade do protocolo original [1,2].
BEGIN;
CREATE TABLE olos_export.large_batch_schema_versions (
  version integer PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE olos_export.large_batch_receipts (
  batch_id uuid PRIMARY KEY REFERENCES olos_export.batches(id),
  manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
  data_digest text NOT NULL CHECK (data_digest ~ '^[0-9a-f]{64}$'),
  record_count integer NOT NULL CHECK (record_count BETWEEN 0 AND 200000),
  format_version text NOT NULL CHECK (format_version='olos-spool/v1')
);
-- Deduplicação por chave comprovada, limitada em memória por parcela e global por resposta.
-- Digest verifica conteúdo, não substitui a identidade original.
CREATE TABLE olos_export.large_batch_keys (
  batch_id uuid NOT NULL REFERENCES olos_export.large_batch_receipts(batch_id),
  source_key text COLLATE "C" NOT NULL,
  payload_digest text NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY(batch_id,source_key)
);
INSERT INTO olos_export.large_batch_schema_versions(version) VALUES (1);
COMMIT;
