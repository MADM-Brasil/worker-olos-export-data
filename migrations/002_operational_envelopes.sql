-- Envelopes internos: NÃO constituem confirmação do layout/tipos/chaves ExportDataV3.
-- A chave é derivada apenas de contratos aprovados; todos nascem bloqueados em 001.
BEGIN;
CREATE FUNCTION olos_export.safe_payload(doc jsonb) RETURNS boolean
 LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
 SELECT jsonb_typeof(doc) = 'object' AND NOT EXISTS (
   SELECT 1 FROM jsonb_each(CASE WHEN jsonb_typeof(doc) = 'object' THEN doc ELSE '{}'::jsonb END) AS kv
   WHERE regexp_replace(lower(kv.key), '[^a-z]', '', 'g') ~ '(password|passwd|senha)'
      OR jsonb_typeof(kv.value) NOT IN ('string','boolean','null')
 );
$$;
REVOKE ALL ON FUNCTION olos_export.safe_payload(jsonb) FROM PUBLIC;

CREATE TABLE olos_export.attempts_raw_data (
  source_key text COLLATE "C" PRIMARY KEY, -- serialização canônica dos componentes comprovados, nunca hash como identidade
  source_table text NOT NULL DEFAULT 'AttemptsRawData' CHECK (source_table = 'AttemptsRawData'),
  contract_version text NOT NULL,
  payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
  last_batch_id uuid NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  FOREIGN KEY (last_batch_id, source_table) REFERENCES olos_export.batches(id, source_table)
);

CREATE TABLE olos_export.inbound_raw_data (
  source_key text COLLATE "C" PRIMARY KEY, -- serialização canônica dos componentes comprovados, nunca hash como identidade
  source_table text NOT NULL DEFAULT 'InboundRawData' CHECK (source_table = 'InboundRawData'),
  contract_version text NOT NULL,
  payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
  last_batch_id uuid NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  FOREIGN KEY (last_batch_id, source_table) REFERENCES olos_export.batches(id, source_table)
);

CREATE TABLE olos_export.pbx_billing_data (
  source_key text COLLATE "C" PRIMARY KEY, -- serialização canônica dos componentes comprovados, nunca hash como identidade
  source_table text NOT NULL DEFAULT 'PbxBillingData' CHECK (source_table = 'PbxBillingData'),
  contract_version text NOT NULL,
  payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
  last_batch_id uuid NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  FOREIGN KEY (last_batch_id, source_table) REFERENCES olos_export.batches(id, source_table)
);

CREATE TABLE olos_export.users (
  source_key text COLLATE "C" PRIMARY KEY, -- serialização canônica dos componentes comprovados, nunca hash como identidade
  source_table text NOT NULL DEFAULT 'Users' CHECK (source_table = 'Users'),
  contract_version text NOT NULL,
  payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
  last_batch_id uuid NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  FOREIGN KEY (last_batch_id, source_table) REFERENCES olos_export.batches(id, source_table)
);

CREATE TABLE olos_export.campaign (
  source_key text COLLATE "C" PRIMARY KEY, -- serialização canônica dos componentes comprovados, nunca hash como identidade
  source_table text NOT NULL DEFAULT 'Campaign' CHECK (source_table = 'Campaign'),
  contract_version text NOT NULL,
  payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
  last_batch_id uuid NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  FOREIGN KEY (last_batch_id, source_table) REFERENCES olos_export.batches(id, source_table)
);

CREATE TABLE olos_export.disposition (
  source_key text COLLATE "C" PRIMARY KEY, -- serialização canônica dos componentes comprovados, nunca hash como identidade
  source_table text NOT NULL DEFAULT 'Disposition' CHECK (source_table = 'Disposition'),
  contract_version text NOT NULL,
  payload jsonb NOT NULL CHECK (olos_export.safe_payload(payload)),
  last_batch_id uuid NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_table, contract_version) REFERENCES olos_export.source_contracts(source_table, version),
  FOREIGN KEY (last_batch_id, source_table) REFERENCES olos_export.batches(id, source_table)
);

INSERT INTO olos_export.schema_versions(version) VALUES (2);
COMMIT;
