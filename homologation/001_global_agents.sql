-- Versioned ADDITIVE opt-in, ONLY for an identified isolated offline database.
-- Does not change raw.olos_calls, source checkpoints, or existing ledgers.
BEGIN;
CREATE TABLE olos_export.offline_agent_state (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 version integer NOT NULL CHECK(version=1),
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 snapshot_digest text CHECK(snapshot_digest ~ '^[0-9a-f]{64}$'),
 received integer NOT NULL DEFAULT 0 CHECK(received>=0),
 skipped integer NOT NULL DEFAULT 0 CHECK(skipped BETWEEN 0 AND received),
 updated_at timestamptz
);
INSERT INTO olos_export.offline_agent_state(singleton,version) VALUES(true,1);
CREATE TABLE olos_export.offline_agents (
 agent_id integer PRIMARY KEY CHECK(agent_id>0),
 user_name text NOT NULL CHECK(length(btrim(user_name))>0 AND length(user_name)<=510)
);
-- Provenance and ownership fence for the name only; no business columns added.
CREATE TABLE olos_export.offline_agent_links (
 call_id bigint PRIMARY KEY,
 agent_id integer NOT NULL CHECK(agent_id>0),
 applied_name_hash text CHECK(applied_name_hash ~ '^[0-9a-f]{64}$')
);
GRANT SELECT,UPDATE ON olos_export.offline_agent_state TO olos_offline_homolog;
GRANT SELECT,INSERT,UPDATE,DELETE ON olos_export.offline_agents TO olos_offline_homolog;
GRANT SELECT,INSERT,UPDATE ON olos_export.offline_agent_links TO olos_offline_homolog;
COMMIT;
