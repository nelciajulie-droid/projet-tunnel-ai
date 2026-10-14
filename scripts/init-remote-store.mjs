// One-shot tool: initialise the remote D1 store (schema + seed key) and run
// sanity checks through the REAL app adapter. Run with bun from repo root:
//   FLAGSHIPROUTER_REMOTE_STORE_URL=... FLAGSHIPROUTER_REMOTE_STORE_TOKEN=... bun scripts/init-remote-store.mjs
import { createRemoteHttpAdapter } from "../src/lib/db/adapters/remoteHttpAdapter.js";
import { runMigrationOnce } from "../src/lib/db/migrate.js";

const url = process.env.FLAGSHIPROUTER_REMOTE_STORE_URL;
const token = process.env.FLAGSHIPROUTER_REMOTE_STORE_TOKEN;
if (!url || !token) {
  console.error("Missing FLAGSHIPROUTER_REMOTE_STORE_URL / FLAGSHIPROUTER_REMOTE_STORE_TOKEN");
  process.exit(1);
}

const SEED_KEY = process.env.FLAGSHIPROUTER_SEED_API_KEY || "";

console.log(`[init] store: ${url}`);
const adapter = await createRemoteHttpAdapter({ url, token });
console.log(`[init] adapter ready (driver=${adapter.driver})`);

// 1. Schema (CREATE TABLE IF NOT EXISTS — idempotent, safe to re-run)
console.time("[init] migrations");
await runMigrationOnce(adapter);
console.timeEnd("[init] migrations");

// 2. Seed the user's API key so /v1 works immediately (idempotent)
if (SEED_KEY) {
  const row = adapter.get(`SELECT id, isActive FROM apiKeys WHERE key = ?`, [SEED_KEY]);
  if (!row) {
    adapter.run(
      `INSERT INTO apiKeys(id, key, name, machineId, isActive, createdAt) VALUES(?, ?, ?, ?, ?, ?)`,
      [crypto.randomUUID(), SEED_KEY, "seeded", null, 1, new Date().toISOString()]
    );
    console.log(`[init] seeded API key ...${SEED_KEY.slice(-6)}`);
  } else {
    console.log(`[init] seed key already present (isActive=${row.isActive})`);
  }
}

// 3. Sanity checks
const tables = adapter.all(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`).map((r) => r.name);
console.log(`[init] tables (${tables.length}):`, tables.join(", "));

const cols = adapter.all(`PRAGMA table_info(apiKeys)`);
console.log(`[init] PRAGMA table_info(apiKeys) →`, cols.map((c) => c.name).join(", "));

const keyRow = adapter.get(`SELECT key, name, isActive FROM apiKeys LIMIT 1`);
console.log(`[init] apiKeys row:`, keyRow ? `...${String(keyRow.key).slice(-6)} (${keyRow.name}, active=${keyRow.isActive})` : "none");

adapter.run(`INSERT INTO _meta(key, value) VALUES('initCheck', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [new Date().toISOString()]);
console.log(`[init] _meta write/read:`, adapter.get(`SELECT value FROM _meta WHERE key='initCheck'`)?.value);

// 4. transaction(fn) shape used by repos
const txResult = adapter.transaction(() => {
  adapter.run(`INSERT INTO _meta(key, value) VALUES('txCheck', 'ok') ON CONFLICT(key) DO UPDATE SET value='ok'`);
  return adapter.get(`SELECT value FROM _meta WHERE key='txCheck'`)?.value;
});
console.log(`[init] transaction(fn):`, txResult);

adapter.close();
console.log(`[init] DONE ✔`);
