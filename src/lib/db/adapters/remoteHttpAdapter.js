// Remote HTTP store adapter — durable database on serverless hosts.
//
// When FLAGSHIPROUTER_REMOTE_STORE_URL is set, driver.js selects this adapter
// INSTEAD of a local SQLite file. All statements are forwarded to the
// "flagship-store" Cloudflare Worker (see /cloudflare-worker) which executes
// them on Cloudflare D1 — a real SQLite-compatible database that SURVIVES
// instance recycles (unlike /tmp on Vercel).
//
// The repos API is synchronous (adapter.get()/run()/all() return values, not
// promises), but HTTP fetch is async. We bridge this with a worker thread +
// SharedArrayBuffer + Atomics.wait: the worker performs the async fetch and
// signals completion; the calling thread blocks deterministically until the
// result lands in shared memory. Net effect: exact same sync semantics as
// node:sqlite / better-sqlite3.
//
// SQL compatibility layer:
// - PRAGMA table_info(x)  →  SELECT ... FROM pragma_table_info('x')
//   (D1 exposes pragma functions as table-valued functions)
// - Connection-level statements (journal_mode, BEGIN/COMMIT/SAVEPOINT,
//   ATTACH, VACUUM, wal_checkpoint, ...) are no-ops — they describe local
//   file/driver behaviour that does not apply to a remote store.
// - transaction(fn) runs fn() directly (best-effort). Cross-instance
//   atomicity is provided by D1's serialized single-writer model; repos only
//   rely on transactions for small read-merge-write sequences.

import { Worker } from "node:worker_threads";

const DEFAULT_TIMEOUT_MS = 9000;
// Response buffer: 8-byte header (status:i32, length:i32) + payload.
const DATA_BUFFER_BYTES = 12 * 1024 * 1024;

// Statements that only make sense against a local file/connection — never sent.
const SESSION_SQL_RE = /^\s*(PRAGMA|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|ATTACH|DETACH|VACUUM|ANALYZE|REINDEX)\b/i;

// PRAGMA table_info(x) → pragma_table_info() table-valued function (D1-safe).
const TABLE_INFO_RE = /^\s*PRAGMA\s+table_info\s*\(\s*(['"[]?)([A-Za-z0-9_]+)['"\]]?\s*\)\s*;?\s*$/i;

function rewriteSql(sql) {
  const m = TABLE_INFO_RE.exec(sql);
  if (m) {
    return {
      sql: `SELECT cid, name, type, "notnull", dflt_value, pk FROM pragma_table_info('${m[2]}')`,
      send: true,
    };
  }
  return { sql, send: !SESSION_SQL_RE.test(sql) };
}

function normalizeParam(p) {
  if (p === undefined) return null;
  if (typeof p === "boolean") return p ? 1 : 0;
  if (typeof p === "bigint") return Number(p);
  return p;
}

// ─── Worker thread source (runs the async fetch on our behalf) ───────────
const BRIDGE_WORKER_SOURCE = `
const { workerData, parentPort } = require("node:worker_threads");
const { dataBuf, url, token, timeoutMs } = workerData;
const ctrl = new Int32Array(workerData.ctrlBuf);
const dv = new DataView(dataBuf);
const u8 = new Uint8Array(dataBuf);
const CAP = u8.length - 8;

parentPort.on("message", async (task) => {
  let status = 500;
  let text = JSON.stringify({ ok: false, error: "bridge worker error" });
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "authorization": "Bearer " + token,
        "content-type": "application/json",
      },
      body: task.body,
      signal: AbortSignal.timeout(timeoutMs || 9000),
    });
    status = res.status;
    text = await res.text();
  } catch (e) {
    status = 599;
    text = JSON.stringify({ ok: false, error: "store fetch failed: " + ((e && e.message) || String(e)) });
  }
  try {
    let out = Buffer.from(text, "utf-8");
    if (out.length > CAP) {
      status = 200;
      out = Buffer.from(JSON.stringify({ ok: false, error: "response too large (" + out.length + " bytes)" }), "utf-8");
    }
    dv.setInt32(0, status, true);
    dv.setInt32(4, out.length, true);
    u8.set(out, 8);
    Atomics.store(ctrl, 0, task.id);
    Atomics.notify(ctrl, 0);
  } catch (e) {
    // Last resort: still release the blocked main thread.
    try {
      dv.setInt32(0, 598, true);
      const errText = Buffer.from(JSON.stringify({ ok: false, error: "bridge write failed: " + ((e && e.message) || String(e)) }), "utf-8");
      dv.setInt32(4, errText.length, true);
      u8.set(errText, 8);
      Atomics.store(ctrl, 0, task.id);
      Atomics.notify(ctrl, 0);
    } catch {}
  }
});
`;

export async function createRemoteHttpAdapter({ url, token, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  if (!url) throw new Error("createRemoteHttpAdapter: url is required");

  const ctrlBuf = new SharedArrayBuffer(4);
  const dataBuf = new SharedArrayBuffer(DATA_BUFFER_BYTES);
  const ctrl = new Int32Array(ctrlBuf);
  const dataDv = new DataView(dataBuf);
  const dataU8 = new Uint8Array(dataBuf);

  const worker = new Worker(BRIDGE_WORKER_SOURCE, {
    eval: true,
    workerData: { ctrlBuf, dataBuf, url, token, timeoutMs },
  });

  let workerBroken = false;
  worker.on("error", (e) => {
    workerBroken = true;
    console.warn(`[DB][remote] bridge worker error: ${e.message}`);
  });
  worker.on("exit", (code) => {
    if (code !== 0) workerBroken = true;
  });

  let taskSeq = 0;

  // Synchronous request/response round trip (blocks this thread until the
  // worker thread completes the async fetch and signals via Atomics).
  function call(payload, opTimeoutMs = timeoutMs) {
    if (workerBroken) throw new Error("[store] bridge worker is not running");
    const id = ++taskSeq;
    worker.postMessage({ id, body: JSON.stringify(payload) });

    const start = Date.now();
    for (;;) {
      const current = Atomics.load(ctrl, 0);
      if (current === id) break;
      const waitRes = Atomics.wait(ctrl, 0, current, 250);
      if (waitRes === "timed-out") {
        if (Date.now() - start > opTimeoutMs) {
          throw new Error(`[store] request timed out after ${opTimeoutMs}ms`);
        }
      }
      // "ok" or "not-equal" → re-check ctrl[0] on next iteration
    }

    const status = dataDv.getInt32(0, true);
    const len = dataDv.getInt32(4, true);
    let parsed;
    try {
      parsed = JSON.parse(Buffer.from(dataU8.buffer, dataU8.byteOffset + 8, len).toString("utf-8"));
    } catch {
      throw new Error(`[store] malformed response (HTTP ${status})`);
    }
    if (status >= 400 || parsed.ok === false) {
      throw new Error(`[store] ${parsed.error || `HTTP ${status}`}`);
    }
    return parsed;
  }

  return {
    driver: "remote-http(d1)",

    run(sql, params = []) {
      const r = rewriteSql(sql);
      if (!r.send) return { changes: 0, lastInsertRowid: 0 };
      const res = call({ mode: "run", sql: r.sql, params: params.map(normalizeParam) });
      return {
        changes: Number(res.changes ?? 0),
        lastInsertRowid: Number(res.lastInsertRowid ?? 0),
      };
    },

    get(sql, params = []) {
      const r = rewriteSql(sql);
      if (!r.send) return undefined;
      const res = call({ mode: "get", sql: r.sql, params: params.map(normalizeParam) });
      return res.row ?? undefined; // JSON null → undefined (node:sqlite semantics)
    },

    all(sql, params = []) {
      const r = rewriteSql(sql);
      if (!r.send) return [];
      const res = call({ mode: "all", sql: r.sql, params: params.map(normalizeParam) });
      return res.rows ?? [];
    },

    exec(sql) {
      const r = rewriteSql(sql);
      if (!r.send) return;
      const single = !/;\s*\S/.test(r.sql);
      // Single statement → run() (safer than D1 exec()); multi-statement → D1 exec().
      call({ mode: single ? "run" : "exec", sql: r.sql });
    },

    transaction(fn) {
      // Best-effort: no remote SAVEPOINTs. D1 serializes writes; repos use
      // transactions only for small read-merge-write sequences.
      return fn();
    },

    checkpoint() { /* WAL is managed by D1 */ },

    close() {
      try { worker.terminate(); } catch {}
    },

    raw: null, // no local handle (backupDbLite paths are skipped/catch-guarded)
  };
}
