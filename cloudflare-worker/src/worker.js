// flagship-store — Cloudflare Worker + D1 durable store for FlagshipRouter.
//
// Purpose: Vercel serverless instances have an ephemeral /tmp filesystem, so the
// app's SQLite database (API keys, provider connections, settings, usage) dies
// with every instance recycle. This Worker exposes a tiny authenticated SQL
// endpoint backed by Cloudflare D1 (SQLite-compatible) so the app can use a
// DURABLE database on serverless hosts via the remoteHttpAdapter.
//
// Security: every statement requires `Authorization: Bearer <STORE_TOKEN>`.
// The token is a shared secret set via `wrangler secret put STORE_TOKEN`.

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
    },
  });
}

function normalizeParam(p) {
  if (p === undefined) return null;
  if (typeof p === "boolean") return p ? 1 : 0;
  if (typeof p === "bigint") return Number(p);
  return p;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Unauthenticated health probe (no data exposure).
    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, store: "flagship-store", time: new Date().toISOString() });
    }

    if (request.method !== "POST") {
      return json({ ok: false, error: "POST only" }, 405);
    }

    const expected = (env.STORE_TOKEN || "").trim();
    const provided = (request.headers.get("authorization") || "").replace(/^Bearer\s+/i, "").trim();
    if (!expected || provided !== expected) {
      return json({ ok: false, error: "unauthorized" }, 401);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return json({ ok: false, error: "invalid JSON body" }, 400);
    }

    const mode = body.mode;
    const sql = typeof body.sql === "string" ? body.sql : "";
    const params = (Array.isArray(body.params) ? body.params : []).map(normalizeParam);

    try {
      // Multi-statement exec (DDL / migrations). D1 exec() runs statements
      // separated by ';' — never use for untrusted input.
      if (mode === "exec") {
        if (!sql.trim()) return json({ ok: false, error: "empty sql" }, 400);
        await env.DB.exec(sql);
        return json({ ok: true, mode: "exec" });
      }

      if (!sql.trim()) return json({ ok: false, error: "empty sql" }, 400);
      const stmt = env.DB.prepare(sql).bind(...params);

      if (mode === "run") {
        const r = await stmt.run();
        return json({
          ok: true,
          mode: "run",
          changes: r.meta?.changes ?? 0,
          lastInsertRowid: r.meta?.last_row_id ?? 0,
        });
      }

      if (mode === "get") {
        const row = await stmt.first();
        // node:sqlite get() returns undefined when there is no row; the
        // adapter maps JSON null back to undefined.
        return json({ ok: true, row: row ?? null });
      }

      if (mode === "all") {
        const r = await stmt.all();
        return json({ ok: true, rows: r.results ?? [] });
      }

      // Atomic multi-statement batch (optional, used by tooling).
      if (mode === "batch") {
        const statements = (Array.isArray(body.statements) ? body.statements : []).map(
          (s) => env.DB.prepare(String(s.sql || "")).bind(...(Array.isArray(s.params) ? s.params : []).map(normalizeParam))
        );
        const results = await env.DB.batch(statements);
        return json({
          ok: true,
          results: results.map((r) => ({
            changes: r.meta?.changes ?? 0,
            lastInsertRowid: r.meta?.last_row_id ?? 0,
            rows: r.results ?? null,
          })),
        });
      }

      return json({ ok: false, error: `unknown mode: ${mode}` }, 400);
    } catch (e) {
      // Return 200 with ok:false so the client adapter can surface the exact
      // D1 error message without retry semantics getting in the way.
      return json({ ok: false, error: (e && e.message) || String(e) });
    }
  },
};
