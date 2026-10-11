# projet-tunnel-ai

AI gateway dashboard that routes coding tools (Claude Code, Codex, Cursor, Cline, OpenCode…) to **free AI models** through one OpenAI-compatible endpoint.

Based on [FlagshipRouter](https://github.com/theRizwan/FlagshipRouter) v1.0.0 (see [LICENSE](LICENSE)), with small adaptations for the Z.ai sandbox preview:

- `skipTrailingSlashRedirect: true` — avoids a redirect loop behind the preview edge
- `allowedDevOrigins: ["*.space-z.ai"]` — allows dev assets from the preview host

## Run locally

```bash
bun install
bun run dev                      # dev server (port from package.json)
# or production:
next build --webpack
node custom-server.js --port 3000
```

Dashboard: http://localhost:3000 — first password comes from the `INITIAL_PASSWORD` env var (default `123456` is rejected for remote clients by design; set `INITIAL_PASSWORD` when deploying).

## Deploy on Vercel

The dashboard deploys as a standard Next.js app. Note that host-bound features (SQLite file persistence across cold starts, local tunnels/cloudflared, background refresh) are limited on serverless — for the full gateway experience run it on your own machine.
