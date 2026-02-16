# NBA Player Trends

This project now supports a fully public, free Cloudflare deployment where:

- Frontend is static React (`frontend/`).
- API is served by Cloudflare Pages Functions (`functions/api/...`).
- Live data is pulled from `stats.nba.com` using the same endpoint family.
- Headshots use NBA CDN by `PLAYER_ID` (no laptop file dependency).

## Local Run

Backend (FastAPI) and frontend dev server:

```bash
cd "/Users/atticusobp/Desktop/player_headshots_26 copy"
./run_local.sh
```

Stop:

```bash
./stop_local.sh
```

Local frontend URL:

- `http://127.0.0.1:5173`

## Cloudflare Deploy (Free)

1. Push this repo to GitHub.
2. In Cloudflare Dashboard: `Workers & Pages` -> `Create` -> `Pages` -> `Connect to Git`.
3. Select repo `aopnba/Player-Trends`.
4. Build settings:
- Framework preset: `Vite`
- Build command: `cd frontend && npm ci && npm run build`
- Build output directory: `frontend/dist`
- Root directory: repo root (leave blank)
5. Functions directory:
- Set to `functions`
6. Deploy.

The frontend and API will share the same domain, and frontend calls `/api/...` directly.

## Password Protect Team Access

Use Cloudflare Zero Trust Access:

1. Open Cloudflare Zero Trust.
2. `Access` -> `Applications` -> `Add application` -> `Self-hosted`.
3. Application domain: your Pages URL.
4. Policy: allow only your team emails (or your email domain).

## API Routes (Cloudflare Functions)

- `GET /api/health`
- `GET /api/players?season=2025-26`
- `GET /api/trends/player?player_id=203507&source=overall&season=2025-26&season_type=Regular%20Season`
