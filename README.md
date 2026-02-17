# NBA Player Trends (Static + Daily Auto-Refresh)

This repo is now set up to run as a **static GitHub Pages app** with **daily NBA data refresh**.

## How it works

- Frontend: React/Vite in `frontend/`
- Static data files: `frontend/public/data/`
- Daily refresh job: `.github/workflows/update-static-data.yml`
- Pages deploy job: `.github/workflows/deploy-pages.yml`

At ~4:00 AM ET each day, GitHub Actions pulls fresh NBA data and commits updated JSON files.
A push triggers GitHub Pages deploy automatically.

## Data source endpoints used in the refresh script

- `commonallplayers`
- `playergamelogs`

Script path:

- `scripts/build_static_data.py`

## Local frontend run

```bash
cd "/Users/atticusobp/Desktop/player_headshots_26 copy/frontend"
npm ci
npm run dev
```

Local URL:

- `http://127.0.0.1:5173`

## Manual data refresh (local)

```bash
cd "/Users/atticusobp/Desktop/player_headshots_26 copy"
python3 -m pip install -r scripts/requirements-static.txt
python3 scripts/build_static_data.py --output frontend/public/data
```

## GitHub setup checklist

1. Keep repo on `main`.
2. In GitHub, enable Pages for this repo.
   - Source: `GitHub Actions`
3. (Optional) Run `Update Static NBA Data` workflow manually once to seed full data immediately.
4. Wait for `Deploy Frontend to GitHub Pages` workflow to finish.

Your live app URL should be:

- `https://aopnba.github.io/Player-Trends/`

## Notes

- Headshots are pulled from NBA CDN by `player_id`.
- If NBA rate-limits a refresh run, the action retries HTTP requests automatically.
- The schedule uses two UTC runs and only executes at actual 4:00 AM New York time.
