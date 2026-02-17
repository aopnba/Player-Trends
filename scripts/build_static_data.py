#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.static import players as static_players
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NBA_BASE = "https://stats.nba.com/stats"
SEASON_TYPES = ["Regular Season", "Playoffs"]
DEFAULT_SEASONS = ["2025-26", "2024-25", "2023-24"]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}


def season_type_slug(value: str) -> str:
    return re.sub(r"\s+", "-", value.strip().lower())


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504, 520, 522, 524],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def fetch_endpoint(session: requests.Session, endpoint: str, params: dict[str, Any], timeout: int = 40) -> dict[str, Any]:
    url = f"{NBA_BASE}/{endpoint}"
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def extract_rows(payload: dict[str, Any], set_name: str | None = None) -> list[dict[str, Any]]:
    if "resultSets" in payload and payload["resultSets"]:
        sets = payload["resultSets"]
        selected = sets[0]
        if set_name:
            for candidate in sets:
                if str(candidate.get("name", "")).lower() == set_name.lower():
                    selected = candidate
                    break
        headers = selected.get("headers", [])
        row_set = selected.get("rowSet", [])
    else:
        selected = payload.get("resultSet", {})
        headers = selected.get("headers", [])
        row_set = selected.get("rowSet", [])

    rows: list[dict[str, Any]] = []
    for row in row_set:
        record = {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
        rows.append(record)
    return rows


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def infer_stat_fields(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []

    blacklist = {"PLAYER_ID", "TEAM_ID", "GAME_ID", "GAME_DATE_EST"}
    out: list[str] = []
    for key in rows[0].keys():
        if key in blacklist or key.endswith("_RANK"):
            continue
        if any(to_float(row.get(key)) is not None for row in rows):
            out.append(key)
    return sorted(set(out))


def build_players_from_static_roster(season: str) -> dict[str, Any]:
    roster = static_players.get_active_players()
    players: list[dict[str, Any]] = []
    for row in roster:
        try:
            player_id = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        full_name = row.get("full_name") or f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
        players.append(
            {
                "player_id": player_id,
                "name": full_name,
                "team_id": None,
                "team": None,
                "is_active": True,
                "headshot_url": f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png",
            }
        )
    players.sort(key=lambda x: (x["name"] or "", x["player_id"]))
    return {"season": season, "count": len(players), "players": players}


def build_gamelogs(season: str, season_type: str) -> dict[str, Any]:
    payload = None
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            endpoint = leaguegamelog.LeagueGameLog(
                counter=0,
                direction="ASC",
                league_id="00",
                player_or_team_abbreviation="P",
                season=season,
                season_type_all_star=season_type,
                sorter="DATE",
                date_from_nullable="",
                date_to_nullable="",
                timeout=90,
                get_request=True,
            )
            payload = endpoint.get_dict()
            break
        except Exception as err:
            last_error = err
            sleep_for = min(20, attempt * 4)
            print(f"[warn] leaguegamelog failed {season} {season_type} attempt {attempt}/5: {err}", flush=True)
            time.sleep(sleep_for)
    if payload is None:
        raise RuntimeError(f"leaguegamelog failed for {season} {season_type}: {last_error}")

    rows = extract_rows(payload, "LeagueGameLog")

    rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    stat_fields = infer_stat_fields(rows)

    return {
        "season": season,
        "season_type": season_type,
        "count": len(rows),
        "stat_fields": stat_fields,
        "rows": rows,
    }


def dump_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def parse_seasons(raw: str | None) -> list[str]:
    if not raw:
        return DEFAULT_SEASONS
    out = [x.strip() for x in raw.split(",") if x.strip()]
    return out or DEFAULT_SEASONS


def main() -> None:
    parser = argparse.ArgumentParser(description="Build static NBA data files for GitHub Pages")
    parser.add_argument("--output", default="frontend/public/data", help="Output data directory")
    parser.add_argument("--seasons", default=os.getenv("STATIC_SEASONS", ""), help="Comma separated seasons")
    parser.add_argument("--default-season", default=os.getenv("DEFAULT_SEASON", "2025-26"))
    args = parser.parse_args()

    seasons = parse_seasons(args.seasons)
    output_root = Path(args.output).resolve()

    files_players: dict[str, str] = {}
    files_gamelogs: dict[str, dict[str, str]] = {}

    for season in seasons:
        print(f"[build] players {season}", flush=True)
        players_payload = build_players_from_static_roster(season)
        players_rel = f"players/{season}.json"
        dump_json(output_root / players_rel, players_payload)
        files_players[season] = players_rel

        files_gamelogs[season] = {}
        for season_type in SEASON_TYPES:
            slug = season_type_slug(season_type)
            print(f"[build] gamelogs {season} {season_type}", flush=True)
            gamelog_payload = build_gamelogs(season, season_type)
            if season == args.default_season and season_type == "Regular Season" and int(gamelog_payload["count"]) == 0:
                raise RuntimeError(f"No LeagueGameLog rows returned for {season} {season_type}")
            rel = f"gamelogs/{season}/{slug}.json"
            dump_json(output_root / rel, gamelog_payload)
            files_gamelogs[season][slug] = rel
            time.sleep(1.5)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "default_season": args.default_season if args.default_season in seasons else seasons[0],
        "seasons": seasons,
        "season_types": SEASON_TYPES,
        "files": {
            "players": files_players,
            "gamelogs": files_gamelogs,
        },
    }
    dump_json(output_root / "manifest.json", manifest)
    print("[build] done", flush=True)


if __name__ == "__main__":
    main()
