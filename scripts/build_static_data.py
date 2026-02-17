#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NBA_BASE = "https://stats.nba.com/stats"
DEFAULT_SEASONS = ["2025-26"]
SEASON_TYPES = ["Regular Season", "Playoffs"]

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


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        read=4,
        connect=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504, 520, 522, 524],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def season_type_slug(value: str) -> str:
    return re.sub(r"\s+", "-", value.strip().lower())


def parse_seasons(raw: str | None) -> list[str]:
    if not raw:
        return DEFAULT_SEASONS
    out = [x.strip() for x in raw.split(",") if x.strip()]
    return out or DEFAULT_SEASONS


def _extract_leaguegamelog_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result_sets = payload.get("resultSets") or []
    if result_sets:
        selected = result_sets[0]
        for rs in result_sets:
            if str(rs.get("name", "")).lower() == "leaguegamelog":
                selected = rs
                break
        headers = selected.get("headers", [])
        row_set = selected.get("rowSet", [])
    else:
        rs = payload.get("resultSet", {})
        headers = rs.get("headers", [])
        row_set = rs.get("rowSet", [])

    rows: list[dict[str, Any]] = []
    for row in row_set:
        rec = {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
        if "PLAYER_ID" not in rec and "Player_ID" in rec:
            rec["PLAYER_ID"] = rec.get("Player_ID")
        rows.append(rec)
    return rows


def fetch_leaguegamelog_all(session: requests.Session, season: str, season_type: str) -> list[dict[str, Any]]:
    url = f"{NBA_BASE}/leaguegamelog"
    params = {
        "Counter": 1000,
        "DateFrom": None,
        "DateTo": None,
        "Direction": "DESC",
        "LeagueID": "00",
        "PlayerOrTeam": "P",
        "Season": season,
        "SeasonType": season_type,
        "Sorter": "DATE",
    }
    resp = session.get(url, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    rows = _extract_leaguegamelog_rows(payload)
    print(f"[build] counter=1000 rows={len(rows)}", flush=True)
    return rows


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        pid = int(r.get("PLAYER_ID") or 0)
        gid = str(r.get("GAME_ID") or "")
        if pid > 0 and gid:
            key = (pid, gid)
            if key in seen:
                continue
            seen.add(key)
        out.append(r)
    return out


def infer_stat_fields(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    blacklist = {"PLAYER_ID", "TEAM_ID", "GAME_ID"}
    keys = set()
    for r in rows:
        keys.update(r.keys())
    out: list[str] = []
    for k in sorted(keys):
        if k in blacklist or k.endswith("_RANK"):
            continue
        numeric = False
        for r in rows:
            v = r.get(k)
            try:
                float(v)
                numeric = True
                break
            except (TypeError, ValueError):
                continue
        if numeric:
            out.append(k)
    return out


def build_players_from_rows(season: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    # Use latest seen row per player to capture current team assignment.
    by_id: dict[int, dict[str, Any]] = {}

    def dt_key(x: dict[str, Any]) -> str:
        return str(x.get("GAME_DATE") or "")

    for r in sorted(rows, key=dt_key):
        try:
            pid = int(r.get("PLAYER_ID"))
        except (TypeError, ValueError):
            continue
        by_id[pid] = {
            "player_id": pid,
            "name": r.get("PLAYER_NAME"),
            "team_id": r.get("TEAM_ID"),
            "team": r.get("TEAM_ABBREVIATION"),
            "is_active": True,
            "headshot_url": f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png",
        }

    players = sorted(by_id.values(), key=lambda x: (x.get("name") or "", x["player_id"]))
    return {"season": season, "count": len(players), "players": players}


def dump_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def build_season_type(session: requests.Session, season: str, season_type: str) -> dict[str, Any]:
    print(f"[build] leaguegamelog all dates {season} {season_type}", flush=True)
    all_rows = fetch_leaguegamelog_all(session, season, season_type)
    all_rows = dedupe_rows(all_rows)
    all_rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    stat_fields = infer_stat_fields(all_rows)
    unique_players = len({int(r.get("PLAYER_ID") or 0) for r in all_rows if r.get("PLAYER_ID") is not None})
    print(f"[build] rows {season} {season_type}: {len(all_rows)} players={unique_players}", flush=True)

    # Guardrail: never publish a tiny partial pull.
    if season_type == "Regular Season" and unique_players < 200:
        raise RuntimeError(
            f"LeagueGameLog returned partial data: only {unique_players} unique players "
            f"for {season} {season_type}. Aborting publish."
        )

    return {
        "season": season,
        "season_type": season_type,
        "count": len(all_rows),
        "stat_fields": stat_fields,
        "rows": all_rows,
        "failed_days": [],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build static NBA data files using LeagueGameLog")
    parser.add_argument("--output", default="frontend/public/data", help="Output data directory")
    parser.add_argument("--seasons", default=os.getenv("STATIC_SEASONS", ""), help="Comma-separated seasons")
    parser.add_argument("--default-season", default=os.getenv("DEFAULT_SEASON", "2025-26"))
    args = parser.parse_args()

    seasons = parse_seasons(args.seasons)
    output_root = Path(args.output).resolve()
    session = make_session()

    files_players: dict[str, str] = {}
    files_gamelogs: dict[str, dict[str, str]] = {}

    for season in seasons:
        files_gamelogs[season] = {}
        season_rows_for_players: list[dict[str, Any]] = []

        for season_type in SEASON_TYPES:
            slug = season_type_slug(season_type)
            payload = build_season_type(session, season, season_type)
            rel = f"gamelogs/{season}/{slug}.json"
            dump_json(output_root / rel, payload)
            files_gamelogs[season][slug] = rel
            if season_type == "Regular Season":
                season_rows_for_players.extend(payload["rows"])

        players_payload = build_players_from_rows(season, season_rows_for_players)
        players_rel = f"players/{season}.json"
        dump_json(output_root / players_rel, players_payload)
        files_players[season] = players_rel

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "default_season": args.default_season if args.default_season in seasons else seasons[0],
        "seasons": seasons,
        "season_types": SEASON_TYPES,
        "files": {"players": files_players, "gamelogs": files_gamelogs},
    }
    dump_json(output_root / "manifest.json", manifest)
    print("[build] done", flush=True)


if __name__ == "__main__":
    main()
