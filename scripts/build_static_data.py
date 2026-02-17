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

import requests
from nba_api.stats.endpoints import commonallplayers, playergamelogs
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
        total=2,
        read=2,
        connect=2,
        backoff_factor=0.6,
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


def build_players(session: requests.Session, season: str, output_root: Path) -> dict[str, Any]:
    try:
        endpoint = commonallplayers.CommonAllPlayers(
            is_only_current_season=1,
            season=season,
            timeout=45,
        )
        frames = endpoint.get_data_frames()
        if not frames:
            raise RuntimeError("No frames returned from commonallplayers")
        rows = frames[0].to_dict(orient="records")
        players = []
        for row in rows:
            try:
                player_id = int(row.get("PERSON_ID"))
            except (TypeError, ValueError):
                continue
            players.append(
                {
                    "player_id": player_id,
                    "name": row.get("DISPLAY_FIRST_LAST"),
                    "team_id": row.get("TEAM_ID"),
                    "team": row.get("TEAM_ABBREVIATION"),
                    "is_active": int(row.get("ROSTERSTATUS") or 0) == 1,
                    "headshot_url": f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png",
                }
            )

        players.sort(key=lambda x: (x["name"] or "", x["player_id"]))
        return {
            "season": season,
            "count": len(players),
            "players": players,
        }
    except Exception as exc:
        fallback_path = output_root / "players" / f"{season}.json"
        if fallback_path.exists():
            print(
                f"[warn] commonallplayers failed for {season}, using cached players file: {exc}",
                flush=True,
            )
            return json.loads(fallback_path.read_text(encoding="utf-8"))
        raise


def build_gamelogs_league(session: requests.Session, season: str, season_type: str) -> list[dict[str, Any]]:
    payload = fetch_endpoint(
        session,
        "leaguegamelog",
        {
            "Counter": 0,
            "DateFrom": "",
            "DateTo": "",
            "Direction": "ASC",
            "LeagueID": "00",
            "PlayerOrTeam": "P",
            "Season": season,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
        timeout=70,
    )
    rows = extract_rows(payload, "LeagueGameLog")
    for row in rows:
        if "PLAYER_ID" in row:
            continue
        if "Player_ID" in row:
            row["PLAYER_ID"] = row.get("Player_ID")
    return rows


def build_gamelogs_per_player(
    session: requests.Session,
    season: str,
    season_type: str,
    player_ids: list[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    total = len(player_ids)
    for idx, player_id in enumerate(player_ids, start=1):
        # Progress every 25 players so Actions logs show forward movement.
        if idx == 1 or idx % 25 == 0 or idx == total:
            print(f"[build] playergamelog {season} {season_type}: {idx}/{total}", flush=True)

        player_rows: list[dict[str, Any]] = []
        last_exc = None
        for attempt in range(1, 4):
            try:
                endpoint = playergamelogs.PlayerGameLogs(
                    player_id_nullable=player_id,
                    season_nullable=season,
                    season_type_nullable=season_type,
                    timeout=35,
                )
                frames = endpoint.get_data_frames()
                if frames:
                    player_rows = frames[0].to_dict(orient="records")
                break
            except Exception as exc:
                last_exc = exc
                time.sleep(min(0.8 * attempt, 2.0))
        if not player_rows and last_exc is not None:
            print(f"[warn] playergamelogs failed for {player_id}: {last_exc}", flush=True)
            continue

        for row in player_rows:
            if "PLAYER_ID" not in row:
                row["PLAYER_ID"] = player_id
        rows.extend(player_rows)
        time.sleep(0.08)
    return rows


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        pid = int(row.get("PLAYER_ID") or 0)
        game_id = str(row.get("GAME_ID") or "")
        key = (pid, game_id)
        if pid <= 0 or not game_id:
            # Keep imperfect rows so we do not silently drop data shape.
            out.append(row)
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def season_start_date(season: str) -> str:
    m = re.match(r"^(\d{4})-", season or "")
    if not m:
        return "2025-10-01"
    return f"{m.group(1)}-10-01"


def ensure_player_rows(
    rows: list[dict[str, Any]],
    players_payload: dict[str, Any],
    season: str,
    season_type: str,
) -> list[dict[str, Any]]:
    # Guarantee at least one row per active player to avoid "No rows" UX failures.
    if season_type != "Regular Season":
        return rows

    by_id = {int(r.get("PLAYER_ID") or 0) for r in rows if r.get("PLAYER_ID") is not None}
    out = list(rows)
    start_date = season_start_date(season)

    for p in players_payload.get("players", []):
        pid = int(p.get("player_id") or 0)
        if not p.get("is_active") or pid <= 0 or pid in by_id:
            continue
        out.append(
            {
                "SEASON_YEAR": season,
                "PLAYER_ID": pid,
                "PLAYER_NAME": p.get("name"),
                "TEAM_ID": p.get("team_id"),
                "TEAM_ABBREVIATION": p.get("team"),
                "TEAM_NAME": "",
                "GAME_ID": f"NO_GAME_{pid}",
                "GAME_DATE": start_date,
                "MATCHUP": "NO GAMES YET",
                "WL": "",
                "MIN": 0,
                "FGM": 0,
                "FGA": 0,
                "FG_PCT": 0,
                "FG3M": 0,
                "FG3A": 0,
                "FG3_PCT": 0,
                "FTM": 0,
                "FTA": 0,
                "FT_PCT": 0,
                "OREB": 0,
                "DREB": 0,
                "REB": 0,
                "AST": 0,
                "TOV": 0,
                "STL": 0,
                "BLK": 0,
                "PF": 0,
                "PTS": 0,
                "PLUS_MINUS": 0,
                "IS_PLACEHOLDER": 1,
            }
        )
    return out


def build_gamelogs(session: requests.Session, season: str, season_type: str, players_payload: dict[str, Any]) -> dict[str, Any]:
    players = players_payload.get("players", [])
    active_player_ids = [int(p.get("player_id") or 0) for p in players if bool(p.get("is_active")) and p.get("player_id")]
    active_player_ids = sorted({pid for pid in active_player_ids if pid > 0})

    print(f"[build] playergamelogs {season} {season_type}: {len(active_player_ids)} active players", flush=True)
    rows = build_gamelogs_per_player(session, season, season_type, active_player_ids)

    rows = dedupe_rows(rows)
    rows = ensure_player_rows(rows, players_payload, season, season_type)
    rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    stat_fields = infer_stat_fields(rows)

    return {
        "season": season,
        "season_type": season_type,
        "count": len(rows),
        "stat_fields": stat_fields,
        "rows": rows,
    }


def validate_coverage(
    season: str,
    season_type: str,
    players_payload: dict[str, Any],
    gamelog_payload: dict[str, Any],
) -> None:
    """Fail the build if static gamelog coverage is clearly incomplete."""
    players = players_payload.get("players", [])
    rows = gamelog_payload.get("rows", [])
    row_player_ids = {int(r.get("PLAYER_ID") or 0) for r in rows if r.get("PLAYER_ID") is not None}

    # Only check active players in regular season.
    if season_type != "Regular Season":
        return

    active_player_ids = {
        int(p.get("player_id") or 0)
        for p in players
        if bool(p.get("is_active")) and p.get("player_id") is not None
    }
    covered = len(active_player_ids & row_player_ids)
    active_total = len(active_player_ids)
    coverage = (covered / active_total) if active_total else 0.0

    # Threshold to avoid publishing obviously partial builds.
    if covered < 50 or coverage < 0.15:
        raise RuntimeError(
            f"Incomplete gamelog coverage for {season} {season_type}: "
            f"{covered}/{active_total} active players with rows ({coverage:.1%}). "
            "Aborting publish to avoid shipping partial data."
        )


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

    session = make_session()
    files_players: dict[str, str] = {}
    files_gamelogs: dict[str, dict[str, str]] = {}

    for season in seasons:
        print(f"[build] players {season}", flush=True)
        players_payload = build_players(session, season, output_root)
        players_rel = f"players/{season}.json"
        dump_json(output_root / players_rel, players_payload)
        files_players[season] = players_rel

        files_gamelogs[season] = {}
        for season_type in SEASON_TYPES:
            slug = season_type_slug(season_type)
            print(f"[build] gamelogs {season} {season_type}", flush=True)
            gamelog_payload = build_gamelogs(session, season, season_type, players_payload)
            validate_coverage(season, season_type, players_payload, gamelog_payload)
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
