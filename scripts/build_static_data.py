#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NBA_BASE = "https://stats.nba.com/stats"
NBA_CDN_SCHEDULE = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
NBA_CDN_BOXSCORE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
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


def fetch_json_via_curl(url: str, timeout: int = 35, attempts: int = 4) -> dict[str, Any]:
    last_error = ""
    for attempt in range(1, attempts + 1):
        proc = subprocess.run(
            [
                "curl",
                "-fsSL",
                "--max-time",
                str(timeout),
                "--retry",
                "2",
                "--retry-delay",
                "1",
                url,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return json.loads(proc.stdout)
        last_error = proc.stderr.strip() or f"curl exit {proc.returncode}"
        time.sleep(min(1.5 * attempt, 4.0))
    raise RuntimeError(f"curl fetch failed for {url}: {last_error}")


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
        payload = fetch_endpoint(
            session,
            "commonallplayers",
            {
                "LeagueID": "00",
                "Season": season,
                "IsOnlyCurrentSeason": "1",
            },
            timeout=45,
        )
        rows = extract_rows(payload, "CommonAllPlayers")
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


def parse_minutes(value: Any) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 2:
                mm = float(parts[0])
                ss = float(parts[1])
                return mm + ss / 60.0
        except ValueError:
            return 0.0
    if s.startswith("PT"):
        m = re.search(r"(\d+)M", s)
        sec = re.search(r"(\d+(?:\.\d+)?)S", s)
        mm = float(m.group(1)) if m else 0.0
        ss = float(sec.group(1)) if sec else 0.0
        return mm + ss / 60.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def build_gamelogs_from_cdn(season: str, season_type: str) -> list[dict[str, Any]]:
    schedule = fetch_json_via_curl(NBA_CDN_SCHEDULE, timeout=45, attempts=5)
    game_dates = schedule.get("leagueSchedule", {}).get("gameDates", [])
    stage_target = 2 if season_type == "Regular Season" else 4
    rows: list[dict[str, Any]] = []

    game_ids: list[str] = []
    season_start = season.split("-")[0]
    for game_day in game_dates:
        for game in game_day.get("games", []):
            game_id = str(game.get("gameId") or "")
            if not game_id.startswith("00"):
                continue
            if not str(game.get("gameDateEst") or "").startswith(season_start):
                # Schedule file spans many seasons; keep selected season year.
                continue
            stage_id = int(game.get("seasonStageId") or 0)
            if stage_id != stage_target:
                continue
            if int(game.get("gameStatus") or 0) != 3:
                continue
            game_ids.append(game_id)

    game_ids = sorted(set(game_ids))
    total_games = len(game_ids)
    print(f"[build] cdn boxscore games {season} {season_type}: {total_games}", flush=True)

    for idx, game_id in enumerate(game_ids, start=1):
        if idx == 1 or idx % 50 == 0 or idx == total_games:
            print(f"[build] cdn boxscore {season} {season_type}: {idx}/{total_games}", flush=True)
        try:
            payload = fetch_json_via_curl(NBA_CDN_BOXSCORE.format(game_id=game_id), timeout=30, attempts=4)
        except Exception as exc:
            print(f"[warn] boxscore failed {game_id}: {exc}", flush=True)
            continue

        game = payload.get("game", {})
        game_date = str(game.get("gameEt") or game.get("gameTimeUTC") or "")[:10]
        home = game.get("homeTeam", {}) or {}
        away = game.get("awayTeam", {}) or {}
        home_score = int(home.get("score") or 0)
        away_score = int(away.get("score") or 0)

        def emit_player(team_obj: dict[str, Any], opp_obj: dict[str, Any], is_home: bool) -> None:
            team_abbr = str(team_obj.get("teamTricode") or "")
            opp_abbr = str(opp_obj.get("teamTricode") or "")
            matchup = f"{team_abbr} vs. {opp_abbr}" if is_home else f"{team_abbr} @ {opp_abbr}"
            wl = "W" if (home_score > away_score and is_home) or (away_score > home_score and not is_home) else "L"
            for p in team_obj.get("players", []) or []:
                stats = p.get("statistics", {}) or {}
                pid = int(p.get("personId") or 0)
                if pid <= 0:
                    continue
                row = {
                    "SEASON_YEAR": season,
                    "PLAYER_ID": pid,
                    "PLAYER_NAME": p.get("name") or f"{p.get('firstName', '')} {p.get('familyName', '')}".strip(),
                    "TEAM_ID": team_obj.get("teamId"),
                    "TEAM_ABBREVIATION": team_abbr,
                    "TEAM_NAME": team_obj.get("teamName"),
                    "GAME_ID": game_id,
                    "GAME_DATE": game_date,
                    "MATCHUP": matchup,
                    "WL": wl,
                    "MIN": parse_minutes(stats.get("minutes")),
                    "FGM": stats.get("fieldGoalsMade"),
                    "FGA": stats.get("fieldGoalsAttempted"),
                    "FG_PCT": stats.get("fieldGoalsPercentage"),
                    "FG3M": stats.get("threePointersMade"),
                    "FG3A": stats.get("threePointersAttempted"),
                    "FG3_PCT": stats.get("threePointersPercentage"),
                    "FTM": stats.get("freeThrowsMade"),
                    "FTA": stats.get("freeThrowsAttempted"),
                    "FT_PCT": stats.get("freeThrowsPercentage"),
                    "OREB": stats.get("reboundsOffensive"),
                    "DREB": stats.get("reboundsDefensive"),
                    "REB": stats.get("reboundsTotal"),
                    "AST": stats.get("assists"),
                    "TOV": stats.get("turnovers"),
                    "STL": stats.get("steals"),
                    "BLK": stats.get("blocks"),
                    "PF": stats.get("foulsPersonal"),
                    "PTS": stats.get("points"),
                    "PLUS_MINUS": stats.get("plusMinusPoints"),
                }
                rows.append(row)

        emit_player(home, away, True)
        emit_player(away, home, False)
        time.sleep(0.03)

    return rows


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

        try:
            payload = fetch_endpoint(
                session,
                "playergamelog",
                {
                    "PlayerID": player_id,
                    "Season": season,
                    "SeasonType": season_type,
                    "LeagueID": "00",
                },
                timeout=35,
            )
        except requests.RequestException as exc:
            print(f"[warn] playergamelog failed for {player_id}: {exc}", flush=True)
            continue

        player_rows = extract_rows(payload, "PlayerGameLog")
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


def build_gamelogs(session: requests.Session, season: str, season_type: str, players_payload: dict[str, Any]) -> dict[str, Any]:
    players = players_payload.get("players", [])
    active_player_ids = [int(p.get("player_id") or 0) for p in players if bool(p.get("is_active")) and p.get("player_id")]
    active_player_ids = sorted({pid for pid in active_player_ids if pid > 0})

    rows: list[dict[str, Any]] = []
    try:
        rows = build_gamelogs_league(session, season, season_type)
        print(f"[build] leaguegamelog rows: {len(rows)}", flush=True)
    except requests.RequestException as exc:
        print(f"[warn] leaguegamelog failed for {season} {season_type}: {exc}", flush=True)

    row_player_ids = {int(r.get("PLAYER_ID") or 0) for r in rows if r.get("PLAYER_ID")}
    missing_player_ids = [pid for pid in active_player_ids if pid not in row_player_ids]

    # If league-level request is clearly partial, backfill with per-player logs.
    if not rows or (season_type == "Regular Season" and len(missing_player_ids) > 25):
        print(
            f"[build] backfill via playergamelog for {len(missing_player_ids)} missing players "
            f"({season} {season_type})",
            flush=True,
        )
        rows.extend(build_gamelogs_per_player(session, season, season_type, missing_player_ids))

    # Final fallback: rebuild from NBA CDN boxscores if coverage is still poor.
    row_player_ids = {int(r.get("PLAYER_ID") or 0) for r in rows if r.get("PLAYER_ID")}
    covered = len(set(active_player_ids) & row_player_ids)
    coverage = (covered / len(active_player_ids)) if active_player_ids else 0.0
    if season_type == "Regular Season" and (covered < 150 or coverage < 0.55):
        print(
            f"[build] stats endpoint coverage still low ({covered}/{len(active_player_ids)}), "
            f"switching to CDN boxscore build for {season} {season_type}",
            flush=True,
        )
        rows = build_gamelogs_from_cdn(season, season_type)

    rows = dedupe_rows(rows)
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
    if covered < 150 or coverage < 0.55:
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
