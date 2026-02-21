#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from nba_api.stats.endpoints import leaguegamelog

SEASON_TYPES = ["Regular Season", "Playoffs"]
DEFAULT_SEASONS = ["2025-26", "2024-25", "2023-24"]

def season_type_slug(value: str) -> str:
    return re.sub(r"\s+", "-", value.strip().lower())


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


def build_players_from_gamelogs(season: str, gamelog_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    by_player: dict[int, dict[str, Any]] = {}
    for payload in gamelog_payloads:
        for row in payload.get("rows", []):
            try:
                player_id = int(row.get("PLAYER_ID"))
            except (TypeError, ValueError):
                continue
            name = str(row.get("PLAYER_NAME") or "").strip()
            team_id = row.get("TEAM_ID")
            team = row.get("TEAM_ABBREVIATION")
            current = by_player.get(player_id)
            if current is None:
                by_player[player_id] = {
                    "player_id": player_id,
                    "name": name,
                    "team_id": team_id,
                    "team": team,
                    "is_active": True,
                    "headshot_url": f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png",
                }
            else:
                if name and (not current.get("name") or len(name) > len(str(current.get("name")))):
                    current["name"] = name
                if team_id:
                    current["team_id"] = team_id
                if team:
                    current["team"] = team

    players = sorted(by_player.values(), key=lambda x: (x.get("name") or "", x["player_id"]))
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


def build_gamelogs_for_date(season: str, season_type: str, game_date: date) -> dict[str, Any]:
    payload = None
    last_error: Exception | None = None
    date_mmddyyyy = game_date.strftime("%m/%d/%Y")
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
                date_from_nullable=date_mmddyyyy,
                date_to_nullable=date_mmddyyyy,
                timeout=90,
                get_request=True,
            )
            payload = endpoint.get_dict()
            break
        except Exception as err:
            last_error = err
            sleep_for = min(20, attempt * 4)
            print(
                f"[warn] leaguegamelog date fetch failed {season} {season_type} {game_date.isoformat()} attempt {attempt}/5: {err}",
                flush=True,
            )
            time.sleep(sleep_for)
    if payload is None:
        raise RuntimeError(f"leaguegamelog date fetch failed for {season} {season_type} {game_date.isoformat()}: {last_error}")

    rows = extract_rows(payload, "LeagueGameLog")
    rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    return {
        "season": season,
        "season_type": season_type,
        "count": len(rows),
        "stat_fields": infer_stat_fields(rows),
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


def load_existing_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def merge_rows_by_date(existing_rows: list[dict[str, Any]], date_rows: list[dict[str, Any]], target_date_iso: str) -> list[dict[str, Any]]:
    # Remove target-date rows first, then add fresh rows, and dedupe by game/player.
    base = [r for r in existing_rows if str(r.get("GAME_DATE") or "") != target_date_iso]
    merged = base + date_rows
    deduped: dict[tuple[str, int], dict[str, Any]] = {}
    for r in merged:
        key = (str(r.get("GAME_ID") or ""), int(r.get("PLAYER_ID") or 0))
        deduped[key] = r
    rows = list(deduped.values())
    rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build static NBA data files for GitHub Pages")
    parser.add_argument("--output", default="frontend/public/data", help="Output data directory")
    parser.add_argument("--seasons", default=os.getenv("STATIC_SEASONS", ""), help="Comma separated seasons")
    parser.add_argument("--default-season", default=os.getenv("DEFAULT_SEASON", "2025-26"))
    parser.add_argument(
        "--incremental-date",
        default=os.getenv("INCREMENTAL_DATE", ""),
        help="YYYY-MM-DD date to refresh only for default season (daily incremental mode)",
    )
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=int(os.getenv("INCREMENTAL_DAYS", "1")),
        help="Number of days (ending at incremental-date) to refresh in incremental mode",
    )
    args = parser.parse_args()

    seasons = parse_seasons(args.seasons)
    output_root = Path(args.output).resolve()
    incremental_date: date | None = None
    if args.incremental_date:
        incremental_date = date.fromisoformat(args.incremental_date)
    incremental_days = max(1, int(args.incremental_days))

    files_players: dict[str, str] = {}
    files_gamelogs: dict[str, dict[str, str]] = {}

    for season in seasons:
        if incremental_date and season != args.default_season:
            # In incremental mode, only refresh current season.
            players_rel = f"players/{season}.json"
            files_players[season] = players_rel
            files_gamelogs[season] = {}
            for season_type in SEASON_TYPES:
                files_gamelogs[season][season_type_slug(season_type)] = f"gamelogs/{season}/{season_type_slug(season_type)}.json"
            continue

        files_gamelogs[season] = {}
        season_gamelog_payloads: list[dict[str, Any]] = []
        for season_type in SEASON_TYPES:
            slug = season_type_slug(season_type)
            print(f"[build] gamelogs {season} {season_type}", flush=True)
            rel = f"gamelogs/{season}/{slug}.json"
            out_path = output_root / rel
            gamelog_payload = None
            try:
                if incremental_date and season == args.default_season:
                    existing = load_existing_json(out_path)
                    if existing is None:
                        raise RuntimeError(f"Missing existing file for incremental update: {out_path}")
                    merged_rows = existing.get("rows", [])
                    for offset in range(incremental_days):
                        target = date.fromordinal(incremental_date.toordinal() - offset)
                        date_payload = build_gamelogs_for_date(season, season_type, target)
                        target_iso = target.isoformat()
                        merged_rows = merge_rows_by_date(merged_rows, date_payload.get("rows", []), target_iso)
                    gamelog_payload = {
                        "season": season,
                        "season_type": season_type,
                        "count": len(merged_rows),
                        "stat_fields": infer_stat_fields(merged_rows),
                        "rows": merged_rows,
                    }
                    dump_json(out_path, gamelog_payload)
                else:
                    gamelog_payload = build_gamelogs(season, season_type)
                    if season == args.default_season and season_type == "Regular Season" and int(gamelog_payload["count"]) == 0:
                        raise RuntimeError(f"No LeagueGameLog rows returned for {season} {season_type}")
                    dump_json(out_path, gamelog_payload)
            except Exception as err:
                existing = load_existing_json(out_path)
                if existing is None:
                    raise RuntimeError(f"Failed to refresh {season} {season_type} and no cached file exists: {err}") from err
                print(f"[warn] using cached gamelog for {season} {season_type}: {err}", flush=True)
                gamelog_payload = existing

            files_gamelogs[season][slug] = rel
            season_gamelog_payloads.append(gamelog_payload)
            time.sleep(1.5)

        print(f"[build] players {season}", flush=True)
        players_payload = build_players_from_gamelogs(season, season_gamelog_payloads)
        players_rel = f"players/{season}.json"
        dump_json(output_root / players_rel, players_payload)
        files_players[season] = players_rel

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
