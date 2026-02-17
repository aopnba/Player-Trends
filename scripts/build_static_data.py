#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nba_api.stats.endpoints import commonallplayers, playergamelogs

SEASON_TYPES = ["Regular Season", "Playoffs"]
DEFAULT_SEASONS = ["2025-26"]


def season_type_slug(value: str) -> str:
    return re.sub(r"\s+", "-", value.strip().lower())


def parse_seasons(raw: str | None) -> list[str]:
    if not raw:
        return DEFAULT_SEASONS
    out = [x.strip() for x in raw.split(",") if x.strip()]
    return out or DEFAULT_SEASONS


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


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        pid = int(row.get("PLAYER_ID") or 0)
        game_id = str(row.get("GAME_ID") or "")
        if pid <= 0 or not game_id:
            out.append(row)
            continue
        key = (pid, game_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def is_placeholder_row(row: dict[str, Any]) -> bool:
    if int(row.get("IS_PLACEHOLDER") or 0) == 1:
        return True
    game_id = str(row.get("GAME_ID") or "")
    matchup = str(row.get("MATCHUP") or "").upper()
    return game_id.startswith("NO_GAME_") or "NO GAMES YET" in matchup


def filter_real_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if not is_placeholder_row(row)]


def fetch_players(season: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    last_exc: Exception | None = None
    # Try current-season first (smaller payload), then all-season if needed.
    current_season_flags = [1, 1, 1, 0, 0]
    for attempt, current_flag in enumerate(current_season_flags, start=1):
        try:
            endpoint = commonallplayers.CommonAllPlayers(
                is_only_current_season=current_flag,
                season=season,
                timeout=120,
            )
            frames = endpoint.get_data_frames()
            if not frames:
                raise RuntimeError("No frames returned from commonallplayers")
            rows = frames[0].to_dict(orient="records")
            if len(rows) < 300:
                raise RuntimeError(
                    f"CommonAllPlayers payload too small on attempt {attempt}: {len(rows)} rows"
                )
            break
        except Exception as exc:
            last_exc = exc
            sleep_for = min(8.0 * attempt, 45.0) + random.uniform(0.0, 1.5)
            print(
                f"[warn] commonallplayers attempt {attempt} failed: {exc}; retrying in {sleep_for:.1f}s",
                flush=True,
            )
            time.sleep(sleep_for)

    if not rows:
        raise RuntimeError(f"CommonAllPlayers failed for {season}: {last_exc}")
    players: list[dict[str, Any]] = []
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
    players = sorted(players, key=lambda x: (x.get("name") or "", int(x.get("player_id") or 0)))
    if len(players) < 300:
        raise RuntimeError(f"CommonAllPlayers returned too few rows for {season}: {len(players)}")
    return players


def fetch_player_gamelogs(player_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
    endpoint = playergamelogs.PlayerGameLogs(
        player_id_nullable=player_id,
        season_nullable=season,
        season_type_nullable=season_type,
        timeout=60,
    )
    frames = endpoint.get_data_frames()
    if not frames:
        return []
    rows = frames[0].to_dict(orient="records")
    for row in rows:
        if "PLAYER_ID" not in row:
            row["PLAYER_ID"] = player_id
    return rows


def build_gamelogs_for_season_type(
    season: str,
    season_type: str,
    active_player_ids: list[int],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    total = len(active_player_ids)
    failures = 0

    for idx, player_id in enumerate(active_player_ids, start=1):
        if idx == 1 or idx % 20 == 0 or idx == total:
            print(f"[build] {season} {season_type} player {idx}/{total}", flush=True)

        player_rows: list[dict[str, Any]] = []
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                player_rows = fetch_player_gamelogs(player_id, season, season_type)
                break
            except Exception as exc:
                last_exc = exc
                time.sleep(min(1.0 * attempt, 3.0))

        if player_rows:
            rows.extend(player_rows)
        else:
            if last_exc is not None:
                failures += 1
                print(f"[warn] player {player_id} failed: {last_exc}", flush=True)

        time.sleep(0.08)

    rows = dedupe_rows(filter_real_rows(rows))
    rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    stat_fields = infer_stat_fields(rows)
    unique_players = len({int(r.get("PLAYER_ID") or 0) for r in rows if r.get("PLAYER_ID") is not None})

    print(
        "[build] "
        f"{season} {season_type} rows={len(rows)} unique_players={unique_players} "
        f"failures={failures}",
        flush=True,
    )

    if season_type == "Regular Season" and unique_players < 200:
        raise RuntimeError(
            f"Incomplete Regular Season pull: only {unique_players} players had rows. "
            "Aborting publish."
        )
    if failures > 25:
        raise RuntimeError(
            f"Too many per-player request failures for {season} {season_type}: {failures}. "
            "Aborting publish."
        )

    return {
        "season": season,
        "season_type": season_type,
        "count": len(rows),
        "stat_fields": stat_fields,
        "rows": rows,
    }


def build_players_payload(season: str, all_players: list[dict[str, Any]], regular_rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = [p for p in all_players if bool(p.get("is_active"))]
    out.sort(key=lambda x: (x.get("name") or "", int(x.get("player_id") or 0)))
    return {"season": season, "count": len(out), "players": out}


def dump_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build static NBA data files using playergamelogs")
    parser.add_argument("--output", default="frontend/public/data", help="Output data directory")
    parser.add_argument("--seasons", default=os.getenv("STATIC_SEASONS", ""), help="Comma-separated seasons")
    parser.add_argument("--default-season", default=os.getenv("DEFAULT_SEASON", "2025-26"))
    args = parser.parse_args()

    seasons = parse_seasons(args.seasons)
    output_root = Path(args.output).resolve()

    files_players: dict[str, str] = {}
    files_gamelogs: dict[str, dict[str, str]] = {}

    for season in seasons:
        print(f"[build] loading players for {season}", flush=True)
        players_rel = f"players/{season}.json"
        all_players = fetch_players(season)
        active_player_ids = sorted(
            {
                int(p.get("player_id") or 0)
                for p in all_players
                if bool(p.get("is_active")) and p.get("player_id") is not None
            }
        )
        print(f"[build] active players {season}: {len(active_player_ids)}", flush=True)

        files_gamelogs[season] = {}
        regular_rows: list[dict[str, Any]] = []

        for season_type in SEASON_TYPES:
            slug = season_type_slug(season_type)
            rel = f"gamelogs/{season}/{slug}.json"
            payload = build_gamelogs_for_season_type(
                season,
                season_type,
                active_player_ids,
            )
            dump_json(output_root / rel, payload)
            files_gamelogs[season][slug] = rel
            if season_type == "Regular Season":
                regular_rows = payload["rows"]

        players_payload = build_players_payload(season, all_players, regular_rows)
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
