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


def fetch_players(season: str) -> list[dict[str, Any]]:
    endpoint = commonallplayers.CommonAllPlayers(
        is_only_current_season=1,
        season=season,
        timeout=60,
    )
    frames = endpoint.get_data_frames()
    if not frames:
        raise RuntimeError("No frames returned from commonallplayers")
    rows = frames[0].to_dict(orient="records")
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


def _rows_by_player(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    out: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        pid = int(row.get("PLAYER_ID") or 0)
        if pid <= 0:
            continue
        out.setdefault(pid, []).append(row)
    return out


def _load_existing_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        if isinstance(rows, list):
            return rows
    except Exception:
        return []
    return []


def build_gamelogs_for_season_type(
    season: str,
    season_type: str,
    active_player_ids: list[int],
    existing_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    total = len(active_player_ids)
    failures = 0
    reused_from_cache = 0
    existing_by_player = _rows_by_player(existing_rows)

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
            fallback_rows = existing_by_player.get(player_id, [])
            if fallback_rows:
                reused_from_cache += 1
                rows.extend(fallback_rows)
            elif last_exc is not None:
                failures += 1
                print(f"[warn] player {player_id} failed: {last_exc}", flush=True)

        time.sleep(0.08)

    rows = dedupe_rows(rows)
    rows.sort(key=lambda r: (str(r.get("GAME_DATE") or ""), int(r.get("PLAYER_ID") or 0)))
    stat_fields = infer_stat_fields(rows)
    unique_players = len({int(r.get("PLAYER_ID") or 0) for r in rows if r.get("PLAYER_ID") is not None})

    print(
        "[build] "
        f"{season} {season_type} rows={len(rows)} unique_players={unique_players} "
        f"failures={failures} reused_from_cache={reused_from_cache}",
        flush=True,
    )

    if season_type == "Regular Season" and unique_players < 200 and not existing_rows:
        raise RuntimeError(
            f"Incomplete Regular Season pull: only {unique_players} players had rows. "
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
    rows_by_pid: set[int] = {int(r.get("PLAYER_ID") or 0) for r in regular_rows if r.get("PLAYER_ID") is not None}

    # Keep only active players with at least one row in selected season,
    # so frontend search maps directly to available static data.
    out = [
        p
        for p in all_players
        if bool(p.get("is_active")) and int(p.get("player_id") or 0) in rows_by_pid
    ]
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
            existing_rows = _load_existing_rows(output_root / rel)
            payload = build_gamelogs_for_season_type(
                season,
                season_type,
                active_player_ids,
                existing_rows,
            )
            dump_json(output_root / rel, payload)
            files_gamelogs[season][slug] = rel
            if season_type == "Regular Season":
                regular_rows = payload["rows"]

        players_payload = build_players_payload(season, all_players, regular_rows)
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
