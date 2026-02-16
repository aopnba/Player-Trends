from __future__ import annotations

import hashlib
import importlib
import inspect
import math
import os
import pkgutil
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from diskcache import Cache
from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from nba_api.stats.endpoints import boxscoreplayertrackv3, commonallplayers

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[2]
HEADSHOT_DIR = Path(os.getenv("HEADSHOT_DIR", str(ROOT_DIR))).resolve()
TEAM_LOGO_DIR = Path(
    os.getenv("TEAM_LOGO_DIR", "/Users/atticusobp/Desktop/team graphs")
).resolve()
DEFAULT_SEASON = os.getenv("DEFAULT_SEASON", "2025-26")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "21600"))
CACHE_DIR = ROOT_DIR / ".cache"

cache = Cache(str(CACHE_DIR))

app = FastAPI(title="NBA Viz API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if HEADSHOT_DIR.exists():
    app.mount("/headshots", StaticFiles(directory=str(HEADSHOT_DIR)), name="headshots")
if TEAM_LOGO_DIR.exists():
    app.mount("/team-logos", StaticFiles(directory=str(TEAM_LOGO_DIR)), name="team-logos")

TECHNICAL_PARAM_NAMES = {"proxy", "headers", "timeout", "get_request"}


def _cached_call(cache_key: str, fn):
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    result = fn()
    cache.set(cache_key, result, expire=CACHE_TTL_SECONDS)
    return result


def _headshot_index() -> dict[int, str]:
    cache_key = "headshot_index_v1"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    mapping: dict[int, str] = {}
    if HEADSHOT_DIR.exists():
        pattern = re.compile(r"^player_(\d+)_26\.jpg$")
        for file in HEADSHOT_DIR.glob("player_*_26.jpg"):
            m = pattern.match(file.name)
            if m:
                mapping[int(m.group(1))] = f"/headshots/{file.name}"

    cache.set(cache_key, mapping, expire=CACHE_TTL_SECONDS)
    return mapping


def _cdn_headshot_url(player_id: int) -> str:
    return f"https://cdn.nba.com/headshots/nba/latest/260x190/{int(player_id)}.png"


def _resolve_headshot_url(player_id: int, local_map: dict[int, str] | None = None) -> str:
    shots = local_map if local_map is not None else _headshot_index()
    return shots.get(int(player_id)) or _cdn_headshot_url(int(player_id))


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def _coerce_param(value: Any, param: inspect.Parameter) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None

    default = param.default
    if default is inspect._empty:
        return value

    if isinstance(default, bool):
        if isinstance(value, bool):
            return value
        return str(value).lower() in {"1", "true", "yes", "y"}

    if isinstance(default, int):
        try:
            return int(value)
        except (TypeError, ValueError):
            return value

    if isinstance(default, float):
        try:
            return float(value)
        except (TypeError, ValueError):
            return value

    return value


def _player_id_from_row(row: dict[str, Any]) -> int | None:
    keys = [
        "PLAYER_ID",
        "PERSON_ID",
        "player_id",
        "person_id",
        "playerId",
        "personId",
    ]
    for key in keys:
        if key in row:
            try:
                return int(row[key])
            except (TypeError, ValueError):
                continue
    return None


def _player_name_from_row(row: dict[str, Any]) -> str | None:
    keys = [
        "PLAYER_NAME",
        "DISPLAY_FIRST_LAST",
        "playerName",
        "player_name",
    ]
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return None


def _attach_headshots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    shots = _headshot_index()
    out: list[dict[str, Any]] = []
    for rec in rows:
        row = dict(rec)
        pid = _player_id_from_row(row)
        if pid is not None:
            row["headshot_url"] = _resolve_headshot_url(pid, shots)
        out.append(row)
    return out


def _safe_number(value: Any) -> float:
    out = _parse_float(value)
    return out if out is not None else 0.0


def _coalesce_synergy_playtypes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Synergy play type responses can contain multiple rows per player (e.g., traded players).
    # Merge per-player rows into one consolidated row.
    grouped: dict[int, list[dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []

    for row in rows:
        pid = _player_id_from_row(row)
        if pid is None:
            passthrough.append(row)
            continue
        grouped.setdefault(pid, []).append(row)

    merged_rows: list[dict[str, Any]] = []
    for pid, player_rows in grouped.items():
        if len(player_rows) == 1:
            merged_rows.append(player_rows[0])
            continue

        base = dict(player_rows[0])
        total_poss = sum(_safe_number(r.get("POSS")) for r in player_rows)
        total_pts = sum(_safe_number(r.get("PTS")) for r in player_rows)
        total_fgm = sum(_safe_number(r.get("FGM")) for r in player_rows)
        total_fga = sum(_safe_number(r.get("FGA")) for r in player_rows)
        total_fgmx = sum(_safe_number(r.get("FGMX")) for r in player_rows)
        total_gp = sum(_safe_number(r.get("GP")) for r in player_rows)

        def poss_weighted(field: str) -> float:
            if total_poss <= 0:
                return 0.0
            return (
                sum(_safe_number(r.get(field)) * _safe_number(r.get("POSS")) for r in player_rows)
                / total_poss
            )

        base["TEAM_ID"] = 0
        base["TEAM_ABBREVIATION"] = "TOT"
        base["TEAM_NAME"] = "Traded Players"
        base["GP"] = int(round(total_gp))
        base["POSS"] = round(total_poss, 3)
        base["PTS"] = round(total_pts, 3)
        base["FGM"] = round(total_fgm, 3)
        base["FGA"] = round(total_fga, 3)
        base["FGMX"] = round(total_fgmx, 3)

        base["PPP"] = round((total_pts / total_poss) if total_poss > 0 else 0.0, 6)
        base["FG_PCT"] = round((total_fgm / total_fga) if total_fga > 0 else 0.0, 6)
        base["EFG_PCT"] = round(
            ((total_fgm + 0.5 * total_fgmx) / total_fga) if total_fga > 0 else 0.0, 6
        )
        base["POSS_PCT"] = round(poss_weighted("POSS_PCT"), 6)
        base["FT_POSS_PCT"] = round(poss_weighted("FT_POSS_PCT"), 6)
        base["TOV_POSS_PCT"] = round(poss_weighted("TOV_POSS_PCT"), 6)
        base["SF_POSS_PCT"] = round(poss_weighted("SF_POSS_PCT"), 6)
        base["PLUSONE_POSS_PCT"] = round(poss_weighted("PLUSONE_POSS_PCT"), 6)
        base["SCORE_POSS_PCT"] = round(poss_weighted("SCORE_POSS_PCT"), 6)
        base["PERCENTILE"] = round(poss_weighted("PERCENTILE"), 6)

        merged_rows.append(base)

    merged_rows.extend(passthrough)
    return merged_rows


def _label_from_module(name: str) -> str:
    return " ".join(part.capitalize() for part in re.split(r"[_\-]", name))


def _tracking_subgroup(module_name: str) -> str:
    if "pass" in module_name:
        return "Passing"
    if "reb" in module_name:
        return "Rebounding"
    if "shotdefend" in module_name or "ptdefend" in module_name:
        return "Defense"
    if "shot" in module_name:
        return "Shooting"
    if "ptstats" in module_name:
        return "Touches"
    return "General"


def _classify_stats_endpoint(module_name: str) -> tuple[str, str]:
    if module_name.startswith("boxscore") or module_name == "hustlestatsboxscore":
        return "Box Scores", "Game Box Scores"

    if "clutch" in module_name:
        return "Clutch", "Player/Team Clutch"

    if module_name in {"synergyplaytypes"}:
        return "Playtype", "Synergy"

    if "shotchart" in module_name or "shotlocation" in module_name:
        return "Shot Dashboard", "Shot Charts"

    if module_name in {"leaguedashoppptshot", "leaguedashplayerptshot", "leaguedashteamptshot"}:
        return "Shot Dashboard", "PT Shot Profiles"

    if module_name in {"defensehub", "leaguedashptdefend", "leaguedashptteamdefend"}:
        return "Defense Dashboard", "Defense"

    if "hustle" in module_name:
        return "General", "Hustle Leaders"

    if "pt" in module_name and (
        module_name.startswith("playerdashpt")
        or module_name.startswith("teamdashpt")
        or module_name.startswith("leaguedashpt")
    ):
        return "Tracking", _tracking_subgroup(module_name)

    if module_name in {"leagueleaders", "homepageleaders", "leaderstiles", "alltimeleadersgrids"}:
        return "General", "Official Leaders"

    if module_name in {"leaguedashplayerstats", "leaguedashteamstats", "playerestimatedmetrics"}:
        return "General", "Advanced Leaders"

    if (
        module_name.startswith("common")
        or module_name.endswith("history")
        or "franchise" in module_name
        or module_name.startswith("playerindex")
        or module_name.startswith("teamdetails")
    ):
        return "Reference", "Players/Teams"

    if (
        module_name.startswith("schedule")
        or module_name.startswith("scoreboard")
        or module_name.startswith("leaguegame")
        or module_name.startswith("playbyplay")
        or module_name.startswith("gamerotation")
        or module_name.startswith("playoffpicture")
        or module_name.startswith("iststandings")
        or module_name.startswith("leaguestandings")
    ):
        return "Game Center", "Schedule & Games"

    if module_name.startswith("draft"):
        return "Draft", "Combine & Draft"

    if "fantasy" in module_name:
        return "Fantasy", "Fantasy"

    return "Other", "Misc"


def _classify_live_endpoint(module_name: str) -> tuple[str, str]:
    if module_name == "boxscore":
        return "Live", "Box Score"
    if module_name == "playbyplay":
        return "Live", "Play By Play"
    if module_name == "scoreboard":
        return "Live", "Scoreboard"
    if module_name == "odds":
        return "Live", "Odds"
    return "Live", "Misc"


def _endpoint_class(module_name: str, domain: str):
    pkg = "nba_api.stats.endpoints" if domain == "stats" else "nba_api.live.nba.endpoints"
    module = importlib.import_module(f"{pkg}.{module_name}")
    classes = [
        cls
        for _, cls in inspect.getmembers(module, inspect.isclass)
        if cls.__module__ == module.__name__
    ]

    if domain == "stats":
        for cls in classes:
            if hasattr(cls, "get_data_frames"):
                return cls
    else:
        for cls in classes:
            if hasattr(cls, "get_dict"):
                return cls

    raise HTTPException(status_code=500, detail=f"Could not resolve endpoint class: {module_name}")


def _parameter_schema(endpoint_cls: Any) -> list[dict[str, Any]]:
    sig = inspect.signature(endpoint_cls.__init__)
    params: list[dict[str, Any]] = []
    for name, param in sig.parameters.items():
        if name == "self" or name in TECHNICAL_PARAM_NAMES:
            continue
        default = None if param.default is inspect._empty else param.default
        params.append(
            {
                "name": name,
                "required": param.default is inspect._empty,
                "default": default,
            }
        )
    return params


def _endpoint_registry() -> dict[str, dict[str, Any]]:
    key = "endpoint_registry_v3"
    cached = cache.get(key)
    if cached is not None:
        return cached

    registry: dict[str, dict[str, Any]] = {}

    import nba_api.stats.endpoints as stats_pkg

    for mod in pkgutil.iter_modules(stats_pkg.__path__):
        if mod.name.startswith("_"):
            continue
        cls = _endpoint_class(mod.name, "stats")
        source, subgroup = _classify_stats_endpoint(mod.name)
        registry[mod.name] = {
            "key": mod.name,
            "domain": "stats",
            "source": source,
            "subgroup": subgroup,
            "label": _label_from_module(mod.name),
            "class_name": cls.__name__,
            "params": _parameter_schema(cls),
        }

    import nba_api.live.nba.endpoints as live_pkg

    for mod in pkgutil.iter_modules(live_pkg.__path__):
        if mod.name.startswith("_"):
            continue
        cls = _endpoint_class(mod.name, "live")
        source, subgroup = _classify_live_endpoint(mod.name)
        key_name = f"live::{mod.name}"
        registry[key_name] = {
            "key": key_name,
            "module": mod.name,
            "domain": "live",
            "source": source,
            "subgroup": subgroup,
            "label": _label_from_module(mod.name),
            "class_name": cls.__name__,
            "params": _parameter_schema(cls),
        }

    cache.set(key, registry, expire=CACHE_TTL_SECONDS)
    return registry


def _catalog_tree() -> dict[str, Any]:
    registry = _endpoint_registry()
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for item in registry.values():
        source = item["source"]
        subgroup = item["subgroup"]
        grouped.setdefault(source, {}).setdefault(subgroup, []).append(item)

    sources = []
    for source in sorted(grouped.keys()):
        subgroups = []
        for subgroup in sorted(grouped[source].keys()):
            endpoints = sorted(grouped[source][subgroup], key=lambda x: x["label"])
            subgroups.append(
                {
                    "name": subgroup,
                    "endpoints": [
                        {
                            "key": e["key"],
                            "domain": e["domain"],
                            "label": e["label"],
                            "class_name": e["class_name"],
                            "params": e["params"],
                        }
                        for e in endpoints
                    ],
                }
            )
        sources.append({"name": source, "subgroups": subgroups})

    return {
        "sources": sources,
        "total_endpoints": len(registry),
        "stats_endpoints": len([x for x in registry.values() if x["domain"] == "stats"]),
        "live_endpoints": len([x for x in registry.values() if x["domain"] == "live"]),
    }


def _filter_params(endpoint_cls: Any, params: dict[str, Any]) -> dict[str, Any]:
    sig = inspect.signature(endpoint_cls.__init__)
    out: dict[str, Any] = {}
    for name, value in params.items():
        if name not in sig.parameters or name in TECHNICAL_PARAM_NAMES:
            continue
        coerced = _coerce_param(value, sig.parameters[name])
        if coerced is not None:
            out[name] = coerced
    return out


def _resolve_endpoint(key: str) -> tuple[dict[str, Any], Any]:
    registry = _endpoint_registry()
    info = registry.get(key)
    if info is None:
        raise HTTPException(status_code=404, detail=f"Unknown endpoint: {key}")
    module_name = info["module"] if info["domain"] == "live" else info["key"]
    cls = _endpoint_class(module_name, info["domain"])
    return info, cls


def _query_stats_endpoint(key: str, params: dict[str, Any], dataset_index: int, max_rows: int):
    info, endpoint_cls = _resolve_endpoint(key)
    filtered = _filter_params(endpoint_cls, params)
    filtered["timeout"] = 30

    cache_key = f"query::stats::{key}::{dataset_index}::{max_rows}::{repr(sorted(filtered.items()))}"

    def load():
        try:
            endpoint = endpoint_cls(**filtered)
            frames = endpoint.get_data_frames()
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"NBA API request failed for endpoint '{key}': {exc}",
            ) from exc
        if not frames:
            raise HTTPException(status_code=502, detail="No data returned from NBA API")

        idx = max(0, min(int(dataset_index), len(frames) - 1))
        frame = frames[idx]
        rows = frame.to_dict(orient="records")
        if key == "synergyplaytypes":
            rows = _coalesce_synergy_playtypes(rows)
        rows = _attach_headshots(rows)
        rows = rows[: max_rows if max_rows > 0 else len(rows)]

        numeric_fields = [
            c for c in frame.columns if str(frame[c].dtype) in ("int64", "float64", "int32", "float32")
        ]

        return {
            "endpoint": key,
            "domain": info["domain"],
            "dataset_index": idx,
            "dataset_count": len(frames),
            "params_used": filtered,
            "row_count": len(rows),
            "columns": list(frame.columns),
            "numeric_fields": numeric_fields,
            "rows": rows,
        }

    return _cached_call(cache_key, load)


def _query_live_endpoint(key: str, params: dict[str, Any]):
    info, endpoint_cls = _resolve_endpoint(key)
    filtered = _filter_params(endpoint_cls, params)
    filtered["timeout"] = 30

    cache_key = f"query::live::{key}::{repr(sorted(filtered.items()))}"

    def load():
        endpoint = endpoint_cls(**filtered)
        payload = endpoint.get_dict()
        return {
            "endpoint": key,
            "domain": info["domain"],
            "params_used": filtered,
            "payload": payload,
        }

    return _cached_call(cache_key, load)


def _inject_season(params: dict[str, Any], season: str):
    season_keys = ["season", "season_nullable", "season_year"]
    for key in season_keys:
        if key in params:
            params[key] = season
            return
    params["season"] = season


def _season_sort_key(season: str) -> int:
    m = re.match(r"^(\d{4})", season)
    if not m:
        return 0
    return int(m.group(1))


def _jitter(player_id: int, season: str, scale: float = 0.08) -> float:
    h = hashlib.md5(f"{player_id}-{season}".encode("utf-8")).hexdigest()
    n = int(h[:8], 16) / 0xFFFFFFFF
    return (n * 2 - 1) * scale


def _season_from_start(start_year: int) -> str:
    return f"{start_year}-{str((start_year + 1) % 100).zfill(2)}"


def _parse_game_date(value: Any) -> datetime:
    if not value:
        return datetime.min
    text = str(value)
    if "T" in text:
        try:
            return datetime.fromisoformat(text.replace("Z", ""))
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.min


def _to_date_key(value: Any) -> str:
    d = _parse_game_date(value)
    if d == datetime.min:
        return ""
    return d.strftime("%Y-%m-%d")


def _trends_numeric_fields(rows: list[dict[str, Any]]) -> list[str]:
    blacklist = {
        "PLAYER_ID",
        "TEAM_ID",
        "personId",
        "teamId",
        "AVAILABLE_FLAG",
        "TEAM_COUNT",
    }
    out: list[str] = []
    if not rows:
        return out
    keys = set()
    for row in rows:
        keys.update(row.keys())
    for key in sorted(keys):
        if key in blacklist or key.endswith("_RANK"):
            continue
        is_numeric = False
        for row in rows:
            value = row.get(key)
            if isinstance(value, (int, float)):
                is_numeric = True
                break
        if is_numeric:
            out.append(key)
    return out


def _tracking_row_for_player(game_id: str, player_id: int) -> dict[str, Any] | None:
    cache_key = f"tracking_game_map::{game_id}"
    cached_map = cache.get(cache_key)
    if cached_map is None:
        try:
            endpoint = boxscoreplayertrackv3.BoxScorePlayerTrackV3(game_id=game_id, timeout=30)
            frames = endpoint.get_data_frames()
        except Exception:
            cache.set(cache_key, {}, expire=CACHE_TTL_SECONDS)
            return None

        game_map: dict[int, dict[str, Any]] = {}
        for frame in frames:
            for row in frame.to_dict(orient="records"):
                pid = row.get("personId")
                try:
                    pid_int = int(pid)
                except (TypeError, ValueError):
                    continue
                game_map[pid_int] = row
        cache.set(cache_key, game_map, expire=CACHE_TTL_SECONDS)
        cached_map = game_map

    return cached_map.get(int(player_id))


def _season_date_range(season: str, season_type: str) -> tuple[datetime, datetime]:
    m = re.match(r"^(\d{4})-(\d{2})$", season)
    if not m:
        raise HTTPException(status_code=400, detail="season must be in YYYY-YY format")
    start_year = int(m.group(1))
    end_year = start_year + 1

    # Approximate NBA calendar windows; endpoint-level filtering handles exact validity.
    if season_type.lower() == "playoffs":
        start = datetime(end_year, 4, 10)
        end = datetime(end_year, 7, 1)
    else:
        start = datetime(start_year, 10, 1)
        end = datetime(end_year, 4, 20)
    return start, end


def _iter_dates(start: datetime, end: datetime):
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


@app.get("/api/health")
def health() -> dict[str, Any]:
    catalog = _catalog_tree()
    return {
        "ok": True,
        "headshot_dir": str(HEADSHOT_DIR),
        "headshots_detected": len(_headshot_index()),
        "default_season": DEFAULT_SEASON,
        "stats_endpoints": catalog["stats_endpoints"],
        "live_endpoints": catalog["live_endpoints"],
    }


@app.get("/api/players")
def players(season: str = Query(DEFAULT_SEASON)) -> dict[str, Any]:
    key = f"players::{season}"

    def load() -> dict[str, Any]:
        endpoint = commonallplayers.CommonAllPlayers(
            is_only_current_season=1,
            season=season,
            timeout=30,
        )
        frames = endpoint.get_data_frames()
        if not frames:
            raise HTTPException(status_code=502, detail="No data returned from NBA API")

        df = frames[0]
        shots = _headshot_index()
        records = []
        for row in df.to_dict(orient="records"):
            pid = int(row["PERSON_ID"])
            records.append(
                {
                    "player_id": pid,
                    "name": row.get("DISPLAY_FIRST_LAST"),
                    "team_id": row.get("TEAM_ID"),
                    "team": row.get("TEAM_ABBREVIATION"),
                    "is_active": row.get("ROSTERSTATUS") == 1,
                    "headshot_url": _resolve_headshot_url(pid, shots),
                }
            )
        return {"season": season, "count": len(records), "players": records}

    return _cached_call(key, load)


@app.get("/api/catalog")
def catalog() -> dict[str, Any]:
    return _catalog_tree()


@app.get("/api/trends/player")
def trends_player(
    player_id: int,
    source: str = Query("overall"),
    season: str = Query(DEFAULT_SEASON),
    season_type: str = Query("Regular Season"),
    tracking_measure: str = Query("Passing"),
) -> dict[str, Any]:
    source = source.strip().lower()
    if source not in {"overall", "tracking"}:
        raise HTTPException(status_code=400, detail="source must be 'overall' or 'tracking'")

    base = _query_stats_endpoint(
        key="playergamelogs",
        params={
            "player_id_nullable": player_id,
            "season_nullable": season,
            "season_type_nullable": season_type,
        },
        dataset_index=0,
        max_rows=500,
    )
    game_rows = sorted(base["rows"], key=lambda r: _parse_game_date(r.get("GAME_DATE")))
    shots = _headshot_index()

    if source == "overall":
        rows = []
        for row in game_rows:
            item = dict(row)
            item["headshot_url"] = _resolve_headshot_url(player_id, shots)
            rows.append(item)
        return {
            "player_id": player_id,
            "source": source,
            "season": season,
            "season_type": season_type,
            "count": len(rows),
            "rows": rows,
            "stat_fields": _trends_numeric_fields(rows),
        }

    game_date_map = {_to_date_key(r.get("GAME_DATE")): r for r in game_rows}
    tracking_rows: list[dict[str, Any]] = []
    range_start, range_end = _season_date_range(season, season_type)

    for date_obj in _iter_dates(range_start, range_end):
        date_str = date_obj.strftime("%Y-%m-%d")
        try:
            day_result = _query_stats_endpoint(
                key="leaguedashptstats",
                params={
                    "season": season,
                    "season_type_all_star": season_type,
                    "per_mode_simple": "PerGame",
                    "player_or_team": "Player",
                    "pt_measure_type": tracking_measure,
                    "date_from_nullable": date_str,
                    "date_to_nullable": date_str,
                },
                dataset_index=0,
                max_rows=4000,
            )
        except HTTPException:
            continue

        selected = None
        for row in day_result["rows"]:
            try:
                if int(row.get("PLAYER_ID")) == int(player_id):
                    selected = row
                    break
            except (TypeError, ValueError):
                continue

        if not selected:
            continue

        game_row = game_date_map.get(date_str, {})
        item = {
            "GAME_DATE": date_str,
            "GAME_ID": game_row.get("GAME_ID"),
            "MATCHUP": game_row.get("MATCHUP"),
            "WL": game_row.get("WL"),
            **selected,
            "headshot_url": _resolve_headshot_url(player_id, shots),
        }
        tracking_rows.append(item)

    return {
        "player_id": player_id,
        "source": source,
        "season": season,
        "season_type": season_type,
        "tracking_measure": tracking_measure,
        "count": len(tracking_rows),
        "rows": tracking_rows,
        "stat_fields": _trends_numeric_fields(tracking_rows),
    }


@app.post("/api/query")
def query_endpoint(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    endpoint_key = str(payload.get("endpoint", "")).strip()
    if not endpoint_key:
        raise HTTPException(status_code=400, detail="Missing endpoint")

    params = payload.get("params") or {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="params must be an object")

    dataset_index = int(payload.get("dataset_index", 0))
    max_rows = int(payload.get("max_rows", 1500))

    info, _ = _resolve_endpoint(endpoint_key)
    if info["domain"] == "stats":
        return _query_stats_endpoint(endpoint_key, params, dataset_index, max_rows)
    return _query_live_endpoint(endpoint_key, params)


@app.post("/api/available_seasons")
def available_seasons(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    endpoint_key = str(payload.get("endpoint", "")).strip()
    if not endpoint_key:
        raise HTTPException(status_code=400, detail="Missing endpoint")

    dataset_index = int(payload.get("dataset_index", 0))
    years_back = int(payload.get("years_back", 12))
    years_back = max(3, min(years_back, 30))
    start_season = str(payload.get("start_season", DEFAULT_SEASON)).strip()
    params_base = payload.get("params") or {}
    if not isinstance(params_base, dict):
        raise HTTPException(status_code=400, detail="params must be an object")

    m = re.match(r"^(\d{4})", start_season)
    if not m:
        raise HTTPException(status_code=400, detail="start_season must look like YYYY-YY")
    start_year = int(m.group(1))

    seasons: list[str] = []
    for offset in range(years_back):
        season = _season_from_start(start_year - offset)
        params = dict(params_base)
        _inject_season(params, season)
        try:
            result = _query_stats_endpoint(
                key=endpoint_key,
                params=params,
                dataset_index=dataset_index,
                max_rows=1,
            )
            if result["row_count"] > 0:
                seasons.append(season)
        except HTTPException:
            continue

    return {"endpoint": endpoint_key, "seasons": seasons}


@app.post("/api/yoy_beeswarm")
def yoy_beeswarm(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    endpoint_key = str(payload.get("endpoint", "")).strip()
    metric = str(payload.get("metric", "")).strip()
    seasons = payload.get("seasons") or []
    base_params = payload.get("params") or {}
    dataset_index = int(payload.get("dataset_index", 0))
    highlighted_players = payload.get("highlight_player_ids") or []

    if not endpoint_key or not metric:
        raise HTTPException(status_code=400, detail="endpoint and metric are required")
    if not isinstance(seasons, list) or not seasons:
        raise HTTPException(status_code=400, detail="seasons must be a non-empty array")

    seasons = [str(s).strip() for s in seasons if str(s).strip()]
    seasons = sorted(set(seasons), key=_season_sort_key)
    highlight_ids = [int(x) for x in highlighted_players[:3]]

    all_points: list[dict[str, Any]] = []
    skipped_seasons: list[str] = []

    for season in seasons:
        params = dict(base_params)
        _inject_season(params, season)
        try:
            result = _query_stats_endpoint(
                key=endpoint_key,
                params=params,
                dataset_index=dataset_index,
                max_rows=4000,
            )
        except HTTPException:
            skipped_seasons.append(season)
            continue

        for row in result["rows"]:
            value = _parse_float(row.get(metric))
            pid = _player_id_from_row(row)
            pname = _player_name_from_row(row)
            if value is None or pid is None:
                continue
            point = {
                "season": season,
                "player_id": pid,
                "player_name": pname or str(pid),
                "metric": metric,
                "value": value,
                "jitter": _jitter(pid, season),
                "headshot_url": row.get("headshot_url"),
                "highlighted": pid in highlight_ids,
            }
            all_points.append(point)

    highlights: dict[int, list[dict[str, Any]]] = {pid: [] for pid in highlight_ids}
    for point in all_points:
        pid = point["player_id"]
        if pid in highlights:
            highlights[pid].append(point)

    for pid in highlights:
        highlights[pid] = sorted(highlights[pid], key=lambda p: _season_sort_key(p["season"]))

    return {
        "endpoint": endpoint_key,
        "metric": metric,
        "seasons": seasons,
        "point_count": len(all_points),
        "points": all_points,
        "highlights": highlights,
        "skipped_seasons": skipped_seasons,
    }


@app.post("/api/cache/clear")
def clear_cache() -> dict[str, Any]:
    cache.clear()
    return {"ok": True}
