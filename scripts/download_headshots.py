#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NBA player headshots into frontend/public/headshots")
    parser.add_argument("--players-json", default="frontend/public/data/players/2025-26.json")
    parser.add_argument("--out-dir", default="frontend/public/headshots")
    parser.add_argument("--size", default="260x190", help="NBA CDN headshot size path, e.g. 260x190 or 1040x760")
    args = parser.parse_args()

    players_json = Path(args.players_json)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = json.loads(players_json.read_text(encoding="utf-8"))
    players = payload.get("players", [])

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    ok = 0
    fail = 0
    for p in players:
        pid = int(p.get("player_id") or 0)
        if pid <= 0:
            continue
        target = out_dir / f"{pid}.png"
        if target.exists() and target.stat().st_size > 0:
            ok += 1
            continue
        url = f"https://cdn.nba.com/headshots/nba/latest/{args.size}/{pid}.png"
        try:
            resp = session.get(url, timeout=30)
            if resp.ok and resp.content:
                target.write_bytes(resp.content)
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1

    print(f"downloaded_or_existing={ok} failed={fail} out_dir={out_dir}")


if __name__ == "__main__":
    main()

