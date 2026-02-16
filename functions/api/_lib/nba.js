const NBA_STATS_BASE = "https://stats.nba.com/stats";
const DEFAULT_SEASON = "2025-26";

const NBA_HEADERS = {
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  Origin: "https://www.nba.com",
  Referer: "https://www.nba.com/",
  "User-Agent":
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  "x-nba-stats-origin": "stats",
  "x-nba-stats-token": "true",
};

export function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=120",
    },
  });
}

export function readQuery(url, key, fallback = "") {
  const val = new URL(url).searchParams.get(key);
  return (val ?? fallback).trim() || fallback;
}

function asRows(resultSet) {
  if (!resultSet) return [];
  const headers = resultSet.headers || [];
  const rows = resultSet.rowSet || [];
  return rows.map((row) => {
    const out = {};
    for (let i = 0; i < headers.length; i += 1) {
      out[headers[i]] = row[i];
    }
    return out;
  });
}

export function extractRows(payload, nameHint = "") {
  if (payload?.resultSets && Array.isArray(payload.resultSets)) {
    const byName =
      nameHint &&
      payload.resultSets.find((set) => String(set?.name || "").toLowerCase() === nameHint.toLowerCase());
    return asRows(byName || payload.resultSets[0]);
  }
  if (payload?.resultSet) return asRows(payload.resultSet);
  return [];
}

export function headshotUrl(playerId) {
  return `https://cdn.nba.com/headshots/nba/latest/260x190/${Number(playerId)}.png`;
}

export async function fetchStats(endpoint, params = {}, context) {
  const url = new URL(`${NBA_STATS_BASE}/${endpoint}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }

  const cache = caches.default;
  const request = new Request(url.toString(), { method: "GET", headers: NBA_HEADERS });
  const cached = await cache.match(request);
  if (cached) return cached.json();

  const response = await fetch(request, { cf: { cacheTtl: 120, cacheEverything: true } });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`NBA API ${endpoint} failed (${response.status}): ${text.slice(0, 200)}`);
  }
  const clone = response.clone();
  context.waitUntil(cache.put(request, clone));
  return response.json();
}

export function defaultSeason() {
  return DEFAULT_SEASON;
}

export function numericFields(rows) {
  if (!rows.length) return [];
  const blacklist = new Set(["PLAYER_ID", "TEAM_ID", "GAME_ID"]);
  const keys = new Set();
  for (const row of rows) {
    for (const key of Object.keys(row)) keys.add(key);
  }
  const out = [];
  for (const key of [...keys].sort()) {
    if (blacklist.has(key) || key.endsWith("_RANK")) continue;
    let isNum = false;
    for (const row of rows) {
      const value = row[key];
      if (typeof value === "number" && Number.isFinite(value)) {
        isNum = true;
        break;
      }
      if (typeof value === "string" && value !== "" && Number.isFinite(Number(value))) {
        isNum = true;
        break;
      }
    }
    if (isNum) out.push(key);
  }
  return out;
}

export function sortByGameDate(rows) {
  return [...rows].sort((a, b) => {
    const da = Date.parse(String(a.GAME_DATE || ""));
    const db = Date.parse(String(b.GAME_DATE || ""));
    return (Number.isNaN(da) ? 0 : da) - (Number.isNaN(db) ? 0 : db);
  });
}
