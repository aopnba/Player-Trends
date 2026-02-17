import { useEffect, useMemo, useRef, useState } from "react";
import Plot from "react-plotly.js";
import { toPng } from "html-to-image";

const ASSET_BASE = import.meta.env.BASE_URL || "/";
const DATA_BASE = `${ASSET_BASE}data`;
const FALLBACK_HEADSHOT = "https://cdn.nba.com/headshots/nba/latest/260x190/fallback.png";
const TRANSPARENT_PIXEL =
  "data:image/gif;base64,R0lGODlhAQABAAAAACwAAAAAAQABAAA=";

const PODCAST_LOGOS = [
  {
    name: "CHGO Bulls Podcast",
    url: `${ASSET_BASE}podcast-logos/CHGO%20Bulls%20Logo.png`,
    headerFont: '"Gotham Black", "Gotham", "Cooper Black", serif',
    primaryColor: "#cd1041",
    secondaryColor: "#000000"
  },
  {
    name: "DLLS Mavs Podcast",
    url: `${ASSET_BASE}podcast-logos/DLLS%20Mavs%20Logo.png`,
    headerFont: '"Factoria", "Cooper Black", serif',
    primaryColor: "#00538c",
    secondaryColor: "#00843d"
  },
  {
    name: "DNVR Nuggets Podcast",
    url: `${ASSET_BASE}podcast-logos/DNVR%20Nuggets%20Logo.png`,
    headerFont: '"Industry Black", "Cooper Black", serif',
    primaryColor: "#ffc13c",
    secondaryColor: "#0f223c"
  },
  {
    name: "PHLY Sixers Podcast",
    url: `${ASSET_BASE}podcast-logos/PHLY%20Sixers%20logo.png`,
    headerFont: '"Bree Serif", "Cooper Black", serif',
    primaryColor: "#006bb6",
    secondaryColor: "#dd0031"
  },
  {
    name: "PHNX Suns Podcast",
    url: `${ASSET_BASE}podcast-logos/PHNX%20Suns%20Logo.png`,
    headerFont: '"Falcon Sport One", "Cooper Black", serif',
    primaryColor: "#e56020",
    secondaryColor: "#35275f"
  }
];

const SEASON_TYPES = ["Regular Season", "Playoffs"];

function slugSeasonType(seasonType) {
  return String(seasonType || "Regular Season").toLowerCase().replace(/\s+/g, "-");
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${path}`);
  }
  return response.json();
}

function rollingAverage(values, windowSize) {
  const out = [];
  for (let i = 0; i < values.length; i += 1) {
    const start = Math.max(0, i - windowSize + 1);
    const slice = values.slice(start, i + 1);
    out.push(slice.reduce((a, b) => a + b, 0) / slice.length);
  }
  return out;
}

function toDateLabel(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function isNumericValue(value) {
  return Number.isFinite(Number(value));
}

async function blobToDataUrl(blob) {
  return await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => resolve(String(reader.result || ""));
    reader.onerror = reject;
    reader.readAsDataURL(blob);
  });
}

async function fetchImageAsDataUrl(url) {
  const response = await fetch(url, { mode: "cors", cache: "no-store" });
  if (!response.ok) throw new Error(`Image fetch failed: ${response.status}`);
  const blob = await response.blob();
  return blobToDataUrl(blob);
}

function inferStatFields(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const blacklist = new Set(["PLAYER_ID", "TEAM_ID", "GAME_ID", "GAME_DATE_EST"]);
  const keys = Object.keys(rows[0] || {});
  return keys.filter((key) => {
    if (blacklist.has(key) || key.endsWith("_RANK")) return false;
    return rows.some((row) => isNumericValue(row[key]));
  });
}

function withHeadshot(player) {
  const pid = Number(player?.player_id || 0);
  const raw = String(player?.headshot_url || "");
  const cdn = `https://cdn.nba.com/headshots/nba/latest/1040x760/${pid}.png`;
  return {
    ...player,
    player_id: pid,
    headshot_url: raw.startsWith("http://") || raw.startsWith("https://") ? raw : cdn
  };
}

function App() {
  const exportRef = useRef(null);
  const logsCacheRef = useRef(new Map());

  const [manifest, setManifest] = useState(null);
  const [players, setPlayers] = useState([]);

  const [playerSearch, setPlayerSearch] = useState("");
  const [playerId, setPlayerId] = useState(0);

  const [seasonType, setSeasonType] = useState("Regular Season");
  const [season, setSeason] = useState("2025-26");
  const [logoName, setLogoName] = useState(PODCAST_LOGOS[0].name);

  const [rollingWindow, setRollingWindow] = useState(5);
  const [statField, setStatField] = useState("");
  const [trendData, setTrendData] = useState(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    async function init() {
      try {
        const m = await fetchJson(`${DATA_BASE}/manifest.json`);
        setManifest(m);
        setSeason(m.default_season || m.seasons?.[0] || "2025-26");
      } catch (err) {
        setError(`Failed to load static data manifest: ${err.message}`);
      }
    }
    init();
  }, []);

  useEffect(() => {
    async function loadPlayers() {
      if (!manifest || !season) return;
      setError("");
      setTrendData(null);
      setStatField("");

      try {
        const playersPath = manifest?.files?.players?.[season] || `players/${season}.json`;
        const payload = await fetchJson(`${DATA_BASE}/${playersPath}`);
        const roster = (payload.players || [])
          .filter((p) => p.is_active !== false)
          .map(withHeadshot)
          .sort((a, b) => String(a.name).localeCompare(String(b.name)));

        setPlayers(roster);
        if (roster.length > 0) {
          const first = roster[0];
          setPlayerId(first.player_id);
          setPlayerSearch(`${first.name} (${first.player_id})`);
        } else {
          setPlayerId(0);
          setPlayerSearch("");
        }
      } catch (err) {
        setPlayers([]);
        setPlayerId(0);
        setPlayerSearch("");
        setError(`Failed to load player list for ${season}: ${err.message}`);
      }
    }

    loadPlayers();
  }, [manifest, season]);

  const playerOptions = useMemo(
    () => players.map((p) => ({ ...p, label: `${p.name} (${p.player_id})` })),
    [players]
  );

  const playerLabelToId = useMemo(() => {
    const map = new Map();
    for (const p of playerOptions) map.set(p.label, p.player_id);
    return map;
  }, [playerOptions]);

  const selectedPlayer = useMemo(
    () => players.find((p) => p.player_id === Number(playerId)),
    [players, playerId]
  );

  const selectedLogo = useMemo(
    () => PODCAST_LOGOS.find((x) => x.name === logoName) || PODCAST_LOGOS[0],
    [logoName]
  );

  const seasonOptions = useMemo(() => manifest?.seasons || [season], [manifest, season]);

  async function getSeasonLogs(selectedSeason, selectedSeasonType) {
    const typeSlug = slugSeasonType(selectedSeasonType);
    const cacheKey = `${selectedSeason}::${typeSlug}`;

    if (logsCacheRef.current.has(cacheKey)) {
      return logsCacheRef.current.get(cacheKey);
    }

    const path =
      manifest?.files?.gamelogs?.[selectedSeason]?.[typeSlug] ||
      `gamelogs/${selectedSeason}/${typeSlug}.json`;
    const payload = await fetchJson(`${DATA_BASE}/${path}`);
    logsCacheRef.current.set(cacheKey, payload);
    return payload;
  }

  async function loadTrends() {
    if (!playerId) return;
    setLoading(true);
    setError("");

    try {
      const payload = await getSeasonLogs(season, seasonType);
      const rows = (payload.rows || [])
        .filter((r) => Number(r.PLAYER_ID) === Number(playerId))
        .sort((a, b) => new Date(a.GAME_DATE) - new Date(b.GAME_DATE));

      if (!rows.length) {
        throw new Error(`No rows for ${selectedPlayer?.name || "player"} in ${season} ${seasonType}`);
      }

      const statFields = (payload.stat_fields && payload.stat_fields.length ? payload.stat_fields : inferStatFields(rows))
        .filter((f) => rows.some((r) => isNumericValue(r[f])));

      if (!statFields.length) {
        throw new Error("No numeric stat fields found in static gamelog data");
      }

      const preferred = statFields.includes("PTS") ? "PTS" : statFields[0];
      setStatField(preferred);
      setTrendData({
        season,
        season_type: seasonType,
        count: rows.length,
        rows,
        stat_fields: statFields
      });
    } catch (err) {
      setTrendData(null);
      setStatField("");
      setError(err.message || "Failed to load trends");
    } finally {
      setLoading(false);
    }
  }

  async function exportPng() {
    if (!exportRef.current) return;
    const restoreFns = [];
    try {
      const headshotImg = exportRef.current.querySelector(".player-side img");
      if (headshotImg) {
        const original = headshotImg.getAttribute("src") || "";
        try {
          const dataUrl = await fetchImageAsDataUrl(selectedPlayer?.headshot_url || FALLBACK_HEADSHOT);
          headshotImg.setAttribute("src", dataUrl);
        } catch {
          headshotImg.setAttribute("src", TRANSPARENT_PIXEL);
        }
        await waitForImageLoad(headshotImg);
        restoreFns.push(() => headshotImg.setAttribute("src", original));
      }

      const logoImg = exportRef.current.querySelector(".team-logo");
      if (logoImg) {
        const original = logoImg.getAttribute("src") || "";
        try {
          const logoUrl = new URL(selectedLogo.url, window.location.href).href;
          const dataUrl = await fetchImageAsDataUrl(logoUrl);
          logoImg.setAttribute("src", dataUrl);
        } catch {
          logoImg.setAttribute("src", TRANSPARENT_PIXEL);
        }
        await waitForImageLoad(logoImg);
        restoreFns.push(() => logoImg.setAttribute("src", original));
      }

      const dataUrl = await toPng(exportRef.current, {
        pixelRatio: 3,
        cacheBust: true,
        backgroundColor: "#f8fafc",
        imagePlaceholder: TRANSPARENT_PIXEL,
        filter: (node) => {
          if (!(node instanceof Element)) return true;
          return !node.classList.contains("modebar") && !node.classList.contains("modebar-container");
        }
      });

      const link = document.createElement("a");
      link.download = `${(selectedPlayer?.name || "player").replace(/\s+/g, "_")}_${statField || "stat"}_trend.png`;
      link.href = dataUrl;
      link.click();
    } catch (err) {
      setError(`Export failed: ${err?.message || String(err)}`);
    } finally {
      for (let i = restoreFns.length - 1; i >= 0; i -= 1) restoreFns[i]();
    }
  }

  const chartModel = useMemo(() => {
    if (!trendData?.rows?.length || !statField) return null;

    const rows = trendData.rows.filter((r) => isNumericValue(r[statField]));
    if (!rows.length) return null;

    const x = rows.map((r) => r.GAME_DATE);
    const y = rows.map((r) => Number(r[statField]));
    const rolling = rollingAverage(y, Math.max(1, Number(rollingWindow) || 1));

    return {
      data: [
        {
          type: "scatter",
          mode: "markers",
          x,
          y,
          text: rows.map(
            (r) => `${r.MATCHUP || ""}<br>${toDateLabel(r.GAME_DATE)}<br>${statField}: ${Number(r[statField]).toFixed(2)}`
          ),
          hovertemplate: "%{text}<extra></extra>",
          marker: {
            size: 11,
            color: "#2f7ed8",
            opacity: 0.86,
            line: { width: 1, color: "#fff" }
          },
          name: `${statField} (Daily)`
        },
        {
          type: "scatter",
          mode: "lines",
          x,
          y: rolling,
          line: { color: "#e63946", width: 4, shape: "spline" },
          name: `Rolling Avg (${rollingWindow})`
        }
      ],
      layout: {
        margin: { l: 64, r: 22, t: 16, b: 72 },
        height: 720,
        autosize: true,
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(255,255,255,0.7)",
        xaxis: {
          title: "Game Date",
          type: "date",
          nticks: 12,
          tickformat: "%b %-d",
          tickangle: -28,
          showgrid: false,
          linecolor: "#172554",
          tickfont: { size: 11 }
        },
        yaxis: {
          title: statField,
          automargin: true,
          gridcolor: "rgba(23,37,84,0.12)",
          zerolinecolor: "rgba(23,37,84,0.18)",
          tickfont: { size: 13 }
        },
        legend: { orientation: "h", x: 0.02, y: 1.09, font: { size: 15 } },
        font: { family: "Cooper Black, Cooper Std, serif", color: "#111827" }
      }
    };
  }, [trendData, statField, rollingWindow]);

  return (
    <div className="app">
      <div className="trend-card">
        <div className="panel">
          <div className="controls">
            <div>
              <label>Player (Search)</label>
              <input
                list="player-options"
                value={playerSearch}
                onChange={(e) => {
                  const value = e.target.value;
                  setPlayerSearch(value);
                  const resolved = playerLabelToId.get(value);
                  if (resolved) setPlayerId(resolved);
                }}
                placeholder="Type player name..."
              />
              <datalist id="player-options">
                {playerOptions.map((p) => (
                  <option key={p.player_id} value={p.label} />
                ))}
              </datalist>
            </div>

            <div>
              <label>Podcast Logo</label>
              <select value={logoName} onChange={(e) => setLogoName(e.target.value)}>
                {PODCAST_LOGOS.map((logo) => (
                  <option key={logo.name}>{logo.name}</option>
                ))}
              </select>
            </div>

            <div>
              <label>Season</label>
              <select value={season} onChange={(e) => setSeason(e.target.value)}>
                {seasonOptions.map((s) => (
                  <option key={s}>{s}</option>
                ))}
              </select>
            </div>

            <div>
              <label>Season Type</label>
              <select value={seasonType} onChange={(e) => setSeasonType(e.target.value)}>
                {SEASON_TYPES.map((st) => (
                  <option key={st}>{st}</option>
                ))}
              </select>
            </div>

            <div>
              <label>Stat</label>
              <select value={statField} onChange={(e) => setStatField(e.target.value)}>
                {(trendData?.stat_fields || []).map((f) => (
                  <option key={f}>{f}</option>
                ))}
              </select>
            </div>

            <div>
              <label>Rolling Window</label>
              <input
                type="number"
                min="1"
                max="20"
                value={rollingWindow}
                onChange={(e) => setRollingWindow(Number(e.target.value || 1))}
              />
            </div>
          </div>

          <div className="actions">
            <button className="primary" onClick={loadTrends} disabled={loading || !playerId}>
              Load Trend
            </button>
            <button onClick={exportPng} disabled={!chartModel}>
              Export PNG (HQ)
            </button>
          </div>

          {error ? <p className="small">Error: {error}</p> : null}
        </div>

        <div
          ref={exportRef}
          className="export-block"
          style={{
            "--header-font": selectedLogo.headerFont,
            "--header-primary": selectedLogo.primaryColor,
            "--header-secondary": selectedLogo.secondaryColor
          }}
        >
          <div className="export-header">
            <div className="player-side">
              <img
                src={selectedPlayer?.headshot_url || FALLBACK_HEADSHOT}
                alt={selectedPlayer?.name || "Player"}
                onError={(e) => {
                  e.currentTarget.src = FALLBACK_HEADSHOT;
                }}
              />
            </div>

            <div className="header-copy">
              <h1>{selectedPlayer?.name || "NBA Player"}</h1>
              <h2>
                {statField || "Stat"} Daily Trend | {season} {seasonType}
              </h2>
            </div>

            <div className="logo-side">
              <img
                className="team-logo"
                src={selectedLogo.url}
                alt={selectedLogo.name}
                onError={(e) => {
                  e.currentTarget.style.display = "none";
                }}
              />
            </div>
          </div>

          <div className="panel chart-wrap">
            {chartModel ? (
              <Plot
                data={chartModel.data}
                layout={chartModel.layout}
                style={{ width: "100%" }}
                config={{ displaylogo: false }}
                useResizeHandler
              />
            ) : (
              <p className="small">Select a player and click "Load Trend".</p>
            )}
          </div>
        </div>
      </div>

      <p className="small">{loading ? "Loading..." : `Rows: ${trendData?.count || 0}`}</p>
    </div>
  );
}

export default App;
