import { useEffect, useMemo, useRef, useState } from "react";
import Plot from "react-plotly.js";
import { toPng } from "html-to-image";

const BROWSER_ORIGIN = typeof window !== "undefined" ? window.location.origin : "";
const URL_API_BASE =
  typeof window !== "undefined"
    ? new URLSearchParams(window.location.search).get("api") || ""
    : "";
const DEFAULT_API_BASE = import.meta.env.DEV ? "http://127.0.0.1:8000" : BROWSER_ORIGIN;
const API_BASE = (URL_API_BASE || import.meta.env.VITE_API_BASE || DEFAULT_API_BASE).replace(/\/+$/, "");
const API_CANDIDATES = Array.from(
  new Set(
    [API_BASE, BROWSER_ORIGIN, "http://127.0.0.1:8000"]
      .map((x) => String(x || "").replace(/\/+$/, ""))
      .filter(Boolean)
  )
);
const ASSET_BASE = import.meta.env.BASE_URL || "/";

async function fetchJsonWithFallback(path) {
  let lastErr = null;
  for (const base of API_CANDIDATES) {
    try {
      const res = await fetch(`${base}${path}`);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr || new Error("Failed to fetch from all API backends");
}

const API = {
  health: () => fetchJsonWithFallback("/api/health"),
  players: (season) => fetchJsonWithFallback(`/api/players?season=${encodeURIComponent(season)}`),
  trends: (playerId, season, seasonType) =>
    fetchJsonWithFallback(
      `/api/trends/player?player_id=${playerId}&source=overall&season=${encodeURIComponent(season)}&season_type=${encodeURIComponent(seasonType)}`
    )
};

const FALLBACK_HEADSHOT = "https://cdn.nba.com/headshots/nba/latest/260x190/fallback.png";

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

function seasonOptionsFromDefault(defaultSeason, count = 8) {
  const y = Number(String(defaultSeason || "2025-26").slice(0, 4));
  if (!Number.isFinite(y)) return ["2025-26", "2024-25", "2023-24"];
  const out = [];
  for (let i = 0; i < count; i += 1) {
    const start = y - i;
    out.push(`${start}-${String((start + 1) % 100).padStart(2, "0")}`);
  }
  return out;
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

function App() {
  const exportRef = useRef(null);

  const [health, setHealth] = useState(null);
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
    API.health()
      .then((h) => {
        setHealth(h);
        const defaultSeason = h.default_season || "2025-26";
        setSeason(defaultSeason);
        return API.players(defaultSeason);
      })
      .then((p) => {
        const roster = (p.players || []).filter((x) => x.is_active);
        setPlayers(roster);
        if (roster.length) {
          const first = roster[0];
          setPlayerId(first.player_id);
          setPlayerSearch(`${first.name} (${first.player_id})`);
        }
      })
      .catch((e) => setError(e.message || "Failed to initialize"));
  }, []);

  const playerOptions = useMemo(
    () =>
      [...players]
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((p) => ({ ...p, label: `${p.name} (${p.player_id})` })),
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

  const seasonOptions = useMemo(
    () => seasonOptionsFromDefault(health?.default_season || season, 10),
    [health?.default_season, season]
  );

  async function loadTrends() {
    if (!playerId) return;
    setLoading(true);
    setError("");
    try {
      const result = await API.trends(playerId, season, seasonType);
      if (result.detail) throw new Error(result.detail);

      result.rows = (result.rows || []).slice().sort((a, b) => new Date(a.GAME_DATE) - new Date(b.GAME_DATE));
      setTrendData(result);

      const preferred = (result.stat_fields || []).includes("PTS") ? "PTS" : (result.stat_fields || [])[0] || "";
      setStatField(preferred);
    } catch (e) {
      setError(e.message || "Failed to load trends");
      setTrendData(null);
      setStatField("");
    } finally {
      setLoading(false);
    }
  }

  async function exportPng() {
    if (!exportRef.current) return;
    try {
      const dataUrl = await toPng(exportRef.current, {
        pixelRatio: 3,
        cacheBust: true,
        backgroundColor: "#f8fafc"
      });

      const link = document.createElement("a");
      link.download = `${(selectedPlayer?.name || "player").replace(/\s+/g, "_")}_${statField || "stat"}_trend.png`;
      link.href = dataUrl;
      link.click();
    } catch (err) {
      setError(`Export failed: ${err?.message || err}`);
    }
  }

  const chartModel = useMemo(() => {
    if (!trendData?.rows?.length || !statField) return null;

    const rows = trendData.rows.filter((r) => Number.isFinite(Number(r[statField])));
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
          text: rows.map((r) => `${r.MATCHUP || ""}<br>${toDateLabel(r.GAME_DATE)}<br>${statField}: ${Number(r[statField]).toFixed(2)}`),
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
                <option>Regular Season</option>
                <option>Playoffs</option>
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
            <h2>{statField || "Stat"} Daily Trend | {season} {seasonType}</h2>
          </div>

          <div className="logo-side">
            <img
              className="team-logo"
              src={selectedLogo.url}
              alt={selectedLogo.name}
              crossOrigin="anonymous"
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
