import {
  defaultSeason,
  extractRows,
  fetchStats,
  headshotUrl,
  json,
  numericFields,
  readQuery,
  sortByGameDate,
} from "../_lib/nba.js";

export const onRequestGet = async ({ request, context }) => {
  try {
    const playerId = Number(readQuery(request.url, "player_id", "0"));
    const season = readQuery(request.url, "season", defaultSeason());
    const seasonType = readQuery(request.url, "season_type", "Regular Season");
    const source = readQuery(request.url, "source", "overall").toLowerCase();

    if (!playerId) {
      return json({ detail: "player_id is required" }, 400);
    }
    if (source !== "overall") {
      return json({ detail: "Only source=overall is supported in public-free mode" }, 400);
    }

    const payload = await fetchStats(
      "playergamelogs",
      {
        Season: season,
        SeasonType: seasonType,
        PlayerID: String(playerId),
        LeagueID: "00",
      },
      context
    );

    const rows = sortByGameDate(extractRows(payload, "PlayerGameLogs")).map((row) => ({
      ...row,
      headshot_url: headshotUrl(playerId),
    }));

    return json({
      player_id: playerId,
      source: "overall",
      season,
      season_type: seasonType,
      count: rows.length,
      rows,
      stat_fields: numericFields(rows),
    });
  } catch (error) {
    return json({ detail: `Failed trends fetch: ${error.message}` }, 502);
  }
};
