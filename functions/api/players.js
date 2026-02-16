import { defaultSeason, extractRows, fetchStats, headshotUrl, json, readQuery } from "./_lib/nba.js";

export const onRequestGet = async ({ request, context }) => {
  try {
    const season = readQuery(request.url, "season", defaultSeason());
    const payload = await fetchStats(
      "commonallplayers",
      {
        LeagueID: "00",
        Season: season,
        IsOnlyCurrentSeason: "1",
      },
      context
    );

    const rows = extractRows(payload, "CommonAllPlayers");
    const players = rows.map((row) => {
      const playerId = Number(row.PERSON_ID);
      return {
        player_id: playerId,
        name: row.DISPLAY_FIRST_LAST,
        team_id: row.TEAM_ID,
        team: row.TEAM_ABBREVIATION,
        is_active: Number(row.ROSTERSTATUS) === 1,
        headshot_url: headshotUrl(playerId),
      };
    });

    return json({
      season,
      count: players.length,
      players,
    });
  } catch (error) {
    return json({ detail: `Failed players fetch: ${error.message}` }, 502);
  }
};
