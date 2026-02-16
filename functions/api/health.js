import { defaultSeason, json } from "./_lib/nba.js";

export const onRequestGet = async () => {
  return json({
    ok: true,
    mode: "cloudflare-pages-functions",
    default_season: defaultSeason(),
    note: "Live data from stats.nba.com via Pages Functions",
  });
};
