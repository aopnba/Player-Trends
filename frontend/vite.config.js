import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ command }) => ({
  // Set VITE_BASE_PATH=/Player-Trends/ only for GitHub Pages builds.
  base: command === "build" ? process.env.VITE_BASE_PATH || "/" : "/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8000",
      "/headshots": "http://127.0.0.1:8000",
      "/team-logos": "http://127.0.0.1:8000"
    }
  }
}));
