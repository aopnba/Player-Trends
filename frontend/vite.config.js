import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ command }) => ({
  // Use relative paths in production so the app works on GitHub Pages and custom hosts.
  base: command === "build" ? "./" : "/",
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
