import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// base "./" so the built bundle can be served from any path prefix
// (FastAPI static mount in production). In development every runtime route,
// including PATCH and SSE, stays same-origin through Vite. This keeps the
// browser contract identical to the built workbench and avoids a special CORS
// path just for identity edits.
export default defineConfig(({ mode }) => {
  const runtime = loadEnv(mode, ".", "").VITE_RUNTIME_ORIGIN || "http://127.0.0.1:8765";
  return {
    plugins: [react()],
    base: "./",
    server: {
      proxy: Object.fromEntries(
        [
          "/health",
          "/status",
          "/projects",
          "/substrates",
          "/engrams",
          "/world",
          "/runtime",
          "/task-fronts",
          "/task-offers",
          "/task-relationships",
          "/activity-centers",
          "/events",
          "/pulse",
          "/tuning",
          "/delegate",
          "/delegations",
          "/harness-turns",
          "/harness",
        ].map((path) => [path, runtime]),
      ),
    },
  };
});
