import { defineConfig, type UserConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ command }): UserConfig => {
  const base: UserConfig = {
    plugins: [react()],
    build: {
      outDir: 'dist',
      sourcemap: false,
    },
  };

  if (command !== 'serve') return base;

  // ── Dev server (vite dev / make dev-ui) ─────────────────────────────────
  //
  // Mirrors the nginx.conf proxy rules so the same JS paths work in both
  // production (nginx) and dev (Vite built-in proxy) without changes.
  //
  // Proxy targets come from environment variables injected by docker-compose:
  //   API_TARGET  — host that runs FastAPI   (default: http://localhost:8080)
  //   PROM_TARGET — host that runs Prometheus (default: http://localhost:9090)
  //
  // usePolling: true is the safest default for Docker bind-mounts across
  // all host OS / Docker Desktop combinations.  Set interval higher if the
  // CPU overhead is noticeable on a large source tree.
  const apiTarget  = process.env['API_TARGET']  ?? 'http://localhost:8080';
  const promTarget = process.env['PROM_TARGET'] ?? 'http://localhost:9090';
  const wsTarget   = apiTarget.replace(/^http/, 'ws');

  return {
    ...base,
    server: {
      host: '0.0.0.0',   // listen on all interfaces inside the container
      port: 5173,
      strictPort: true,
      watch: {
        usePolling: true,
        interval: 300,
      },
      proxy: {
        // REST API  →  strips /api prefix, forwards to FastAPI
        '/api': {
          target: apiTarget,
          rewrite: (path) => path.replace(/^\/api/, ''),
          changeOrigin: true,
        },
        // WebSocket  →  no path rewrite, target is already ws://
        '/ws': {
          target: wsTarget,
          ws: true,
          changeOrigin: true,
        },
        // Prometheus query API  →  strips /prom prefix
        '/prom': {
          target: promTarget,
          rewrite: (path) => path.replace(/^\/prom/, ''),
          changeOrigin: true,
        },
      },
    },
  };
});
