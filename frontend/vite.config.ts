import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const backendTarget = env.VITE_DEV_API_PROXY_TARGET || 'http://127.0.0.1:8787'

  return {
    plugins: [react()],
    server: {
      proxy: {
        // Keep /api intact when forwarding to Wrangler. Its asset routing
        // runs the Worker first for /api/*, then toBackendRequest removes the
        // prefix before Express routing. Stripping it here makes Wrangler
        // treat /transactions etc. as SPA navigations and return index.html.
        '/api': {
          target: backendTarget,
          changeOrigin: true,
        },
      },
    },
  }
})
