import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const backendTarget = env.VITE_DEV_API_PROXY_TARGET || 'http://127.0.0.1:8787'

  return {
    plugins: [react()],
    server: {
      proxy: {
        // Single catch-all: every /api/* path goes to the backend. Never
        // enumerate routes here — a missing entry makes Vite serve
        // index.html for that API call ("<!doctype is not valid JSON").
        '/api': {
          target: backendTarget,
          changeOrigin: true,
          rewrite: path => path.replace(/^\/api/, ''),
        },
      },
    },
  }
})
