import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const backendTarget = env.VITE_DEV_API_PROXY_TARGET || 'http://127.0.0.1:8787'

  return {
    plugins: [react()],
    server: {
      proxy: {
        '/api/accounts': {
          target: backendTarget,
          changeOrigin: true,
          rewrite: path => path.replace(/^\/api/, ''),
        },
        '/api/categories': {
          target: backendTarget,
          changeOrigin: true,
          rewrite: path => path.replace(/^\/api/, ''),
        },
        '/api/transactions': {
          target: backendTarget,
          changeOrigin: true,
          rewrite: path => path.replace(/^\/api/, ''),
        },
        '/api/oauth': {
          target: backendTarget,
          changeOrigin: true,
          rewrite: path => path.replace(/^\/api/, ''),
        },
      },
    },
  }
})
