import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import * as path from 'path';

export default defineConfig({
  plugins: [vue()],
  server: {
    host: '0.0.0.0', // Hálózati elérés Dockerben
    port: 5173,
    strictPort: true,
    watch: {
      usePolling: true, // Windows-on szükséges a fájlváltozások észleléséhez
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src')
    }
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  }
})