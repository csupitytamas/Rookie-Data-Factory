import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import * as path from 'path';

// A Vite konfigurációja a fejlesztői szerverhez, a beépülő modulokhoz és az elérési utak feloldásához.
export default defineConfig({
  plugins: [vue()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    watch: {
      usePolling: true,
    },
  },

  // Az elérési utak feloldásának konfigurálása: a '@' jel a 'src' könyvtárra mutat.
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src')
    }
  },

  // Build konfiguráció: a kimeneti könyvtár meghatározása és ürítése minden fordítás előtt.
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  }
})