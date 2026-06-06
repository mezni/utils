import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@borne-map/ui': path.resolve(__dirname, '../../packages/ui/src')
    }
  },
  server: {
    port: 5174,
    open: true
  }
})