import baseConfig from '@borne-map/ui/tailwind.config.base.js'

/** @type {import('tailwindcss').Config} */
export default {
  presets: [baseConfig],
  content: [
    './index.html',
    './src/**/*.{ts,tsx}',
  ],
}
