/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        background: '#020617',
        foreground: '#F8FAFC',
        surface: '#0F172A',
        surfaceAlt: '#1A1E2F',
        muted: '#334155',
        border: '#1E2938',
        primary: '#F97316',
        accent: '#22C55E',
        destructive: '#EF4444',
        warning: '#FBBF24',
        info: '#3B82F6',
      },
      fontFamily: {
        mono: ['Fira Code', 'Cascadia Code', 'JetBrains Mono', 'monospace'],
        sans: ['Fira Sans', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
