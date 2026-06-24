/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        background: '#020617',
        foreground: '#F8FAFC',
        muted: '#1A1E2F',
        surface: '#0F172A',
        border: '#1E293B',
      },
    },
  },
  plugins: [],
}
