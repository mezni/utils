module.exports = {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        ev: {
          bg: '#F8FAF6',       // Ultra-bright crisp canvas background
          surface: '#FFFFFF',  // Clean white card layers
          mapBg: '#EAF0E6',    // Light, organic map terrain base
          green: '#007943',    // High-contrast, legible forest/emerald green
          glow: '#00E676',     // Bright neon green for live map pins
          muted: '#6B7280',    // Slate gray for secondary text labels
          border: '#E5E7EB',   // Subtle light gray divider lines
        }
      },
      fontFamily: {
        sans: ['Plus Jakarta Sans', 'Inter', 'sans-serif'],
      }
    }
  },
  plugins: [],
}
