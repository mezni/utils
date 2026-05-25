/** @type {import('tailwindcss').Config} */
const config = {
  theme: {
    extend: {
      colors: {
        accent: {
          DEFAULT: "#22c55e",
          light: "#4ade80",
          dark: "#16a34a",
          muted: "#dcfce7",
        },
        surface: {
          DEFAULT: "#ffffff",
          overlay: "rgba(255,255,255,0.92)",
          card: "#f9fafb",
        },
      },
      borderRadius: {
        md: "6px",
        lg: "8px",
        xl: "0.75rem",
        "2xl": "1rem",
      },
      spacing: {
        "component-gap": "16px",
        "internal-pad": "24px",
        "section-gap": "32px",
      },
      boxShadow: {
        float: "0 4px 24px rgba(0,0,0,0.10)",
        card: "0 2px 12px rgba(0,0,0,0.07)",
      },
    },
  },
  plugins: [],
}

module.exports = config
