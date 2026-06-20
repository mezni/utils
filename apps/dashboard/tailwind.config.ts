import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
      },
      colors: {
        primary: {
          DEFAULT: "#F97316",
          50: "#FFF7ED",
          100: "#FFEDD5",
          200: "#FED7AA",
          300: "#FDBA74",
          400: "#FB923C",
          500: "#F97316",
          600: "#EA580C",
          700: "#C2410C",
          800: "#9A3412",
          900: "#7C2D12",
        },
        surface: {
          DEFAULT: "#1F2937",
          dark: "#111827",
          light: "#37414F",
        },
        accent: {
          DEFAULT: "#22C55E",
          hover: "#16A34A",
        },
        destructive: {
          DEFAULT: "#EF4444",
          hover: "#DC2626",
        },
        muted: {
          DEFAULT: "#37414F",
          foreground: "#9CA3AF",
        },
        border: {
          DEFAULT: "#374151",
        },
      },
    },
  },
  plugins: [],
} satisfies Config;
