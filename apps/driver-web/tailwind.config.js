/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        pine: '#007943',
        'pine-deep': '#166534',
        'moss-tint': '#EAF0E6',
        ink: '#1F2937',
        'ink-muted': '#6B7280',
        'ink-subtle': '#9CA3AF',
        surface: '#FFFFFF',
        'surface-muted': '#F3F4F6',
        border: '#D1D5DB',
        'border-subtle': '#E5E7EB',
        error: '#DC2626',
        'error-surface': '#FEF2F2',
        success: '#16A34A',
      },
      fontFamily: {
        body: ['system-ui', '-apple-system', 'sans-serif'],
      },
      borderRadius: {
        sm: '4px',
        md: '6px',
        lg: '8px',
      },
    },
  },
  plugins: [],
}
