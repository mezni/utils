const baseConfig = require('../../packages/ui/tailwind.config.base.js');

/** @type {import('tailwindcss').Config} */
module.exports = {
  presets: [baseConfig],
  content: ['./index.html', './src/**/*.{ts,tsx}'],
};
