const base = require('../../packages/ui/tailwind.config.base');

module.exports = {
  presets: [base],
  content: ['./index.html', './src/**/*.{ts,tsx}'],
};
