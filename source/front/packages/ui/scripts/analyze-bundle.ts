import fs from 'fs';
import path from 'path';

const uiBuildPath = path.join(__dirname, '../dist/index.js');

if (!fs.existsSync(uiBuildPath)) {
  console.error('Build output not found. Run pnpm build:ui first.');
  process.exit(1);
}

const bundle = fs.readFileSync(uiBuildPath, 'utf8');

// Count lines
const lines = bundle.split('\n').length;
console.log(`Total lines: ${lines}`);

// Count characters
const chars = bundle.length;
console.log(`Total characters: ${chars}`);

// Estimate gzipped size (approximate)
const gzipped = Math.ceil(chars * 0.3);
console.log(`Estimated gzipped size: ~${gzipped} bytes`);

if (gzipped > 50 * 1024) {
  console.error(`❌ Bundle exceeds 50KB gzipped threshold (${(gzipped / 1024).toFixed(1)}KB)`);
  process.exit(1);
} else {
  console.log(`✅ Bundle within 50KB gzipped threshold (${(gzipped / 1024).toFixed(1)}KB)`);
}
