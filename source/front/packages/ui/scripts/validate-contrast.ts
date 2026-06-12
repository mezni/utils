import { colors } from '@bornemap/tokens';

function hexToRgb(hex: string): [number, number, number] {
  const clean = hex.replace('#', '');
  const r = parseInt(clean.substring(0, 2), 16);
  const g = parseInt(clean.substring(2, 4), 16);
  const b = parseInt(clean.substring(4, 6), 16);
  return [r, g, b];
}

function relativeLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

function contrastRatio(hex1: string, hex2: string): number {
  const [r1, g1, b1] = hexToRgb(hex1);
  const [r2, g2, b2] = hexToRgb(hex2);
  const l1 = relativeLuminance(r1, g1, b1);
  const l2 = relativeLuminance(r2, g2, b2);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

interface Pair {
  fg: string;
  bg: string;
  name: string;
  threshold: number;
}

const pairs: Pair[] = [
  { fg: colors.light.foreground, bg: colors.light.background, name: 'light: foreground on background', threshold: 4.5 },
  { fg: colors.light.foreground, bg: colors.light.card, name: 'light: foreground on card', threshold: 4.5 },
  { fg: colors.light.cardForeground, bg: colors.light.card, name: 'light: cardForeground on card', threshold: 4.5 },
  { fg: colors.light.onPrimary, bg: colors.light.primary, name: 'light: onPrimary on primary', threshold: 4.5 },
  { fg: colors.light.onSecondary, bg: colors.light.secondary, name: 'light: onSecondary on secondary', threshold: 4.5 },
  { fg: colors.light.onAccent, bg: colors.light.accent, name: 'light: onAccent on accent', threshold: 4.5 },
  { fg: colors.light.onDestructive, bg: colors.light.destructive, name: 'light: onDestructive on destructive', threshold: 4.5 },
  { fg: colors.light.mutedForeground, bg: colors.light.muted, name: 'light: mutedForeground on muted', threshold: 3.0 },
  { fg: colors.dark.foreground, bg: colors.dark.background, name: 'dark: foreground on background', threshold: 4.5 },
  { fg: colors.dark.foreground, bg: colors.dark.card, name: 'dark: foreground on card', threshold: 4.5 },
  { fg: colors.dark.cardForeground, bg: colors.dark.card, name: 'dark: cardForeground on card', threshold: 4.5 },
  { fg: colors.dark.onPrimary, bg: colors.dark.primary, name: 'dark: onPrimary on primary', threshold: 4.5 },
  { fg: colors.dark.onSecondary, bg: colors.dark.secondary, name: 'dark: onSecondary on secondary', threshold: 4.5 },
  { fg: colors.dark.onAccent, bg: colors.dark.accent, name: 'dark: onAccent on accent', threshold: 4.5 },
  { fg: colors.dark.onDestructive, bg: colors.dark.destructive, name: 'dark: onDestructive on destructive', threshold: 4.5 },
  { fg: colors.dark.mutedForeground, bg: colors.dark.muted, name: 'dark: mutedForeground on muted', threshold: 3.0 },
];

let allPass = true;

for (const pair of pairs) {
  const ratio = contrastRatio(pair.fg, pair.bg);
  const pass = ratio >= pair.threshold;
  const status = pass ? '✓' : '✗';
  console.log(`${status} ${pair.name}: ${ratio.toFixed(2)}:1 (threshold ${pair.threshold}:1)`);
  if (!pass) allPass = false;
}

if (allPass) {
  console.log('\n✅ All color pairs pass WCAG AA contrast requirements.');
} else {
  console.log('\n❌ Some color pairs fail WCAG AA contrast requirements.');
  process.exit(1);
}
