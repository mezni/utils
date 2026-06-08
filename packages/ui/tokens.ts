export const colors = {
  pine: '#007943',
  pineDeep: '#166534',
  mossTint: '#EAF0E6',
  ink: '#1F2937',
  inkMuted: '#6B7280',
  inkSubtle: '#9CA3AF',
  surface: '#FFFFFF',
  surfaceMuted: '#F3F4F6',
  border: '#D1D5DB',
  borderSubtle: '#E5E7EB',
  error: '#DC2626',
  errorSurface: '#FEF2F2',
  success: '#16A34A',
} as const;

export const typography = {
  fontFamily: "system-ui, -apple-system, sans-serif",
  display: { fontSize: 'clamp(1.5rem, 4vw, 2rem)', fontWeight: '700' as const, lineHeight: 1.2 },
  title: { fontSize: 'clamp(1rem, 2.5vw, 1.25rem)', fontWeight: '600' as const, lineHeight: 1.4 },
  body: { fontSize: 14, fontWeight: '400' as const, lineHeight: 1.5 },
  label: { fontSize: 12, fontWeight: '500' as const, lineHeight: 1.25, letterSpacing: 0.5 },
} as const;

export const spacing = {
  xs: 4,
  sm: 8,
  md: 16,
  lg: 24,
  xl: 32,
} as const;

export const borderRadius = {
  sm: 4,
  md: 6,
  lg: 8,
} as const;
