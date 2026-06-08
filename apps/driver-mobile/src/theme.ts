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
  body: {
    fontSize: 14,
    lineHeight: 21,
  },
  label: {
    fontSize: 12,
    lineHeight: 15,
    letterSpacing: 0.5,
  },
} as const;

export const spacing = {
  xs: 4,
  sm: 8,
  md: 16,
  lg: 24,
  xl: 32,
} as const;
