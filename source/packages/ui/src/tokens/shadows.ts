export const shadows = {
  card: '0 1px 3px rgba(0, 0, 0, 0.1), 0 1px 2px rgba(0, 0, 0, 0.06)',
  panel: '0 4px 6px rgba(0, 0, 0, 0.1), 0 2px 4px rgba(0, 0, 0, 0.06)',
  float: '0 10px 15px rgba(0, 0, 0, 0.1), 0 4px 6px rgba(0, 0, 0, 0.05)',
  pin: '0 0 12px rgba(0, 230, 118, 0.5), 0 0 24px rgba(0, 230, 118, 0.3)',
} as const;

export type ShadowTokens = typeof shadows;
