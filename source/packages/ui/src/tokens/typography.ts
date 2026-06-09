export const typography = {
  font: {
    family: {
      sans: 'Plus Jakarta Sans, Inter, system-ui, sans-serif',
      arabic: 'Cairo, system-ui, sans-serif',
    },
  },
  size: {
    xs: '10px',
    sm: '12px',
    base: '14px',
    lg: '16px',
    xl: '18px',
    '2xl': '20px',
    '3xl': '24px',
  },
  weight: {
    regular: 400,
    medium: 500,
    semibold: 600,
    bold: 700,
    extrabold: 800,
  },
} as const;

export type TypographyTokens = typeof typography;
