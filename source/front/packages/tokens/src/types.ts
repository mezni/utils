export interface ColorScheme {
  primary: string;
  onPrimary: string;
  secondary: string;
  onSecondary: string;
  accent: string;
  onAccent: string;
  background: string;
  foreground: string;
  card: string;
  cardForeground: string;
  muted: string;
  mutedForeground: string;
  border: string;
  destructive: string;
  onDestructive: string;
  success: string;
  warning: string;
  info: string;
  ring: string;
}

export interface Colors {
  light: ColorScheme;
  dark: ColorScheme;
}

export type SpacingKey = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 8 | 10 | 12 | 16;

export interface TypographyFontFamily {
  sans: string;
  mono: string;
}

export interface TypographyFontSize {
  xs: number;
  sm: number;
  base: number;
  lg: number;
  xl: number;
  '2xl': number;
  '3xl': number;
  '4xl': number;
}

export interface TypographyFontWeight {
  normal: number;
  medium: number;
  semibold: number;
  bold: number;
  extrabold: number;
}

export interface TypographyLineHeight {
  tight: number;
  normal: number;
  relaxed: number;
}

export interface TypographyTokens {
  font: {
    family: TypographyFontFamily;
    size: TypographyFontSize;
    weight: TypographyFontWeight;
    lineHeight: TypographyLineHeight;
  };
}

export interface ShadowTokens {
  sm: string;
  md: string;
  lg: string;
  xl: string;
}

export interface RadiiTokens {
  none: number;
  sm: number;
  md: number;
  lg: number;
  full: number;
}

export interface BreakpointTokens {
  mobile: number;
  tablet: number;
  desktop: number;
  wide: number;
}

export interface OpacityTokens {
  disabled: number;
  overlay: number;
  subtle: number;
}

export interface IconSizeTokens {
  sm: number;
  md: number;
  lg: number;
  xl: number;
}
