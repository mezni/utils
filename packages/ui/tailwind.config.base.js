import {
  brandPrimary, brandSecondary, brandLight, brandDark,
  success, warning, error,
  neutral100, neutral200, neutral300, neutral400, neutral500, neutral600, neutral700,
} from './src/tokens/colors'
import {
  spacing0, spacing1, spacing2, spacing3, spacing4, spacing5,
  spacing6, spacing7, spacing8, spacing10, spacing12,
} from './src/tokens/spacing'
import { radiusSm, radiusMd, radiusLg, radiusXl, radiusFull } from './src/tokens/radius'
import {
  fontFamilySans, fontFamilyMono,
  fontSizeSm, fontSizeBase, fontSizeMd, fontSizeLg, fontSizeXl, fontSize2xl, fontSize3xl, fontSize4xl,
  fontWeightRegular, fontWeightMedium, fontWeightSemibold, fontWeightBold,
  lineHeightTight, lineHeightNormal, lineHeightRelaxed,
} from './src/tokens/typography'

/** @type {import('tailwindcss').Config} */
export default {
  theme: {
    extend: {
      colors: {
        brand: {
          primary: brandPrimary,
          secondary: brandSecondary,
          light: brandLight,
          dark: brandDark,
        },
        semantic: {
          success,
          warning,
          error,
        },
        neutral: {
          100: neutral100,
          200: neutral200,
          300: neutral300,
          400: neutral400,
          500: neutral500,
          600: neutral600,
          700: neutral700,
        },
      },
      spacing: {
        0: spacing0,
        1: spacing1,
        2: spacing2,
        3: spacing3,
        4: spacing4,
        5: spacing5,
        6: spacing6,
        7: spacing7,
        8: spacing8,
        10: spacing10,
        12: spacing12,
      },
      borderRadius: {
        sm: `${radiusSm}px`,
        md: `${radiusMd}px`,
        lg: `${radiusLg}px`,
        xl: `${radiusXl}px`,
        full: `${radiusFull}px`,
      },
      fontFamily: {
        sans: [fontFamilySans, 'sans-serif'],
        mono: [fontFamilyMono, 'monospace'],
      },
      fontSize: {
        sm: `${fontSizeSm}px`,
        base: `${fontSizeBase}px`,
        md: `${fontSizeMd}px`,
        lg: `${fontSizeLg}px`,
        xl: `${fontSizeXl}px`,
        '2xl': `${fontSize2xl}px`,
        '3xl': `${fontSize3xl}px`,
        '4xl': `${fontSize4xl}px`,
      },
      fontWeight: {
        regular: fontWeightRegular,
        medium: fontWeightMedium,
        semibold: fontWeightSemibold,
        bold: fontWeightBold,
      },
      lineHeight: {
        tight: lineHeightTight,
        normal: lineHeightNormal,
        relaxed: lineHeightRelaxed,
      },
    },
  },
}
