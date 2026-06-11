export interface ColorPalette {
  primary: string;
  secondary: string;
  background: string;
  surface: string;
  text: string;
  textSecondary: string;
  error: string;
  success: string;
  border: string;
  skeleton: string;
  skeletonHighlight: string;
  overlay: string;
}

export interface ThemeColors {
  light: ColorPalette;
  dark: ColorPalette;
}

const common = {
  primary: '#0066FF',
  secondary: '#6B7280',
  error: '#EF4444',
  success: '#10B981',
};

export const colors: ThemeColors = {
  light: {
    ...common,
    background: '#FFFFFF',
    surface: '#F9FAFB',
    text: '#111827',
    textSecondary: '#6B7280',
    border: '#E5E7EB',
    skeleton: '#E5E7EB',
    skeletonHighlight: '#F3F4F6',
    overlay: 'rgba(0, 0, 0, 0.5)',
  },
  dark: {
    ...common,
    background: '#111827',
    surface: '#1F2937',
    text: '#F9FAFB',
    textSecondary: '#9CA3AF',
    border: '#374151',
    skeleton: '#374151',
    skeletonHighlight: '#4B5563',
    overlay: 'rgba(0, 0, 0, 0.7)',
  },
};
