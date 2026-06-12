import React from 'react';
import { colors as tokenColors, spacing, radii } from '@bornemap/tokens';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import type { ButtonVariant, ButtonSize } from './Button';

export interface ButtonWebProps {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  disabled?: boolean;
  fullWidth?: boolean;
  onPress: () => void;
  children: React.ReactNode;
  className?: string;
}

export function Button({
  variant = 'primary',
  size = 'md',
  loading = false,
  disabled = false,
  fullWidth = false,
  onPress,
  children,
  className,
}: ButtonWebProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;

  const variantStyle = getWebVariantStyle(variant, theme, disabled);
  const sizeStyle = getWebSizeStyle(size);

  const baseStyle: React.CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: `${radii.md}px`,
    border: variant === 'outline' ? `1px solid ${disabled ? theme.muted : theme.primary}` : 'none',
    cursor: disabled || loading ? 'not-allowed' : 'pointer',
    opacity: disabled ? 0.5 : 1,
    width: fullWidth ? '100%' : undefined,
    fontWeight: 600,
    transition: 'background-color 0.2s, opacity 0.2s',
    pointerEvents: disabled || loading ? 'none' : 'auto',
    ...variantStyle,
    ...sizeStyle,
  };

  return (
    <button
      onClick={onPress}
      disabled={disabled || loading}
      className={className}
      style={baseStyle}
    >
      {loading ? '...' : children}
    </button>
  );
}

function getWebVariantStyle(
  variant: ButtonVariant,
  theme: typeof tokenColors.light,
  disabled: boolean,
): React.CSSProperties {
  switch (variant) {
    case 'primary':
      return { backgroundColor: disabled ? theme.muted : theme.primary, color: theme.onPrimary };
    case 'secondary':
      return { backgroundColor: disabled ? theme.muted : theme.secondary, color: theme.onSecondary };
    case 'outline':
      return { backgroundColor: 'transparent', color: disabled ? theme.mutedForeground : theme.primary };
    case 'ghost':
      return { backgroundColor: 'transparent', color: disabled ? theme.mutedForeground : theme.foreground };
    case 'destructive':
      return { backgroundColor: disabled ? theme.muted : theme.destructive, color: theme.onDestructive };
  }
}

function getWebSizeStyle(size: ButtonSize): React.CSSProperties {
  switch (size) {
    case 'sm':
      return { padding: `${spacing[2]}px ${spacing[3]}px`, fontSize: 14 };
    case 'md':
      return { padding: `${spacing[3]}px ${spacing[4]}px`, fontSize: 16 };
    case 'lg':
      return { padding: `${spacing[4]}px ${spacing[6]}px`, fontSize: 18 };
  }
}
