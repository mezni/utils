import React from 'react';
import {
  TouchableOpacity,
  Text,
  ActivityIndicator,
  StyleSheet,
  ViewStyle,
  TextStyle,
} from 'react-native';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import { colors as tokenColors, spacing, radii } from '@bornemap/tokens';

export type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost' | 'destructive';
export type ButtonSize = 'sm' | 'md' | 'lg';

export interface ButtonProps {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  disabled?: boolean;
  fullWidth?: boolean;
  onPress: () => void;
  children: React.ReactNode;
  className?: string;
  style?: ViewStyle;
}

export function Button({
  variant = 'primary',
  size = 'md',
  loading = false,
  disabled = false,
  fullWidth = false,
  onPress,
  children,
  style,
}: ButtonProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;

  const variantStyles = getVariantStyles(variant, theme, disabled);
  const sizeStyles = getSizeStyles(size);

  return (
    <TouchableOpacity
      onPress={onPress}
      disabled={disabled || loading}
      activeOpacity={0.7}
      style={[
        styles.base,
        variantStyles.button,
        sizeStyles.button,
        fullWidth && styles.fullWidth,
        disabled && styles.disabled,
        style,
      ]}
    >
      {loading ? (
        <ActivityIndicator
          color={variantStyles.text.color as string}
          size="small"
        />
      ) : (
        <Text style={[styles.text, variantStyles.text, sizeStyles.text]}>
          {children}
        </Text>
      )}
    </TouchableOpacity>
  );
}

function getVariantStyles(variant: ButtonVariant, theme: typeof tokenColors.light, disabled: boolean) {
  const styles: { button: ViewStyle; text: TextStyle } = {
    button: {},
    text: {},
  };

  switch (variant) {
    case 'primary':
      styles.button = { backgroundColor: disabled ? theme.muted : theme.primary };
      styles.text = { color: theme.onPrimary };
      break;
    case 'secondary':
      styles.button = { backgroundColor: disabled ? theme.muted : theme.secondary };
      styles.text = { color: theme.onSecondary };
      break;
    case 'outline':
      styles.button = {
        backgroundColor: 'transparent',
        borderWidth: 1,
        borderColor: disabled ? theme.muted : theme.primary,
      };
      styles.text = { color: disabled ? theme.mutedForeground : theme.primary };
      break;
    case 'ghost':
      styles.button = { backgroundColor: 'transparent' };
      styles.text = { color: disabled ? theme.mutedForeground : theme.foreground };
      break;
    case 'destructive':
      styles.button = { backgroundColor: disabled ? theme.muted : theme.destructive };
      styles.text = { color: theme.onDestructive };
      break;
  }

  return styles;
}

function getSizeStyles(size: ButtonSize) {
  switch (size) {
    case 'sm':
      return {
        button: { paddingVertical: spacing[2], paddingHorizontal: spacing[3] } as ViewStyle,
        text: { fontSize: 14 } as TextStyle,
      };
    case 'md':
      return {
        button: { paddingVertical: spacing[3], paddingHorizontal: spacing[4] } as ViewStyle,
        text: { fontSize: 16 } as TextStyle,
      };
    case 'lg':
      return {
        button: { paddingVertical: spacing[4], paddingHorizontal: spacing[6] } as ViewStyle,
        text: { fontSize: 18 } as TextStyle,
      };
  }
}

const styles = StyleSheet.create({
  base: {
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: radii.md,
    flexDirection: 'row',
  },
  fullWidth: {
    width: '100%',
  },
  disabled: {
    opacity: 0.5,
  },
  text: {
    fontWeight: '600',
  },
});
