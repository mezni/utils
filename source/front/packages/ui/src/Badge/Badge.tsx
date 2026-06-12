import React from 'react';
import { View, Text, StyleSheet, ViewStyle } from 'react-native';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import { colors as tokenColors, spacing, radii } from '@bornemap/tokens';

export type BadgeVariant = 'default' | 'success' | 'warning' | 'error' | 'info';
export type BadgeSize = 'sm' | 'md' | 'lg';

export interface BadgeProps {
  variant?: BadgeVariant;
  size?: BadgeSize;
  children: React.ReactNode;
  className?: string;
  style?: ViewStyle;
}

export function Badge({
  variant = 'default',
  size = 'md',
  children,
  style,
}: BadgeProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;

  const variantColors = getBadgeVariantColors(variant, theme);
  const sizeStyles = getBadgeSizeStyles(size);

  return (
    <View
      style={[
        styles.base,
        {
          backgroundColor: variantColors.background,
          borderRadius: radii.full,
        },
        sizeStyles.container,
        style,
      ]}
    >
      <Text
        style={[
          styles.text,
          { color: variantColors.text },
          sizeStyles.text,
        ]}
      >
        {children}
      </Text>
    </View>
  );
}

function getBadgeVariantColors(variant: BadgeVariant, theme: typeof tokenColors.light) {
  switch (variant) {
    case 'default':
      return { background: theme.muted, text: theme.foreground };
    case 'success':
      return { background: theme.success + '20', text: theme.success };
    case 'warning':
      return { background: theme.warning + '20', text: theme.warning };
    case 'error':
      return { background: theme.destructive + '20', text: theme.destructive };
    case 'info':
      return { background: theme.info + '20', text: theme.info };
  }
}

function getBadgeSizeStyles(size: BadgeSize) {
  switch (size) {
    case 'sm':
      return {
        container: { paddingVertical: 2, paddingHorizontal: spacing[2] } as ViewStyle,
        text: { fontSize: 11 } as any,
      };
    case 'md':
      return {
        container: { paddingVertical: 4, paddingHorizontal: spacing[3] } as ViewStyle,
        text: { fontSize: 13 } as any,
      };
    case 'lg':
      return {
        container: { paddingVertical: 6, paddingHorizontal: spacing[4] } as ViewStyle,
        text: { fontSize: 15 } as any,
      };
  }
}

const styles = StyleSheet.create({
  base: {
    alignSelf: 'flex-start',
  },
  text: {
    fontWeight: '600',
  },
});
