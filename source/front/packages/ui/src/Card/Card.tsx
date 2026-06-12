import React from 'react';
import { View, TouchableOpacity, StyleSheet, ViewStyle } from 'react-native';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import { colors as tokenColors, spacing, radii } from '@bornemap/tokens';

export type CardVariant = 'default' | 'elevated' | 'interactive';

export interface CardProps {
  variant?: CardVariant;
  header?: React.ReactNode;
  footer?: React.ReactNode;
  onPress?: () => void;
  children: React.ReactNode;
  className?: string;
  style?: ViewStyle;
}

export function Card({
  variant = 'default',
  header,
  footer,
  onPress,
  children,
  style,
}: CardProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;

  const cardVariantStyle = getCardVariantStyle(variant, theme);

  const cardContent = (
    <>
      {header && (
        <View style={[styles.header, { borderBottomColor: theme.border }]}>
          {header}
        </View>
      )}
      <View style={styles.body}>{children}</View>
      {footer && (
        <View style={[styles.footer, { borderTopColor: theme.border }]}>
          {footer}
        </View>
      )}
    </>
  );

  const cardStyle = [
    styles.base,
    {
      backgroundColor: theme.card,
      borderRadius: radii.lg,
      ...cardVariantStyle,
    },
    style,
  ];

  if (variant === 'interactive' && onPress) {
    return (
      <TouchableOpacity onPress={onPress} activeOpacity={0.7} style={cardStyle}>
        {cardContent}
      </TouchableOpacity>
    );
  }

  return <View style={cardStyle}>{cardContent}</View>;
}

function getCardVariantStyle(variant: CardVariant, theme: typeof tokenColors.light): ViewStyle {
  switch (variant) {
    case 'default':
      return {
        borderWidth: 1,
        borderColor: theme.border,
      };
    case 'elevated':
      return {
        shadowColor: '#000',
        shadowOffset: { width: 0, height: 4 },
        shadowOpacity: 0.1,
        shadowRadius: 6,
        elevation: 4,
      };
    case 'interactive':
      return {
        borderWidth: 1,
        borderColor: theme.border,
      };
  }
}

const styles = StyleSheet.create({
  base: {
    overflow: 'hidden',
  },
  header: {
    padding: spacing[4],
    borderBottomWidth: 1,
  },
  body: {
    padding: spacing[4],
  },
  footer: {
    padding: spacing[4],
    borderTopWidth: 1,
  },
});
