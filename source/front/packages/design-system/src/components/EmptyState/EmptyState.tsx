import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { useTheme } from '../../tokens/ThemeContext';
import { spacing } from '../../tokens/spacing';
import { typography } from '../../tokens/typography';
import { Button } from '../Button';

export interface EmptyStateProps {
  title: string;
  description?: string;
  illustration?: React.ReactNode;
  ctaLabel?: string;
  onCtaPress?: () => void;
}

export function EmptyState({
  title,
  description,
  illustration,
  ctaLabel,
  onCtaPress,
}: EmptyStateProps) {
  const { palette } = useTheme();

  return (
    <View style={[styles.container, { backgroundColor: palette.background }]}>
      {illustration && <View style={styles.illustration}>{illustration}</View>}
      <Text style={[styles.title, { color: palette.text }]}>{title}</Text>
      {description && (
        <Text style={[styles.description, { color: palette.textSecondary }]}>
          {description}
        </Text>
      )}
      {ctaLabel && onCtaPress && (
        <View style={styles.cta}>
          <Button variant="primary" label={ctaLabel} onPress={onCtaPress} />
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: spacing.xl,
  },
  illustration: {
    marginBottom: spacing.lg,
  },
  title: {
    fontSize: typography.fontSize.title,
    fontFamily: typography.fontFamily.bold,
    fontWeight: typography.fontWeight.bold,
    textAlign: 'center',
    marginBottom: spacing.sm,
  },
  description: {
    fontSize: typography.fontSize.body,
    fontFamily: typography.fontFamily.regular,
    fontWeight: typography.fontWeight.regular,
    textAlign: 'center',
    lineHeight: typography.fontSize.body * typography.lineHeight.relaxed,
    marginBottom: spacing.lg,
  },
  cta: {
    marginTop: spacing.md,
  },
});
