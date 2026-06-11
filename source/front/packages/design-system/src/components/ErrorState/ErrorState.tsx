import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { useTheme } from '../../tokens/ThemeContext';
import { spacing } from '../../tokens/spacing';
import { typography } from '../../tokens/typography';
import { Button } from '../Button';

export interface ErrorStateProps {
  message: string;
  onRetry: () => void;
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  const { palette } = useTheme();

  return (
    <View style={[styles.container, { backgroundColor: palette.background }]}>
      <Text style={styles.icon}>⚠</Text>
      <Text style={[styles.message, { color: palette.text }]}>{message}</Text>
      <View style={styles.cta}>
        <Button variant="primary" label="Retry" onPress={onRetry} />
      </View>
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
  icon: {
    fontSize: 48,
    marginBottom: spacing.md,
  },
  message: {
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
