import React from 'react';
import { View, Text, StyleSheet, ViewStyle } from 'react-native';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import { colors as tokenColors, spacing } from '@bornemap/tokens';
import { Button } from '../Button/Button';

export interface EmptyStateProps {
  icon?: React.ReactNode;
  title: string;
  description?: string;
  action?: {
    label: string;
    onPress: () => void;
  };
  className?: string;
  style?: ViewStyle;
}

export function EmptyState({
  icon,
  title,
  description,
  action,
  style,
}: EmptyStateProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;

  return (
    <View style={[styles.container, style]}>
      {icon && <View style={styles.icon}>{icon}</View>}
      <Text
        style={[
          styles.title,
          { color: theme.foreground },
        ]}
      >
        {title}
      </Text>
      {description && (
        <Text
          style={[
            styles.description,
            { color: theme.mutedForeground },
          ]}
        >
          {description}
        </Text>
      )}
      {action && (
        <View style={styles.action}>
          <Button variant="primary" onPress={action.onPress}>
            {action.label}
          </Button>
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    alignItems: 'center',
    justifyContent: 'center',
    padding: spacing[8],
  },
  icon: {
    marginBottom: spacing[4],
  },
  title: {
    fontSize: 18,
    fontWeight: '600',
    textAlign: 'center',
    marginBottom: spacing[2],
  },
  description: {
    fontSize: 14,
    textAlign: 'center',
    lineHeight: 20,
    marginBottom: spacing[4],
  },
  action: {
    marginTop: spacing[2],
  },
});
