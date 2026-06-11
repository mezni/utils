import React, { useCallback } from 'react';
import {
  Pressable,
  Text,
  ActivityIndicator,
  StyleSheet,
  ViewStyle,
} from 'react-native';
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withSpring,
} from 'react-native-reanimated';
import * as Haptics from 'expo-haptics';
import { spacing } from '../../tokens/spacing';
import { typography } from '../../tokens/typography';
import { radii } from '../../tokens/radii';
import { useTheme } from '../../tokens/ThemeContext';

type ButtonVariant = 'primary' | 'secondary' | 'ghost';

export interface ButtonProps {
  variant?: ButtonVariant;
  label: string;
  onPress: () => void;
  disabled?: boolean;
  loading?: boolean;
}

const AnimatedPressable = Animated.createAnimatedComponent(Pressable);

export function Button({
  variant = 'primary',
  label,
  onPress,
  disabled = false,
  loading = false,
}: ButtonProps) {
  const scale = useSharedValue(1);
  const { palette } = useTheme();

  const animatedStyle = useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
  }));

  const handlePressIn = useCallback(() => {
    if (disabled || loading) return;
    scale.value = withSpring(0.97, { damping: 15, stiffness: 200 });
    Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);
  }, [disabled, loading, scale]);

  const handlePressOut = useCallback(() => {
    scale.value = withSpring(1, { damping: 15, stiffness: 200 });
  }, [scale]);

  const isDimmed = disabled || loading;

  const variantStyle: ViewStyle = {
    ...(variant === 'primary' && { backgroundColor: palette.primary }),
    ...(variant === 'secondary' && {
      backgroundColor: palette.surface,
      borderWidth: 1,
      borderColor: palette.border,
    }),
    ...(variant === 'ghost' && { backgroundColor: 'transparent' }),
  };

  const labelColor =
    variant === 'primary' ? '#FFFFFF'
    : variant === 'secondary' ? palette.text
    : palette.primary;

  return (
    <AnimatedPressable
      onPress={isDimmed ? undefined : onPress}
      onPressIn={handlePressIn}
      onPressOut={handlePressOut}
      disabled={disabled || loading}
      style={[
        animatedStyle,
        styles.base,
        variantStyle,
        isDimmed && styles.dimmed,
      ]}
    >
      {loading ? (
        <ActivityIndicator color={labelColor} />
      ) : (
        <Text style={[styles.label, { color: labelColor }]}>{label}</Text>
      )}
    </AnimatedPressable>
  );
}

const styles = StyleSheet.create({
  base: {
    paddingVertical: spacing.sm,
    paddingHorizontal: spacing.lg,
    borderRadius: radii.md,
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: 44,
  },
  dimmed: {
    opacity: 0.4,
  },
  label: {
    fontSize: typography.fontSize.body,
    fontFamily: typography.fontFamily.medium,
    fontWeight: typography.fontWeight.semibold,
  },
});
