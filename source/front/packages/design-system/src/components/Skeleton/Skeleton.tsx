import React, { useEffect } from 'react';
import { View, StyleSheet } from 'react-native';
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withRepeat,
  withTiming,
  Easing,
} from 'react-native-reanimated';
import { useTheme } from '../../tokens/ThemeContext';
import { spacing } from '../../tokens/spacing';
import { radii } from '../../tokens/radii';

type SkeletonVariant = 'map' | 'list';

interface SkeletonProps {
  variant: SkeletonVariant;
  rows?: number;
  width?: number | string;
  height?: number | string;
}

export function Skeleton({
  variant,
  rows = 3,
  width = '100%',
  height = '100%',
}: SkeletonProps) {
  const { palette } = useTheme();
  const opacity = useSharedValue(1);

  useEffect(() => {
    opacity.value = withRepeat(
      withTiming(0.3, { duration: 1000, easing: Easing.inOut(Easing.ease) }),
      -1,
      true,
    );
  }, [opacity]);

  const shimmerStyle = useAnimatedStyle(() => ({
    opacity: opacity.value,
  }));

  if (variant === 'map') {
    return (
      <View style={{ width, height }} testID="skeleton-map">
        <Animated.View
          style={[
            styles.mapBlock,
            { backgroundColor: palette.skeleton },
            shimmerStyle,
          ]}
        />
      </View>
    );
  }

  return (
    <View style={{ width, height }} testID="skeleton-list">
      {Array.from({ length: rows }).map((_, i) => (
        <Animated.View
          key={i}
          testID="skeleton-list-row"
          style={[
            styles.listRow,
            { backgroundColor: palette.skeleton },
            shimmerStyle,
          ]}
        >
          <View
            style={[
              styles.avatar,
              { backgroundColor: palette.skeletonHighlight },
            ]}
          />
          <View style={styles.textBlock}>
            <View
              style={[
                styles.line,
                styles.lineShort,
                { backgroundColor: palette.skeletonHighlight },
              ]}
            />
            <View
              style={[
                styles.line,
                styles.lineLong,
                { backgroundColor: palette.skeletonHighlight },
              ]}
            />
          </View>
        </Animated.View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  mapBlock: {
    flex: 1,
    borderRadius: radii.lg,
  },
  listRow: {
    flexDirection: 'row',
    alignItems: 'center',
    padding: spacing.md,
    marginBottom: spacing.sm,
    borderRadius: radii.md,
  },
  avatar: {
    width: 40,
    height: 40,
    borderRadius: 20,
    marginRight: spacing.sm,
  },
  textBlock: {
    flex: 1,
  },
  line: {
    height: 12,
    borderRadius: radii.sm,
    marginBottom: spacing.xxs,
  },
  lineShort: {
    width: '60%',
  },
  lineLong: {
    width: '100%',
  },
});
