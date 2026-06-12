import { useEffect, useRef } from 'react';
import { View, Animated, ViewStyle, DimensionValue } from 'react-native';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import { colors as tokenColors, spacing, radii } from '@bornemap/tokens';

export type SkeletonShape = 'rectangular' | 'circular' | 'text';

export interface SkeletonProps {
  shape?: SkeletonShape;
  width?: number | string;
  height?: number | string;
  lines?: number;
  className?: string;
  style?: ViewStyle;
}

export function Skeleton({
  shape = 'text',
  width,
  height,
  lines = 3,
  style,
}: SkeletonProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;
  const pulseAnim = useRef(new Animated.Value(0)).current;

  useEffect(() => {
    const animation = Animated.loop(
      Animated.sequence([
        Animated.timing(pulseAnim, {
          toValue: 1,
          duration: 1000,
          useNativeDriver: true,
        }),
        Animated.timing(pulseAnim, {
          toValue: 0,
          duration: 1000,
          useNativeDriver: true,
        }),
      ]),
    );
    animation.start();
    return () => animation.stop();
  }, [pulseAnim]);

  const opacity = pulseAnim.interpolate({
    inputRange: [0, 1],
    outputRange: [0.3, 0.7],
  });

  const skeletonColor = theme.muted;

  if (shape === 'text') {
    return (
      <View style={style}>
        {Array.from({ length: lines }, (_, i) => (
          <Animated.View
            key={i}
            style={{
              height: 12,
              width: (i === lines - 1 ? '60%' : '100%') as DimensionValue,
              backgroundColor: skeletonColor,
              borderRadius: radii.sm,
              marginBottom: spacing[2],
              opacity,
            }}
          />
        ))}
      </View>
    );
  }

  const shapeStyle: ViewStyle = {};
  if (width) shapeStyle.width = width as DimensionValue;
  if (height) shapeStyle.height = height as DimensionValue;

  if (shape === 'circular') {
    const size = (typeof width === 'number' ? width : 48) as DimensionValue;
    shapeStyle.width = size;
    shapeStyle.height = size;
    shapeStyle.borderRadius = typeof size === 'number' ? size / 2 : 24;
  } else {
    shapeStyle.height = (height || 200) as DimensionValue;
    shapeStyle.borderRadius = radii.md;
  }

  return (
    <Animated.View
      style={[
        {
          backgroundColor: skeletonColor,
          opacity,
        },
        shapeStyle,
        style,
      ]}
    />
  );
}
