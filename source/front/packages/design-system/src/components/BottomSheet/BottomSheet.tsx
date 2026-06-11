import React, { useEffect } from 'react';
import {
  View,
  ScrollView,
  StyleSheet,
  Dimensions,
  LayoutChangeEvent,
} from 'react-native';
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withSpring,
  runOnJS,
} from 'react-native-reanimated';
import { GestureDetector, Gesture } from 'react-native-gesture-handler';
import { useTheme } from '../../tokens/ThemeContext';
import { spacing } from '../../tokens/spacing';
import { radii } from '../../tokens/radii';

interface BottomSheetProps {
  isOpen: boolean;
  onClose: () => void;
  snapPoints?: [string | number, string | number];
  children: React.ReactNode;
  disableScrollWhenCollapsed?: boolean;
}

const { height: SCREEN_HEIGHT } = Dimensions.get('window');

function resolveSnap(snap: string | number, containerHeight: number): number {
  if (typeof snap === 'number') return snap;
  if (snap.endsWith('%')) {
    return (parseFloat(snap) / 100) * (containerHeight || SCREEN_HEIGHT);
  }
  return SCREEN_HEIGHT - parseFloat(snap);
}

export function BottomSheet({
  isOpen,
  onClose,
  snapPoints = ['60%', '85%'],
  children,
  disableScrollWhenCollapsed = true,
}: BottomSheetProps) {
  const { palette } = useTheme();
  const translateY = useSharedValue(SCREEN_HEIGHT);
  const [containerHeight, setContainerHeight] = React.useState(SCREEN_HEIGHT);

  const resolvedSnaps = React.useMemo(
    () => snapPoints.map((s) => resolveSnap(s, containerHeight)),
    [snapPoints, containerHeight],
  );

  const topOffset = containerHeight - resolvedSnaps[0];
  const dismissThreshold = resolvedSnaps[0] * 0.3;

  useEffect(() => {
    if (isOpen) {
      translateY.value = withSpring(topOffset, {
        damping: 20,
        stiffness: 200,
      });
    } else {
      translateY.value = withSpring(SCREEN_HEIGHT, { damping: 20, stiffness: 200 });
    }
  }, [isOpen, topOffset, translateY]);

  const panGesture = Gesture.Pan()
    .onUpdate((event) => {
      const newTranslate = topOffset + event.translationY;
      translateY.value = Math.max(newTranslate, 0);
    })
    .onEnd((event) => {
      if (event.translationY > dismissThreshold) {
        runOnJS(onClose)();
      } else {
        translateY.value = withSpring(topOffset, {
          damping: 20,
          stiffness: 200,
        });
      }
    });

  const animatedStyle = useAnimatedStyle(() => ({
    transform: [{ translateY: translateY.value }],
  }));

  const handleLayout = (e: LayoutChangeEvent) => {
    setContainerHeight(e.nativeEvent.layout.height);
  };

  return (
    <View
      style={[StyleSheet.absoluteFill, styles.overlay]}
      pointerEvents={isOpen ? 'auto' : 'none'}
      onLayout={handleLayout}
    >
      <GestureDetector gesture={panGesture}>
        <Animated.View
          style={[
            styles.sheet,
            {
              height: resolvedSnaps[1],
              backgroundColor: palette.surface,
              borderTopLeftRadius: radii.xl,
              borderTopRightRadius: radii.xl,
            },
            animatedStyle,
          ]}
        >
          <View style={styles.handle}>
            <View
              style={[
                styles.handleBar,
                { backgroundColor: palette.border },
              ]}
            />
          </View>
          <ScrollView
            style={styles.content}
            scrollEnabled={!disableScrollWhenCollapsed}
            bounces={false}
          >
            {children}
          </ScrollView>
        </Animated.View>
      </GestureDetector>
    </View>
  );
}

const styles = StyleSheet.create({
  overlay: {
    justifyContent: 'flex-end',
  },
  sheet: {
    position: 'absolute',
    left: 0,
    right: 0,
    bottom: 0,
    paddingBottom: spacing.xl,
  },
  handle: {
    alignItems: 'center',
    paddingVertical: spacing.sm,
  },
  handleBar: {
    width: 40,
    height: 4,
    borderRadius: radii.full,
  },
  content: {
    flex: 1,
    paddingHorizontal: spacing.md,
  },
});
