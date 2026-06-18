import { useEffect, useRef } from 'react'
import { View, Animated, StyleSheet } from 'react-native'

export function ShimmerSkeleton() {
  const opacity = useRef(new Animated.Value(0.3))

  useEffect(() => {
    const animation = Animated.loop(
      Animated.sequence([
        Animated.timing(opacity.current, {
          toValue: 1,
          duration: 800,
          useNativeDriver: true,
        }),
        Animated.timing(opacity.current, {
          toValue: 0.3,
          duration: 800,
          useNativeDriver: true,
        }),
      ]),
    )
    animation.start()
    return () => animation.stop()
  }, [])

  return (
    <View style={styles.container}>
      <View style={styles.row}>
        <Animated.View style={[styles.circle, { opacity: opacity.current }]} />
        <View style={styles.textBlock}>
          <Animated.View style={[styles.line, { width: '60%', opacity: opacity.current }]} />
          <Animated.View style={[styles.line, { width: '40%', opacity: opacity.current }]} />
        </View>
      </View>
      <View style={styles.row}>
        <Animated.View style={[styles.circle, { opacity: opacity.current }]} />
        <View style={styles.textBlock}>
          <Animated.View style={[styles.line, { width: '50%', opacity: opacity.current }]} />
          <Animated.View style={[styles.line, { width: '35%', opacity: opacity.current }]} />
        </View>
      </View>
      <View style={styles.row}>
        <Animated.View style={[styles.circle, { opacity: opacity.current }]} />
        <View style={styles.textBlock}>
          <Animated.View style={[styles.line, { width: '55%', opacity: opacity.current }]} />
          <Animated.View style={[styles.line, { width: '30%', opacity: opacity.current }]} />
        </View>
      </View>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: 'rgba(255, 255, 255, 0.85)',
    padding: 16,
    justifyContent: 'center',
  },
  row: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 16,
  },
  circle: {
    width: 40,
    height: 40,
    borderRadius: 20,
    backgroundColor: '#E0E0E0',
    marginRight: 12,
  },
  textBlock: {
    flex: 1,
  },
  line: {
    height: 12,
    borderRadius: 6,
    backgroundColor: '#E0E0E0',
    marginBottom: 6,
  },
})
