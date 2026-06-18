import { View, Text, StyleSheet } from 'react-native'

export function MacroZoomOverlay() {
  return (
    <View style={styles.container}>
      <Text style={styles.text}>
        Zoom in closer to view available charging stations.
      </Text>
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
    justifyContent: 'center',
    alignItems: 'center',
    padding: 32,
    zIndex: 20,
  },
  text: {
    fontSize: 18,
    fontWeight: '600',
    color: '#555',
    textAlign: 'center',
    lineHeight: 26,
  },
})
