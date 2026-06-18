import { View, Text, StyleSheet } from 'react-native'

export function EmptyState() {
  return (
    <View style={styles.container}>
      <Text style={styles.icon}>🔌</Text>
      <Text style={styles.title}>No stations nearby</Text>
      <Text style={styles.body}>
        There are no charging stations in this area. Try panning towards a major city:
      </Text>
      <Text style={styles.cities}>Tunis · Sousse · Sfax</Text>
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
    backgroundColor: 'rgba(255, 255, 255, 0.9)',
    justifyContent: 'center',
    alignItems: 'center',
    padding: 32,
  },
  icon: {
    fontSize: 48,
    marginBottom: 16,
  },
  title: {
    fontSize: 20,
    fontWeight: '600',
    marginBottom: 8,
    color: '#333',
  },
  body: {
    fontSize: 14,
    color: '#666',
    textAlign: 'center',
    marginBottom: 12,
    lineHeight: 20,
  },
  cities: {
    fontSize: 16,
    fontWeight: '600',
    color: '#2563EB',
  },
})
