import { View, Text, StyleSheet } from 'react-native'

export function OfflineBanner() {
  return (
    <View style={styles.container}>
      <Text style={styles.text}>
        Viewing cached data. Connect to the internet for real-time status updates.
      </Text>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: '#FEF3C7',
    paddingVertical: 8,
    paddingHorizontal: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#F59E0B',
    zIndex: 10,
  },
  text: {
    fontSize: 13,
    color: '#92400E',
    textAlign: 'center',
  },
})
