import React, { useCallback, useState } from 'react'
import { View, Text, ActivityIndicator, StyleSheet } from 'react-native'
import { SafeAreaProvider } from 'react-native-safe-area-context'
import { useFonts, PlusJakartaSans_400Regular, PlusJakartaSans_600SemiBold, PlusJakartaSans_700Bold } from '@expo-google-fonts/plus-jakarta-sans'
import { brandPrimary, brandLight } from '@borne-map/ui/src/tokens/native'
import RootNavigator from './navigation/RootNavigator'
import { FavoritesProvider } from './hooks/useFavorites'
import './i18n'

export default function App() {
  const [fontsLoaded] = useFonts({
    PlusJakartaSans_400Regular,
    PlusJakartaSans_600SemiBold,
    PlusJakartaSans_700Bold,
  })

  if (!fontsLoaded) {
    return (
      <View style={styles.loading}>
        <ActivityIndicator size="large" color={brandPrimary} />
      </View>
    )
  }

  return (
    <SafeAreaProvider>
      <FavoritesProvider>
        <RootNavigator />
      </FavoritesProvider>
    </SafeAreaProvider>
  )
}

const styles = StyleSheet.create({
  loading: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: brandLight,
  },
})
