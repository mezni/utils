import React from 'react';
import { Platform } from 'react-native';
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';
import { StatusBar } from 'expo-status-bar';
import { AppProvider } from './src/context/AppContext';
import NavigationProvider from './src/context/NavigationProvider';
import MapPortal from './src/components/MapPortal';
import MapScreen from './src/screens/MapScreen';
import NavBar from './src/components/NavBar';

export default function App() {
  return (
    <SafeAreaProvider>
      <AppProvider>
        <NavigationProvider>
          <StatusBar style="dark" />
          {Platform.OS === 'web' ? <DesktopApp /> : <MobileApp />}
        </NavigationProvider>
      </AppProvider>
    </SafeAreaProvider>
  );
}

function DesktopApp() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', width: '100vw' }}>
      <NavBar />
      <div style={{ flex: 1, position: 'relative' }}>
        <MapPortal />
      </div>
    </div>
  );
}

function MobileApp() {
  return (
    <SafeAreaView style={{ flex: 1, backgroundColor: '#FFFFFF' }}>
      <MapScreen />
    </SafeAreaView>
  );
}
