import { StatusBar } from 'expo-status-bar';
import { NavigationContainer } from '@react-navigation/native';
import { createNativeStackNavigator } from '@react-navigation/native-stack';
import MapScreen from './src/screens/MapScreen';
import StationDetailScreen from './src/screens/StationDetailScreen';

const Stack = createNativeStackNavigator();

export default function App() {
  return (
    <NavigationContainer>
      <Stack.Navigator initialRouteName="Map">
        <Stack.Screen
          name="Map"
          component={MapScreen}
          options={{ title: 'EV Chargers', headerShown: false }}
        />
        <Stack.Screen
          name="StationDetail"
          component={StationDetailScreen}
          options={{ title: 'Station Details' }}
        />
      </Stack.Navigator>
      <StatusBar style="auto" />
    </NavigationContainer>
  );
}
