import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { MapScreen } from '../screens/MapScreen';
import { StationDetailScreen } from '../screens/StationDetailScreen';

export type RootStackParamList = {
  Map: undefined;
  StationDetail: { stationId: string };
};

const Stack = createNativeStackNavigator<RootStackParamList>();

export function AppNavigator() {
  return (
    <Stack.Navigator initialRouteName="Map">
      <Stack.Screen
        name="Map"
        component={MapScreen}
        options={{ headerShown: false }}
      />
      <Stack.Screen
        name="StationDetail"
        component={StationDetailScreen}
        options={{ title: 'Station Detail' }}
      />
    </Stack.Navigator>
  );
}
