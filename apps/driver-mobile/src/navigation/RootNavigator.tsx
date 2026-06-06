import React from 'react'
import { NavigationContainer } from '@react-navigation/native'
import { createBottomTabNavigator } from '@react-navigation/bottom-tabs'
import { createNativeStackNavigator } from '@react-navigation/native-stack'
import type { RootTabParamList, RootStackParamList } from './types'

import HomeMapScreen from '../screens/HomeMapScreen'
import StationListScreen from '../screens/StationListScreen'
import StationDetailScreen from '../screens/StationDetailScreen'
import SearchScreen from '../screens/SearchScreen'
import FavoritesScreen from '../screens/FavoritesScreen'
import ProfileScreen from '../screens/ProfileScreen'
import LoginRegisterScreen from '../screens/LoginRegisterScreen'

const Tab = createBottomTabNavigator<RootTabParamList>()
const Stack = createNativeStackNavigator<RootStackParamList>()

function MainTabs() {
  return (
    <Tab.Navigator
      screenOptions={{ headerShown: false, tabBarStyle: { display: 'none' } }}
    >
      <Tab.Screen name="HomeMap" component={HomeMapScreen} />
      <Tab.Screen name="StationList" component={StationListScreen} />
      <Tab.Screen name="Search" component={SearchScreen} />
      <Tab.Screen name="Favorites" component={FavoritesScreen} />
      <Tab.Screen name="Profile" component={ProfileScreen} />
    </Tab.Navigator>
  )
}

export default function RootNavigator() {
  return (
    <NavigationContainer>
      <Stack.Navigator screenOptions={{ headerShown: false }}>
        <Stack.Screen name="MainTabs" component={MainTabs} />
        <Stack.Screen name="StationDetail" component={StationDetailScreen} />
        <Stack.Screen name="LoginRegister" component={LoginRegisterScreen} />
      </Stack.Navigator>
    </NavigationContainer>
  )
}
