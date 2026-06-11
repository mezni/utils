import React from 'react';
import { View, StyleSheet } from 'react-native';
import { Skeleton } from './Skeleton';

export default {
  title: 'Skeleton',
  component: Skeleton,
};

export function MapSkeleton() {
  return (
    <View style={styles.fullScreen}>
      <Skeleton variant="map" />
    </View>
  );
}

export function ListSkeleton() {
  return (
    <View style={styles.container}>
      <Skeleton variant="list" rows={5} />
    </View>
  );
}

const styles = StyleSheet.create({
  fullScreen: {
    flex: 1,
    height: 400,
    backgroundColor: '#fff',
  },
  container: {
    padding: 16,
    backgroundColor: '#fff',
  },
});
