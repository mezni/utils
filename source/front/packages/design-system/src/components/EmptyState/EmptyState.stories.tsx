import React from 'react';
import { View, StyleSheet } from 'react-native';
import { EmptyState } from './EmptyState';

export default {
  title: 'EmptyState',
  component: EmptyState,
};

export function NoStations() {
  return (
    <View style={styles.container}>
      <EmptyState
        title="No stations nearby"
        description="Try expanding your search area or check back later"
        ctaLabel="Refresh"
        onCtaPress={() => {}}
      />
    </View>
  );
}

export function NoDescription() {
  return (
    <View style={styles.container}>
      <EmptyState title="GPS unavailable" />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
  },
});
