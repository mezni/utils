import React from 'react';
import { View, StyleSheet } from 'react-native';
import { ErrorState } from './ErrorState';

export default {
  title: 'ErrorState',
  component: ErrorState,
};

export function Default() {
  return (
    <View style={styles.container}>
      <ErrorState
        message="Unable to load stations. Please check your connection."
        onRetry={() => {}}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
  },
});
