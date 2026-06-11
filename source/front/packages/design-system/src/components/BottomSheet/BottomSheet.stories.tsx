import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { BottomSheet } from './BottomSheet';

export default {
  title: 'BottomSheet',
  component: BottomSheet,
};

export function Default() {
  return (
    <View style={styles.container}>
      <BottomSheet isOpen={true} onClose={() => {}}>
        <Text>Station details go here</Text>
      </BottomSheet>
    </View>
  );
}

export function WithScrollableContent() {
  return (
    <View style={styles.container}>
      <BottomSheet isOpen={true} onClose={() => {}} snapPoints={['60%', '85%']}>
        {Array.from({ length: 20 }).map((_, i) => (
          <Text key={i} style={styles.listItem}>
            List item {i + 1}
          </Text>
        ))}
      </BottomSheet>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#ccc',
  },
  listItem: {
    paddingVertical: 12,
    fontSize: 16,
  },
});
