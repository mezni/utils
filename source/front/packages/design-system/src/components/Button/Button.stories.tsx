import React from 'react';
import { View, StyleSheet } from 'react-native';
import { Button } from './Button';

export default {
  title: 'Button',
  component: Button,
};

export function Primary() {
  return (
    <View style={styles.container}>
      <Button variant="primary" label="Find Stations" onPress={() => {}} />
    </View>
  );
}

export function Secondary() {
  return (
    <View style={styles.container}>
      <Button variant="secondary" label="Cancel" onPress={() => {}} />
    </View>
  );
}

export function Ghost() {
  return (
    <View style={styles.container}>
      <Button variant="ghost" label="Skip" onPress={() => {}} />
    </View>
  );
}

export function Disabled() {
  return (
    <View style={styles.container}>
      <Button variant="primary" label="Disabled" disabled onPress={() => {}} />
    </View>
  );
}

export function Loading() {
  return (
    <View style={styles.container}>
      <Button variant="primary" label="Loading..." loading onPress={() => {}} />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 16,
    backgroundColor: '#fff',
  },
});
