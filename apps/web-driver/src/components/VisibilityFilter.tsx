import React from 'react';
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native';
import type { Station } from '@bornemap/shared-types';

interface VisibilityFilterProps {
  selectedVisibility: string;
  onSelectVisibility: (visibility: string) => void;
  stations: Station[];
}

const visibilityOptions = [
  { value: 'all', label: 'All', icon: '🌐' },
  { value: 'commercial', label: 'Commercial', icon: '🏢' },
  { value: 'private_home', label: 'Private', icon: '🏠' },
];

export const VisibilityFilter: React.FC<VisibilityFilterProps> = ({
  selectedVisibility,
  onSelectVisibility,
  stations,
}) => {
  const stats = calculateVisibilityStats(stations);

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Visibility Filter</Text>
      <View style={styles.optionsContainer}>
        {visibilityOptions.map((option) => {
          const isSelected = selectedVisibility === option.value;
          const isAvailable = stats[option.value as keyof typeof stats] > 0;

          return (
            <TouchableOpacity
              key={option.value}
              style={[
                styles.option,
                isSelected && styles.optionSelected,
                !isAvailable && styles.optionDisabled,
              ]}
              onPress={() => isAvailable && onSelectVisibility(option.value)}
              disabled={!isAvailable}
            >
              <Text style={styles.optionIcon}>{option.icon}</Text>
              <Text style={[
                styles.optionLabel,
                isSelected && styles.optionLabelSelected,
              ]}>
                {option.label}
              </Text>
              <Text style={styles.optionCount}>
                {stats[option.value as keyof typeof stats] || 0}
              </Text>
            </TouchableOpacity>
          );
        })}
      </View>
    </View>
  );
}

function calculateVisibilityStats(stations: Station[]) {
  const stats = {
    all: stations.length,
    commercial: 0,
    private_home: 0,
  };

  stations.forEach((station) => {
    if (stats[station.visibility as keyof typeof stats] !== undefined) {
      stats[station.visibility as keyof typeof stats] =
        (stats[station.visibility as keyof typeof stats] || 0) + 1;
    }
  });

  return stats;
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: 'white',
    padding: 12,
    borderRadius: 8,
    marginBottom: 10,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.25,
    shadowRadius: 3.84,
    elevation: 5,
  },
  title: {
    fontSize: 14,
    fontWeight: 'bold',
    color: '#333',
    marginBottom: 8,
  },
  optionsContainer: {
    flexDirection: 'row',
    justifyContent: 'space-between',
  },
  option: {
    flex: 1,
    backgroundColor: '#f5f5f5',
    padding: 12,
    borderRadius: 6,
    alignItems: 'center',
    marginHorizontal: 4,
  },
  optionSelected: {
    backgroundColor: '#4CAF50',
    padding: 12,
    borderRadius: 6,
    alignItems: 'center',
    marginHorizontal: 4,
  },
  optionDisabled: {
    opacity: 0.5,
  },
  optionIcon: {
    fontSize: 20,
    marginBottom: 4,
  },
  optionLabel: {
    fontSize: 11,
    fontWeight: '600',
    color: '#333',
  },
  optionLabelSelected: {
    color: 'white',
  },
  optionCount: {
    fontSize: 12,
    fontWeight: 'bold',
    color: '#333',
  },
});
