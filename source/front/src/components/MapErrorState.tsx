import { View, StyleSheet } from 'react-native';
import { ErrorState, EmptyState } from '@borne/design-system';

interface MapErrorStateProps {
  error: string | null;
  isEmpty: boolean;
  loading: boolean;
  onRetry: () => void;
}

export function MapErrorState({
  error,
  isEmpty,
  loading,
  onRetry,
}: MapErrorStateProps) {
  if (loading || (!error && !isEmpty)) return null;

  if (error) {
    return (
      <View style={styles.overlay} pointerEvents="box-none">
        <ErrorState message={error} onRetry={onRetry} />
      </View>
    );
  }

  if (isEmpty) {
    return (
      <View style={styles.overlay} pointerEvents="box-none">
        <EmptyState title="No stations nearby" />
      </View>
    );
  }

  return null;
}

const styles = StyleSheet.create({
  overlay: {
    ...StyleSheet.absoluteFillObject,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: 'rgba(0,0,0,0.3)',
  },
});
