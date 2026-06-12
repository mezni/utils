import { View, Text, ActivityIndicator, TouchableOpacity, StyleSheet } from 'react-native';
import { useTheme } from '../ThemeProvider/ThemeProvider';
import { colors as tokenColors, spacing, radii } from '@bornemap/tokens';

export interface LoadingOverlayProps {
  visible: boolean;
  message?: string;
  cancelable?: boolean;
  onCancel?: () => void;
}

export function LoadingOverlay({
  visible,
  message,
  cancelable = false,
  onCancel,
}: LoadingOverlayProps) {
  const { isDark } = useTheme();
  const theme = isDark ? tokenColors.dark : tokenColors.light;

  if (!visible) return null;

  return (
    <View
      style={[
        styles.overlay,
        { backgroundColor: isDark ? 'rgba(0,0,0,0.7)' : 'rgba(0,0,0,0.5)' },
      ]}
    >
      <View
        style={[
          styles.content,
          {
            backgroundColor: theme.card,
            borderRadius: radii.lg,
          },
        ]}
      >
        <ActivityIndicator size="large" color={theme.primary} />
        {message && (
          <Text style={[styles.message, { color: theme.foreground }]}>
            {message}
          </Text>
        )}
        {cancelable && onCancel && (
          <TouchableOpacity onPress={onCancel} style={styles.cancelButton}>
            <Text style={{ color: theme.primary, fontWeight: '600' }}>Cancel</Text>
          </TouchableOpacity>
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  overlay: {
    ...StyleSheet.absoluteFillObject,
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 1000,
  },
  content: {
    alignItems: 'center',
    padding: spacing[6],
    minWidth: 200,
  },
  message: {
    marginTop: spacing[4],
    fontSize: 16,
    textAlign: 'center',
  },
  cancelButton: {
    marginTop: spacing[4],
    paddingVertical: spacing[2],
    paddingHorizontal: spacing[4],
  },
});
