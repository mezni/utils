import React, { Component } from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { colors as tokenColors, spacing } from '@bornemap/tokens';
import { Button } from '../Button/Button';

export interface ErrorBoundaryProps {
  fallback?: React.ReactNode;
  onError?: (error: Error, info: React.ErrorInfo) => void;
  children: React.ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    this.props.onError?.(error, info);
  }

  handleRetry = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      const isDark = false;

      return (
        <View
          style={[
            styles.container,
            {
              backgroundColor: isDark ? tokenColors.dark.background : tokenColors.light.background,
            },
          ]}
        >
          <Text
            style={[
              styles.title,
              { color: isDark ? tokenColors.dark.foreground : tokenColors.light.foreground },
            ]}
          >
            Something went wrong
          </Text>
          <Text
            style={[
              styles.message,
              {
                color: isDark
                  ? tokenColors.dark.mutedForeground
                  : tokenColors.light.mutedForeground,
              },
            ]}
          >
            {this.state.error?.message || 'An unexpected error occurred'}
          </Text>
          <Button variant="primary" onPress={this.handleRetry}>
            Try Again
          </Button>
        </View>
      );
    }

    return this.props.children;
  }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: spacing[8],
  },
  title: {
    fontSize: 20,
    fontWeight: '700',
    marginBottom: spacing[2],
  },
  message: {
    fontSize: 14,
    textAlign: 'center',
    marginBottom: spacing[6],
  },
});
