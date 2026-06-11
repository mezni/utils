import React from 'react';
import { render, fireEvent } from '@testing-library/react-native';
import { EmptyState } from '../../src/components/EmptyState';

describe('EmptyState', () => {
  it('renders title', () => {
    const { getByText } = render(<EmptyState title="No results" />);
    expect(getByText('No results')).toBeTruthy();
  });

  it('renders description when provided', () => {
    const { getByText } = render(
      <EmptyState title="No results" description="Try again later" />,
    );
    expect(getByText('Try again later')).toBeTruthy();
  });

  it('renders CTA button when ctaLabel is provided', () => {
    const { getByText } = render(
      <EmptyState title="No results" ctaLabel="Refresh" onCtaPress={() => {}} />,
    );
    expect(getByText('Refresh')).toBeTruthy();
  });

  it('fires onCtaPress when CTA is tapped', () => {
    const onPress = jest.fn();
    const { getByText } = render(
      <EmptyState title="No results" ctaLabel="Refresh" onCtaPress={onPress} />,
    );
    fireEvent.press(getByText('Refresh'));
    expect(onPress).toHaveBeenCalledTimes(1);
  });
});
