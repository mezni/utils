import React from 'react';
import { render, fireEvent } from '@testing-library/react-native';
import { ErrorState } from '../../src/components/ErrorState';

describe('ErrorState', () => {
  it('renders error message', () => {
    const { getByText } = render(
      <ErrorState message="Something went wrong" onRetry={() => {}} />,
    );
    expect(getByText('Something went wrong')).toBeTruthy();
  });

  it('renders retry button', () => {
    const { getByText } = render(
      <ErrorState message="Error" onRetry={() => {}} />,
    );
    expect(getByText('Retry')).toBeTruthy();
  });

  it('fires onRetry when retry button is tapped', () => {
    const onRetry = jest.fn();
    const { getByText } = render(
      <ErrorState message="Error" onRetry={onRetry} />,
    );
    fireEvent.press(getByText('Retry'));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });
});
