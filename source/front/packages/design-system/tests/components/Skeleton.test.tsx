import React from 'react';
import { render } from '@testing-library/react-native';
import { Skeleton } from '../../src/components/Skeleton';

describe('Skeleton', () => {
  it('renders map variant', () => {
    const { getByTestId } = render(<Skeleton variant="map" />);
    expect(getByTestId('skeleton-map')).toBeTruthy();
  });

  it('renders list variant with default rows', () => {
    const { getAllByTestId } = render(<Skeleton variant="list" />);
    const rows = getAllByTestId('skeleton-list-row');
    expect(rows).toHaveLength(3);
  });

  it('renders list variant with custom row count', () => {
    const { getAllByTestId } = render(<Skeleton variant="list" rows={5} />);
    const rows = getAllByTestId('skeleton-list-row');
    expect(rows).toHaveLength(5);
  });
});
