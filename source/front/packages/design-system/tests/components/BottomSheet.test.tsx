import React from 'react';
import { render, act } from '@testing-library/react-native';
import { BottomSheet } from '../../src/components/BottomSheet';

describe('BottomSheet', () => {
  it('renders children when open', () => {
    const { getByText } = render(
      <BottomSheet isOpen={true} onClose={() => {}}>
        <Text>Sheet Content</Text>
      </BottomSheet>,
    );
    expect(getByText('Sheet Content')).toBeTruthy();
  });
});
