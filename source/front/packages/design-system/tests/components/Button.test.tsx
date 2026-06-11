import React from 'react';
import { render, fireEvent } from '@testing-library/react-native';
import { Button } from '../../src/components/Button';

describe('Button', () => {
  it('renders label text', () => {
    const { getByText } = render(
      <Button label="Tap me" onPress={() => {}} />,
    );
    expect(getByText('Tap me')).toBeTruthy();
  });

  it('renders primary variant by default', () => {
    const { getByText } = render(
      <Button label="Primary" onPress={() => {}} />,
    );
    expect(getByText('Primary')).toBeTruthy();
  });

  it('fires onPress when tapped', () => {
    const onPress = jest.fn();
    const { getByText } = render(
      <Button label="Press" onPress={onPress} />,
    );
    fireEvent.press(getByText('Press'));
    expect(onPress).toHaveBeenCalledTimes(1);
  });

  it('does not fire onPress when disabled', () => {
    const onPress = jest.fn();
    const { getByText } = render(
      <Button label="Disabled" onPress={onPress} disabled />,
    );
    fireEvent.press(getByText('Disabled'));
    expect(onPress).not.toHaveBeenCalled();
  });

  it('does not fire onPress when loading', () => {
    const onPress = jest.fn();
    const { getByRole } = render(
      <Button label="Loading" onPress={onPress} loading />,
    );
    fireEvent.press(getByRole('button'));
    expect(onPress).not.toHaveBeenCalled();
  });
});
