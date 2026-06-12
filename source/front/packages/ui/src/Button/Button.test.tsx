import { render, fireEvent } from '@testing-library/react-native';
import { Button } from './Button';

describe('Button', () => {
  it('renders children text', () => {
    const { getByText } = render(<Button onPress={() => {}}>Click me</Button>);
    expect(getByText('Click me')).toBeTruthy();
  });

  it('calls onPress when pressed', () => {
    const onPress = jest.fn();
    const { getByText } = render(<Button onPress={onPress}>Click</Button>);
    fireEvent.press(getByText('Click'));
    expect(onPress).toHaveBeenCalledTimes(1);
  });

  it('does not call onPress when disabled', () => {
    const onPress = jest.fn();
    const { getByText } = render(<Button onPress={onPress} disabled>Click</Button>);
    fireEvent.press(getByText('Click'));
    expect(onPress).not.toHaveBeenCalled();
  });

  it('does not call onPress when loading', () => {
    const onPress = jest.fn();
    const { getByText } = render(<Button onPress={onPress} loading>Click</Button>);
    fireEvent.press(getByText('Click'));
    expect(onPress).not.toHaveBeenCalled();
  });

  it('renders all variants', () => {
    const variants = ['primary', 'secondary', 'outline', 'ghost', 'destructive'] as const;
    for (const variant of variants) {
      const { getByText, unmount } = render(
        <Button variant={variant} onPress={() => {}}>{variant}</Button>,
      );
      expect(getByText(variant)).toBeTruthy();
      unmount();
    }
  });

  it('renders all sizes', () => {
    const sizes = ['sm', 'md', 'lg'] as const;
    for (const size of sizes) {
      const { getByText, unmount } = render(
        <Button size={size} onPress={() => {}}>{size}</Button>,
      );
      expect(getByText(size)).toBeTruthy();
      unmount();
    }
  });

  it('applies fullWidth style', () => {
    const { getByText } = render(<Button fullWidth onPress={() => {}}>Full</Button>);
    expect(getByText('Full')).toBeTruthy();
  });
});
