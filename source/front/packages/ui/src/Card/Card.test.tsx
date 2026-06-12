import { render, fireEvent } from '@testing-library/react-native';
import { Text } from 'react-native';
import { Card } from './Card';

describe('Card', () => {
  it('renders children', () => {
    const { getByText } = render(<Card><Text>Content</Text></Card>);
    expect(getByText('Content')).toBeTruthy();
  });

  it('renders header and footer', () => {
    const { getByText } = render(
      <Card header={<Text>Header</Text>} footer={<Text>Footer</Text>}>
        <Text>Body</Text>
      </Card>,
    );
    expect(getByText('Header')).toBeTruthy();
    expect(getByText('Body')).toBeTruthy();
    expect(getByText('Footer')).toBeTruthy();
  });

  it('calls onPress for interactive variant', () => {
    const onPress = jest.fn();
    const { getByText } = render(
      <Card variant="interactive" onPress={onPress}>
        <Text>Pressable</Text>
      </Card>,
    );
    fireEvent.press(getByText('Pressable'));
    expect(onPress).toHaveBeenCalledTimes(1);
  });

  it('renders all variants', () => {
    const variants = ['default', 'elevated', 'interactive'] as const;
    for (const variant of variants) {
      const { getByText, unmount } = render(
        <Card variant={variant}><Text>{variant}</Text></Card>,
      );
      expect(getByText(variant)).toBeTruthy();
      unmount();
    }
  });
});
