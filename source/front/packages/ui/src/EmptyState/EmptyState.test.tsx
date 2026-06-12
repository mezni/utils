import { render, fireEvent } from '@testing-library/react-native';
import { Text } from 'react-native';
import { EmptyState } from './EmptyState';

describe('EmptyState', () => {
  it('renders title and description', () => {
    const { getByText } = render(
      <EmptyState title="No data" description="Nothing to show here" />,
    );
    expect(getByText('No data')).toBeTruthy();
    expect(getByText('Nothing to show here')).toBeTruthy();
  });

  it('renders action button and handles press', () => {
    const onPress = jest.fn();
    const { getByText } = render(
      <EmptyState
        title="Empty"
        action={{ label: 'Retry', onPress }}
      />,
    );
    fireEvent.press(getByText('Retry'));
    expect(onPress).toHaveBeenCalledTimes(1);
  });

  it('renders with icon', () => {
    const { getByTestId } = render(
      <EmptyState
        title="No results"
        icon={<Text testID="custom-icon">🔍</Text>}
      />,
    );
    expect(getByTestId('custom-icon')).toBeTruthy();
  });
});
