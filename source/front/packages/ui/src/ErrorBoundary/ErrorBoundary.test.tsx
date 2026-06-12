import { render, fireEvent } from '@testing-library/react-native';
import { Text } from 'react-native';
import { ErrorBoundary } from './ErrorBoundary';

describe('ErrorBoundary', () => {
  it('renders children when no error', () => {
    const { getByText } = render(
      <ErrorBoundary>
        <Text>All good</Text>
      </ErrorBoundary>,
    );
    expect(getByText('All good')).toBeTruthy();
  });

  it('renders error UI when child throws', () => {
    function Thrower() {
      throw new Error('Boom!');
    }

    const { getByText } = render(
      <ErrorBoundary>
        <Thrower />
      </ErrorBoundary>,
    );
    expect(getByText('Something went wrong')).toBeTruthy();
    expect(getByText('Boom!')).toBeTruthy();
  });

  it('renders custom fallback instead of default', () => {
    function Thrower() {
      throw new Error('Boom!');
    }

    const { getByText } = render(
      <ErrorBoundary fallback={<Text>Custom error</Text>}>
        <Thrower />
      </ErrorBoundary>,
    );
    expect(getByText('Custom error')).toBeTruthy();
  });

  it('retries after error', () => {
    function Thrower() {
      throw new Error('Boom!');
    }

    const { getByText, queryByText } = render(
      <ErrorBoundary>
        <Thrower />
      </ErrorBoundary>,
    );

    expect(getByText('Something went wrong')).toBeTruthy();
    fireEvent.press(getByText('Try Again'));
    expect(getByText('Something went wrong')).toBeTruthy();
  });
});
