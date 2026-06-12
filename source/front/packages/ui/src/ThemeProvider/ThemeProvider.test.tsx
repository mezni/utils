import { render } from '@testing-library/react-native';
import { Text } from 'react-native';
import { ThemeProvider, useTheme } from './ThemeProvider';

describe('ThemeProvider', () => {
  it('renders children', () => {
    const { getByText } = render(
      <ThemeProvider>
        <Text>Hello</Text>
      </ThemeProvider>,
    );
    expect(getByText('Hello')).toBeTruthy();
  });

  it('provides default system mode', () => {
    function Consumer() {
      const { mode } = useTheme();
      return <Text>{mode}</Text>;
    }
    const { getByText } = render(
      <ThemeProvider>
        <Consumer />
      </ThemeProvider>,
    );
    expect(getByText('system')).toBeTruthy();
  });

  it('accepts light mode', () => {
    function Consumer() {
      const { mode, isDark } = useTheme();
      return <Text>{mode}-{isDark ? 'dark' : 'light'}</Text>;
    }
    const { getByText } = render(
      <ThemeProvider mode="light">
        <Consumer />
      </ThemeProvider>,
    );
    expect(getByText('light-light')).toBeTruthy();
  });

  it('accepts dark mode', () => {
    function Consumer() {
      const { mode, isDark } = useTheme();
      return <Text>{mode}-{isDark ? 'dark' : 'light'}</Text>;
    }
    const { getByText } = render(
      <ThemeProvider mode="dark">
        <Consumer />
      </ThemeProvider>,
    );
    expect(getByText('dark-dark')).toBeTruthy();
  });
});
