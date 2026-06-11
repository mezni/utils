import React, { createContext, useContext, useEffect, useState } from 'react';
import { Appearance, ColorSchemeName } from 'react-native';
import { ColorPalette, colors } from './colors';

interface ThemeContextValue {
  palette: ColorPalette;
  colorScheme: NonNullable<ColorSchemeName>;
}

const ThemeContext = createContext<ThemeContextValue>({
  palette: colors.light,
  colorScheme: 'light',
});

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [colorScheme, setColorScheme] = useState<NonNullable<ColorSchemeName>>(
    () => Appearance.getColorScheme() ?? 'light',
  );

  useEffect(() => {
    const listener = Appearance.addChangeListener(({ colorScheme: scheme }) => {
      setColorScheme(scheme ?? 'light');
    });
    return () => listener.remove();
  }, []);

  const value: ThemeContextValue = {
    palette: colors[colorScheme === 'dark' ? 'dark' : 'light'],
    colorScheme,
  };

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  );
}

export function useTheme(): ThemeContextValue {
  return useContext(ThemeContext);
}
