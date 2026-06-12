import React, { createContext, useContext, useState, useEffect, useCallback, useMemo } from 'react';
import { useColorScheme } from 'react-native';

export type ThemeMode = 'light' | 'dark' | 'system';

export interface ThemeContextValue {
  mode: ThemeMode;
  isDark: boolean;
  resolvedMode: 'light' | 'dark';
  setMode: (mode: ThemeMode) => void;
}

const ThemeContext = createContext<ThemeContextValue>({
  mode: 'system',
  isDark: false,
  resolvedMode: 'light',
  setMode: () => {},
});

export function useTheme(): ThemeContextValue {
  return useContext(ThemeContext);
}

export interface ThemeProviderProps {
  mode?: ThemeMode;
  onModeChange?: (mode: ThemeMode) => void;
  children: React.ReactNode;
}

export function ThemeProvider({
  mode: initialMode = 'system',
  onModeChange,
  children,
}: ThemeProviderProps) {
  const systemScheme = useColorScheme();
  const [mode, setModeState] = useState<ThemeMode>(initialMode);

  useEffect(() => {
    setModeState(initialMode);
  }, [initialMode]);

  const resolvedMode: 'light' | 'dark' =
    mode === 'system' ? (systemScheme === 'dark' ? 'dark' : 'light') : mode;

  const setMode = useCallback(
    (newMode: ThemeMode) => {
      setModeState(newMode);
      onModeChange?.(newMode);
    },
    [onModeChange],
  );

  const value = useMemo(
    () => ({
      mode,
      isDark: resolvedMode === 'dark',
      resolvedMode,
      setMode,
    }),
    [mode, resolvedMode, setMode],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}
