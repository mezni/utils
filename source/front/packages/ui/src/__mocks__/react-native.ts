import React from 'react';

const mockComponent = (name: string) => {
  const Comp = ({ children, style, ...props }: any) =>
    React.createElement('div', { ...props, 'data-testid': name, style }, children);
  return Comp;
};

const mockText = ({ children, style, ...props }: any) =>
  React.createElement('span', { ...props, 'data-testid': 'Text', style }, children);

const mockView = ({ children, style, ...props }: any) =>
  React.createElement('div', { ...props, 'data-testid': 'View', style }, children);

const AnimatedMock = {
  View: mockComponent('Animated.View'),
  Text: mockComponent('Animated.Text'),
  Value: (v: number) => ({ interpolate: () => ({}) }),
  timing: () => ({ start: () => {} }),
  loop: (a: any) => a,
  sequence: (a: any[]) => a[0],
};

export const View = mockView;
export const Text = mockText;
export const StyleSheet = {
  create: (styles: any) => styles,
  absoluteFill: {},
  absoluteFillObject: { position: 'absolute', top: 0, left: 0, right: 0, bottom: 0 },
  hairlineWidth: 1,
  flatten: (s: any) => s,
};
export const TouchableOpacity = ({ children, onPress, style, activeOpacity, disabled }: any) =>
  React.createElement(
    'button',
    { onClick: disabled ? undefined : onPress, style, disabled },
    children,
  );
export const ActivityIndicator = ({ color, size }: any) =>
  React.createElement('div', { 'data-testid': 'ActivityIndicator' });
export const Animated = AnimatedMock;
export const Platform = { OS: 'web', select: (obj: any) => obj.web };
export const useColorScheme = () => 'light';
