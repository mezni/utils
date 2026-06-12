import { Platform } from 'react-native';

export const isWeb = Platform.OS === 'web';
export const isNative = Platform.OS === 'ios' || Platform.OS === 'android';

export function platformSpecific<T>(web: T, native: T): T {
  return isWeb ? web : native;
}
