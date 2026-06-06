export function getToken<T>(
  name: string,
  tokensMap: Record<string, T>,
): T {
  const value = tokensMap[name]
  if (value === undefined) {
    throw new Error(`Token "${name}" is not defined`)
  }
  return value
}
