module.exports = {
  root: true,
  extends: ['eslint:recommended'],
  parser: '@typescript-eslint/parser',
  plugins: ['@typescript-eslint'],
  env: {
    'react-native/react-native': true,
    es2021: true,
    node: true,
  },
  settings: {
    react: {
      version: 'detect',
    },
  },
  rules: {
    'no-unused-vars': 'off',
    '@typescript-eslint/no-unused-vars': ['warn'],
  },
}
