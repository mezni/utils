module.exports = {
  projects: [
    {
      displayName: 'ui',
      testMatch: ['<rootDir>/packages/ui/src/**/*.test.tsx'],
      transform: {
        '^.+\\.tsx?$': ['ts-jest', { tsconfig: '<rootDir>/packages/ui/tsconfig.json', jsx: 'react-jsx' }],
        '^.+\\.jsx?$': 'babel-jest',
      },
      transformIgnorePatterns: [
        'node_modules/(?!(.pnpm|react-native|@react-native|@testing-library/react-native|react-native-reanimated)/)',
      ],
      moduleNameMapper: {
        '@bornemap/tokens': '<rootDir>/packages/tokens/src/index.ts',
      },
      testEnvironment: 'node',
    },
  ],
};
