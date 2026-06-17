import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./packages/shared-types/tests/setup.ts', './packages/api-client/tests/setup.ts', './packages/shared-hooks/tests/setup.ts'],
  },
  resolve: {
    alias: {
      '@bornemap/shared-types': './packages/shared-types/src/index.ts',
      '@bornemap/api-client': './packages/api-client/src/index.ts',
      '@bornemap/shared-hooks': './packages/shared-hooks/src/index.ts',
    },
  },
});
