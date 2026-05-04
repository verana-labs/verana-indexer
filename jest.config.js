// jest.config.cjs
export default {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/*.spec.ts'],
  globalSetup: '<rootDir>/test/config/global_setup.ts',
  globalTeardown: '<rootDir>/test/config/global_teardown.ts',
  transform: {
    '^.+\\.ts$': [
      'ts-jest',
      {
        tsconfig: 'tsconfig.json',
        isolatedModules: true,
        useESM: true,
      },
    ],
  },
  extensionsToTreatAsEsm: ['.ts'],
  moduleNameMapper: {
    '^@verana-labs/verre$': '<rootDir>/test/config/mocks/verre.ts',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  globals: {
    'ts-jest': {
      isolatedModules: true,
    },
  },
};
