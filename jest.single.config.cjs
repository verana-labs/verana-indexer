// Use CommonJS syntax since Jest doesn't fully support ESM config yet
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
   globals: {
    'ts-jest': {
      isolatedModules: true // Add this to tsconfig.json too
    }
  },
  testMatch: ['**/crawl_proposal.spec.ts'],
  transform: {
    '^.+\\.ts$': ['ts-jest', {
      tsconfig: 'tsconfig.json',
      isolatedModules: true,
      useESM: true // Enable ESM support
    }]
  },
  
  extensionsToTreatAsEsm: ['.ts'], // Treat .ts files as ESM
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1' // Map .js imports to .ts
  }
}