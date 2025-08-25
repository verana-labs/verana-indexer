module.exports = {
  root: true,
  env: {
    node: true,
    es2022: true,
    jest: true,
  },
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'airbnb-base',
    'airbnb-typescript/base',
    'prettier',
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
    project: ['./tsconfig.json', './tsconfig.build.json'],
    tsconfigRootDir: __dirname,
  },
  plugins: ['@typescript-eslint', 'import', 'prettier'],
  rules: {
    // Disable Prettier errors in ESLint; use `pnpm run format:check` instead
    'prettier/prettier': 'off',
    // Allow CRLF/LF differences across OS
    'linebreak-style': 'off',
    'import/extensions': 'off',
    'import/prefer-default-export': 'off',
    'import/no-unresolved': 'off',

    // Stylistic/ergonomic relaxations for legacy codebase to get CI green
    'max-len': 'off',
    'no-plusplus': 'off',
    'no-continue': 'off',
    'no-await-in-loop': 'off',
    'no-restricted-syntax': 'off',
    'default-case': 'off',
    'object-shorthand': 'off',
    'prefer-destructuring': 'off',
    radix: 'off',
    '@typescript-eslint/lines-between-class-members': 'off',
    '@typescript-eslint/no-use-before-define': 'off',
    '@typescript-eslint/no-shadow': 'off',
    '@typescript-eslint/return-await': 'off',
    'no-nested-ternary': 'off',
    '@typescript-eslint/no-useless-constructor': 'off',
    'no-empty': 'off',
    '@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
    '@typescript-eslint/explicit-function-return-type': 'off',
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/no-explicit-any': 'warn',
    'no-console': 'warn',
    'no-debugger': 'error',
    'class-methods-use-this': 'off',
    'no-underscore-dangle': 'off',
    'max-len': 'off',
  },
  settings: {
    'import/resolver': {
      typescript: {
        alwaysTryTypes: true,
        project: ['./tsconfig.json', './tsconfig.build.json'],
      },
    },
  },
  ignorePatterns: ['dist/', 'node_modules/', '*.js', '*.mjs'],
};
