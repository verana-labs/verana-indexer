import { Knex } from 'knex';
import network from './network.json' assert { type: 'json' };
import configJson from './config.json' assert { type: 'json' };
import { Config } from './src/common';

// Update with your config settings.

const config: { [key: string]: Knex.Config } = {
  development: {
    client: 'pg',
    migrations: {
      directory: ['./migrations'],
    },
    connection: {
      database: network.find((item) => item.chainId === configJson.chainId)
        ?.databaseName,
      host: Config.POSTGRES_HOST,
      user: Config.POSTGRES_USER,
      password: Config.POSTGRES_PASSWORD,
      port: Config.POSTGRES_PORT,
      statement_timeout: Config.POSTGRES_STATEMENT_TIMEOUT,
    },
    pool: {
      min: 1,
      max: parseInt(Config.POSTGRES_POOL_MAX ?? '5', 10),
    },
  },
  test: {
    client: 'pg',
    migrations: {
      directory: ['./migrations',],
    },
    connection: {
      database: Config.POSTGRES_DB_TEST,
      host: 'localhost',
      user: Config.POSTGRES_USER,
      password: Config.POSTGRES_PASSWORD,
      port: Config.POSTGRES_PORT,
      statement_timeout: Config.POSTGRES_STATEMENT_TIMEOUT,
    },
  },
  production: {
    client: 'pg',
    migrations: {
      directory: ['./migrations',],
    },
    connection: {
      database: network.find((item) => item.chainId === configJson.chainId)
        ?.databaseName,
      host: Config.POSTGRES_HOST,
      user: Config.POSTGRES_USER,
      password: Config.POSTGRES_PASSWORD,
      port: Config.POSTGRES_PORT,
      statement_timeout: Config.POSTGRES_STATEMENT_TIMEOUT,
    },
    pool: {
      min: 1,
      max: parseInt(Config.POSTGRES_POOL_MAX ?? '5', 10),
    },
  },
  sourcify: {
    client: 'pg',
    connection: {
      database: Config.SOURCIFY_POSTGRES_DB,
      host: Config.SOURCIFY_POSTGRES_HOST,
      user: Config.SOURCIFY_POSTGRES_USER,
      password: Config.SOURCIFY_POSTGRES_PASSWORD,
      port: Config.SOURCIFY_POSTGRES_PORT,
      statement_timeout: Config.SOURCIFY_POSTGRES_STATEMENT_TIMEOUT,
    },
    pool: {
      min: 1,
      max: parseInt(Config.SOURCIFY_POSTGRES_POOL_MAX ?? '5', 10),
    },
  },
};

export default config;
