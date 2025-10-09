import { Knex } from 'knex';
import network from './network.json' with { type: 'json' };
import configJson from './config.json' with { type: 'json' };
import { Config } from './src/common';

const DEFAULT_POOL_MAX = 10;
const DEFAULT_STATEMENT_TIMEOUT = 20000; 

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
      statement_timeout: parseInt(Config.POSTGRES_STATEMENT_TIMEOUT ?? DEFAULT_STATEMENT_TIMEOUT.toString(), 10),
    },
    pool: {
      min: 2,
      max: parseInt(Config.POSTGRES_POOL_MAX ?? DEFAULT_POOL_MAX.toString(), 10),
      acquireTimeoutMillis: 60000, 
      idleTimeoutMillis: 10000,    
    },
  },
  test: {
    client: 'pg',
    migrations: {
      directory: ['./migrations'],
    },
    connection: {
      database: Config.POSTGRES_DB_TEST,
      host: 'localhost',
      user: Config.POSTGRES_USER,
      password: Config.POSTGRES_PASSWORD,
      port: Config.POSTGRES_PORT,
      statement_timeout: parseInt(Config.POSTGRES_STATEMENT_TIMEOUT ?? DEFAULT_STATEMENT_TIMEOUT.toString(), 10),
    },
    pool: {
      min: 1,
      max: 5,
      acquireTimeoutMillis: 30000,
      idleTimeoutMillis: 5000,
    },
  },
  production: {
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
      statement_timeout: parseInt(Config.POSTGRES_STATEMENT_TIMEOUT ?? DEFAULT_STATEMENT_TIMEOUT.toString(), 10),
    },
    pool: {
      min: 2,
      max: parseInt(Config.POSTGRES_POOL_MAX ?? DEFAULT_POOL_MAX.toString(), 10),
      acquireTimeoutMillis: 60000,
      idleTimeoutMillis: 10000,
    },
  },
};

export default config;
