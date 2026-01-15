// eslint-disable-next-line @typescript-eslint/no-unused-vars
import type { Config } from '@jest/types';
import knex from '../../src/common/utils/db_connection';

export default async function setUp(
  _globalConfig: Config.GlobalConfig,
  _projectConfig: Config.ProjectConfig
) {
  try {
    console.log('Running database migrations for tests...');
    
    const [pending] = await knex.migrate.list();
    
    if (pending && pending.length > 0) {
      console.log(`Found ${pending.length} pending migration(s), running migrations...`);
      await knex.migrate.latest();
      console.log('Migrations completed successfully.');
    } else {
      console.log('No pending migrations, database is up to date.');
    }
  } catch (error: any) {
    console.error('Error running migrations in test setup:', error?.message || error);
    throw error;
  }
}
