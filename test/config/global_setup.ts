// eslint-disable-next-line @typescript-eslint/no-unused-vars
import type { Config } from '@jest/types';
import knex from '../../src/common/utils/db_connection';

export default async function setUp(
  _globalConfig: Config.GlobalConfig,
  _projectConfig: Config.ProjectConfig
) {
  try {
    console.log('Verifying test database setup...');
    
    const hasBlockTable = await knex.schema.hasTable('block');
    const hasMigrationsTable = await knex.schema.hasTable('knex_migrations');
    
    if (!hasBlockTable || !hasMigrationsTable) {
      console.warn('⚠️  Database tables are missing. Migrations should have been run by the workflow step.');
      console.warn('⚠️  If you see this in CI, check that "Run migrations" step completed successfully.');
      console.warn('⚠️  Tests may fail if required tables are missing.');
    } else {
      console.log('✅ Database tables verified. Test database is ready.');
    }
  } catch (error: any) {
    console.error('Error verifying database setup:', error?.message || error);
    console.warn('⚠️  Continuing with tests - migrations should have been run by workflow step.');
  }
}
