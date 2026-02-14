import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  const partitionResult = await knex.raw(`
    SELECT
      child.relname AS partition_name
    FROM pg_inherits
      JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
      JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'transaction_message'
    ORDER BY child.relname;
  `);

  const partitions = partitionResult.rows.map((row: { partition_name: string }) => row.partition_name);

  for (const partitionName of partitions) {
    const indexName = `${partitionName}_tx_id_index_unique`;
    
    const indexExistsResult = await knex.raw(`
      SELECT 1 
      FROM pg_indexes 
      WHERE schemaname = 'public' 
        AND tablename = '${partitionName}' 
        AND indexname = '${indexName}'
    `);

    if (indexExistsResult.rows.length === 0) {
      try {
        await knex.raw(`
          CREATE UNIQUE INDEX "${indexName}" 
          ON "${partitionName}" (tx_id, index)
        `);
        console.log(`Created unique index ${indexName} on partition ${partitionName}`);
      } catch (error: any) {
        console.warn(`Failed to create unique index on ${partitionName}: ${error.message}`);
        const duplicates = await knex.raw(`
          SELECT tx_id, index, COUNT(*) as count
          FROM "${partitionName}"
          GROUP BY tx_id, index
          HAVING COUNT(*) > 1
          LIMIT 10
        `);
        
        if (duplicates.rows.length > 0) {
          console.warn(`Found duplicate (tx_id, index) pairs in ${partitionName}. Please clean up duplicates before running this migration.`);
        }
      }
    } else {
      console.log(`Index ${indexName} already exists on partition ${partitionName}`);
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  const partitionResult = await knex.raw(`
    SELECT
      child.relname AS partition_name
    FROM pg_inherits
      JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
      JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'transaction_message'
    ORDER BY child.relname;
  `);

  const partitions = partitionResult.rows.map((row: { partition_name: string }) => row.partition_name);

  for (const partitionName of partitions) {
    const indexName = `${partitionName}_tx_id_index_unique`;
    
    try {
      await knex.raw(`DROP INDEX IF EXISTS "${indexName}"`);
      console.log(`Dropped index ${indexName} from partition ${partitionName}`);
    } catch (error: any) {
      console.warn(`Failed to drop index ${indexName} from ${partitionName}: ${error.message}`);
    }
  }
}
