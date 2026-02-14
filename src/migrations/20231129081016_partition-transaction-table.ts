import { Knex } from 'knex';
import config from '../config.json' with { type: 'json' };

export async function up(knex: Knex): Promise<void> {
  await knex.raw(
    `set statement_timeout to ${config.migrationTransactionToPartition.statementTimeout}`
  );
  await knex.transaction(async (trx) => {
    const transactionExists = await knex.raw(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'transaction'
      );
    `).transacting(trx);
    
    const transactionTableExists = transactionExists.rows[0].exists;
    
    const isPartitioned = await knex.raw(`
      SELECT EXISTS (
        SELECT FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
        AND c.relname = 'transaction'
        AND c.relkind = 'p'
      );
    `).transacting(trx);
    
    const transactionIsPartitioned = isPartitioned.rows[0].exists;
    
    const oldPartitionExists = await knex.raw(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'transaction_partition_0_100000000'
      );
    `).transacting(trx);
    
    const oldPartitionTableExists = oldPartitionExists.rows[0].exists;
    
    const partitionExists = await knex.raw(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'transaction_partition'
      );
    `).transacting(trx);
    
    const partitionTableExists = partitionExists.rows[0].exists;

    if (transactionIsPartitioned) {
      console.log('Transaction table is already partitioned. Skipping migration.');
      return;
    }

    if (!transactionTableExists && oldPartitionTableExists && partitionTableExists) {
      console.log('Migration partially completed. Completing migration...');
      await knex
        .raw('ALTER TABLE transaction_partition RENAME TO transaction;')
        .transacting(trx);
      
      try {
        await knex.raw(`
          ALTER TABLE transaction_partition_0_100000000
          ALTER COLUMN hash TYPE TEXT USING hash::TEXT;
        `).transacting(trx);
        console.log('Fixed hash column type from VARCHAR to TEXT');
      } catch (err: any) {
        if (!err.message?.includes('type "text"') && !err.message?.includes('does not exist')) {
          console.warn(`Warning fixing hash column: ${err.message}`);
        }
      }

      try {
        await knex.raw(`
          ALTER TABLE transaction_partition_0_100000000
          ALTER COLUMN codespace TYPE TEXT USING codespace::TEXT;
        `).transacting(trx);
        console.log('Fixed codespace column type from VARCHAR to TEXT');
      } catch (err: any) {
        if (!err.message?.includes('type "text"') && !err.message?.includes('does not exist')) {
          console.warn(`Warning fixing codespace column: ${err.message}`);
        }
      }
      
      const oldSeqTransaction = await knex.raw(
        `SELECT last_value FROM transaction_id_seq;`
      ).transacting(trx);
      const oldSeqValue = oldSeqTransaction.rows[0].last_value;
      await knex
        .raw(
          `ALTER SEQUENCE transaction_partition_id_seq RESTART WITH ${oldSeqValue};`
        )
        .transacting(trx);

      await knex
        .raw(
          `ALTER TABLE transaction ATTACH PARTITION transaction_partition_0_100000000 FOR VALUES FROM (0) TO (100000000)`
        )
        .transacting(trx);
      
      let startId = config.migrationTransactionToPartition.startId;
      let endId = config.migrationTransactionToPartition.endId;
      const step = config.migrationTransactionToPartition.step;
      for (let i = startId; i < endId; i += step) {
        const partitionName = `transaction_partition_${i}_${i + step}`;
        const partitionExistsCheck = await knex.raw(`
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '${partitionName}'
          );
        `).transacting(trx);
        
        if (!partitionExistsCheck.rows[0].exists) {
          await knex
            .raw(`CREATE TABLE ${partitionName} (LIKE transaction INCLUDING ALL)`)
            .transacting(trx);
          await knex
            .raw(
              `ALTER TABLE transaction ATTACH PARTITION ${partitionName} FOR VALUES FROM (${i}) TO (${i + step})`
            )
            .transacting(trx);
        }
      }
      return;
    }

    if (!transactionTableExists) {
      throw new Error('Transaction table does not exist. Cannot proceed with migration.');
    }

    // Create event table with config support partition on block height column
    if (!partitionTableExists) {
      await knex.raw(`
        CREATE TABLE transaction_partition
        (
          id SERIAL PRIMARY KEY,
          height INTEGER NOT NULL,
          hash TEXT NOT NULL,
          codespace  TEXT NOT NULL,
          code INTEGER NOT NULL,
          gas_used BIGINT NOT NULL,
          gas_wanted BIGINT NOT NULL,
          gas_limit BIGINT NOT NULL,
          fee JSONB NOT NULL,
          timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
          data JSONB,
          memo TEXT,
          index INTEGER
        ) PARTITION BY RANGE(id);
        CREATE INDEX transaction_partition_height_index
        ON transaction_partition (height);
        CREATE INDEX transaction_partition_index_index
        ON transaction_partition (index);
        CREATE INDEX transaction_partition_hash_index
          ON transaction_partition (hash);
        CREATE INDEX transaction_partition_timestamp_index
          ON transaction_partition (timestamp);
      `).transacting(trx);
    }

    // Update new table name(event_partition) to event name
    await knex
      .raw(
        `
        ALTER TABLE transaction DROP CONSTRAINT IF EXISTS transaction_height_foreign;
        ALTER TABLE transaction RENAME TO transaction_partition_0_100000000;
      `
      )
      .transacting(trx);
    
    try {
      await knex.raw(`
        ALTER TABLE transaction_partition_0_100000000
        ALTER COLUMN hash TYPE TEXT USING hash::TEXT;
      `).transacting(trx);
      console.log('Fixed hash column type from VARCHAR to TEXT');
    } catch (err: any) {
      if (!err.message?.includes('type "text"') && !err.message?.includes('does not exist')) {
        console.warn(`Warning fixing hash column: ${err.message}`);
      }
    }

    try {
      await knex.raw(`
        ALTER TABLE transaction_partition_0_100000000
        ALTER COLUMN codespace TYPE TEXT USING codespace::TEXT;
      `).transacting(trx);
      console.log('Fixed codespace column type from VARCHAR to TEXT');
    } catch (err: any) {
      if (!err.message?.includes('type "text"') && !err.message?.includes('does not exist')) {
        console.warn(`Warning fixing codespace column: ${err.message}`);
      }
    }
    
    await knex
      .raw('ALTER TABLE transaction_partition RENAME TO transaction;')
      .transacting(trx);

    // Drop fk on old table and create again fk point to new transaction partitioned table
    await knex
      .raw(
        `
        ALTER TABLE transaction_message DROP CONSTRAINT IF EXISTS transaction_message_tx_id_foreign;
        ALTER TABLE event DROP CONSTRAINT IF EXISTS event_partition_transaction_foreign;
        ALTER TABLE event_attribute DROP CONSTRAINT IF EXISTS event_attribute_partition_tx_id_foreign;
        ALTER TABLE vote DROP CONSTRAINT IF EXISTS vote_tx_id_foreign;
        ALTER TABLE power_event DROP CONSTRAINT IF EXISTS power_event_tx_id_foreign;
        ALTER TABLE feegrant_history DROP CONSTRAINT IF EXISTS feegrant_history_tx_id_foreign;
        ALTER TABLE feegrant DROP CONSTRAINT IF EXISTS feegrant_init_tx_id_foreign;
      `
      )
      .transacting(trx);

    // update seq
    const oldSeqTransaction = await knex.raw(
      `SELECT last_value FROM transaction_id_seq;`
    );
    const oldSeqValue = oldSeqTransaction.rows[0].last_value;
    await knex
      .raw(
        `ALTER SEQUENCE transaction_partition_id_seq RESTART WITH ${oldSeqValue};`
      )
      .transacting(trx);

    // add old table transaction into transaction partitioned
    await knex
      .raw(
        `ALTER TABLE transaction ATTACH PARTITION transaction_partition_0_100000000 FOR VALUES FROM (0) TO (100000000)`
      )
      .transacting(trx);
    /**
     * @description: Create partition base on id column and range value by step
     * Then apply partition to table
     */
    let startId = config.migrationTransactionToPartition.startId;
    let endId = config.migrationTransactionToPartition.endId;
    const step = config.migrationTransactionToPartition.step;
    for (let i = startId; i < endId; i += step) {
      const partitionName = `transaction_partition_${i}_${i + step}`;
      
      const partitionExistsCheck = await knex.raw(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = '${partitionName}'
        );
      `).transacting(trx);
      
      if (!partitionExistsCheck.rows[0].exists) {
        await knex
          .raw(`CREATE TABLE ${partitionName} (LIKE transaction INCLUDING ALL)`)
          .transacting(trx);
        await knex
          .raw(
            `ALTER TABLE transaction ATTACH PARTITION ${partitionName} FOR VALUES FROM (${i}) TO (${i + step})`
          )
          .transacting(trx);
      } else {
        console.log(`Partition ${partitionName} already exists. Skipping creation.`);
      }
    }
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.transaction(async (trx) => {
    await knex
      .raw(
        `
        ALTER TABLE transaction DETACH PARTITION transaction_partition_0_100000000;
      `
      )
      .transacting(trx);
    await knex
      .raw('alter table transaction rename to transaction_partition;')
      .transacting(trx);
    await knex
      .raw(
        'alter table transaction_partition_0_100000000 rename to transaction;'
      )
      .transacting(trx);
    await knex.schema.dropTableIfExists('transaction_partition');

    await knex
      .raw(
        `
        ALTER TABLE transaction_message DROP CONSTRAINT transaction_message_tx_id_foreign;
        ALTER TABLE event DROP CONSTRAINT event_partition_transaction_foreign;
        ALTER TABLE event_attribute DROP CONSTRAINT event_attribute_partition_tx_id_foreign;
        ALTER TABLE vote DROP CONSTRAINT vote_tx_id_foreign;
        ALTER TABLE power_event DROP CONSTRAINT power_event_tx_id_foreign;
        ALTER TABLE feegrant_history DROP CONSTRAINT feegrant_history_tx_id_foreign;
        ALTER TABLE feegrant DROP CONSTRAINT feegrant_init_tx_id_foreign;
      `
      )
      .transacting(trx);
    await knex
      .raw(
        `
        ALTER TABLE transaction_message ADD CONSTRAINT transaction_message_tx_id_foreign FOREIGN KEY (tx_id) REFERENCES transaction(id);
        ALTER TABLE event ADD CONSTRAINT event_partition_transaction_foreign FOREIGN KEY (tx_id) REFERENCES transaction(id);
        ALTER TABLE event_attribute ADD CONSTRAINT event_attribute_partition_tx_id_foreign FOREIGN KEY (tx_id) REFERENCES transaction(id);
        ALTER TABLE vote ADD CONSTRAINT vote_tx_id_foreign FOREIGN KEY (tx_id) REFERENCES transaction(id);
        ALTER TABLE power_event ADD CONSTRAINT power_event_tx_id_foreign FOREIGN KEY (tx_id) REFERENCES transaction(id);
        ALTER TABLE feegrant_history ADD CONSTRAINT feegrant_history_tx_id_foreign FOREIGN KEY (tx_id) REFERENCES transaction(id);
        ALTER TABLE feegrant ADD CONSTRAINT feegrant_init_tx_id_foreign FOREIGN KEY (init_tx_id) REFERENCES transaction(id);
      `
      )
      .transacting(trx);
  });
}
