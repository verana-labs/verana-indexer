import { Knex } from 'knex';
import { EvmEvent } from '../../src/models';
import config from '../../config.json' with { type: 'json' };

export async function up(knex: Knex): Promise<void> {
  const initPartitionName = `${EvmEvent.tableName}_partition_${config.migrationEvmEventToPartition.startId}_${config.migrationEvmEventToPartition.endId}`;
  await knex.raw(
    `
      CREATE TABLE evm_event
      (
        id SERIAL PRIMARY KEY ,
        tx_id        INTEGER NOT NULL,
        evm_tx_id    INTEGER NOT NULL,
        address      VARCHAR(255),
        topic0       VARCHAR(66),
        topic1       VARCHAR(66),
        topic2       VARCHAR(66),
        topic3       VARCHAR(66),
        block_height INTEGER,
        tx_hash      VARCHAR(255),
        tx_index     INTEGER,
        block_hash   VARCHAR(255)
      ) PARTITION BY RANGE (id);
    CREATE INDEX evm_event_address_index ON evm_event("address");
    CREATE INDEX evm_event_tx_id_index ON evm_event("tx_id");
    CREATE INDEX evm_event_evm_tx_id_index ON evm_event("evm_tx_id");
    CREATE INDEX evm_block_height_index ON evm_event("block_height");
    CREATE INDEX evm_block_tx_hash_index ON evm_event("tx_hash");
    CREATE INDEX evm_block_topic0_index ON evm_event("topic0") WHERE topic0 IS NOT NULL;
    CREATE INDEX evm_block_topic1_index ON evm_event("topic1") WHERE topic1 IS NOT NULL;
    CREATE INDEX evm_block_topic2_index ON evm_event("topic2") WHERE topic2 IS NOT NULL;
    CREATE INDEX evm_block_topic3_index ON evm_event("topic3") WHERE topic3 IS NOT NULL;
    CREATE TABLE ${initPartitionName}
      (LIKE ${config.jobCheckNeedCreateEvmEventPartition.templateTable} INCLUDING ALL EXCLUDING CONSTRAINTS);
    ALTER TABLE ${EvmEvent.tableName} ATTACH PARTITION ${initPartitionName}
      FOR VALUES FROM (${config.migrationEvmEventToPartition.startId}) to (${config.migrationEvmEventToPartition.endId})
  `
  );
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw(`DROP TABLE ${EvmEvent.tableName}`);
}
