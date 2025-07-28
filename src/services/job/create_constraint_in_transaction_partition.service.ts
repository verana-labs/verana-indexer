import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import BigNumber from 'bignumber.js';
import BullableService, { QueueHandler } from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE } from '../../common';
import knex from '../../common/utils/db_connection';
import config from '../../../config.json' assert { type: 'json' };
import { Transaction } from '../../models';

@Service({
  name: SERVICE.V1.JobService.CreateConstraintInTransactionPartition.key,
  version: 1,
})
export default class CreateConstraintInTransactionPartitionJob extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  public createConstraintTransactionStatus = {
    currentPartitionEmpty: 'currentPartitionEmpty',
    currentPartitionDoneOrInserting: 'currentPartitionDoneOrInserting',
    constraintUpdated: 'constraintUpdated',
  };

  public insertionStatus = {
    empty: 'empty',
    done: 'done',
    inserting: 'inserting',
  };

  public async getLatestTransactionByPartition(
    partitionName: string
  ): Promise<Transaction> {
    const latestTransaction = await knex.raw(`
      SELECT * FROM ${partitionName} ORDER BY id DESC LIMIT 1
    `);
    return latestTransaction.rows[0];
  }

  /**
   * @description get max min tx_id and max min block height in partition
   * @param partitionName
   */
  public async getMaxMinIdAndHeightByPartition(partitionName: string): Promise<{
    min_id: number;
    max_id: number;
    min_height: number;
    max_height: number;
  }> {
    await knex.raw(
      `set statement_timeout to ${config.jobCreateConstraintInTransactionPartition.statementTimeout}`
    );
    const boundariesResult = await knex.raw(`
        SELECT
            min(id) min_id, max(id) max_id, min(height) min_height, max(height) max_height
        FROM ${partitionName}`);

    return {
      min_id: boundariesResult.rows[0].min_id,
      max_id: boundariesResult.rows[0].max_id,
      min_height: boundariesResult.rows[0].min_height,
      max_height: boundariesResult.rows[0].max_height,
    };
  }

  /**
   * @description Check partition insertion is done or inserting or empty
   * @param partitionName
   * @param toId
   * @public
   */
  public async getPartitionInsertionInfo(
    partitionName: string,
    toId: string
  ): Promise<string> {
    const latestTransactionPartition =
      await this.getLatestTransactionByPartition(partitionName);

    if (!latestTransactionPartition) return this.insertionStatus.empty;

    const endValueOfPartition = BigNumber(toId).minus(1);

    if (endValueOfPartition.eq(latestTransactionPartition.id))
      return this.insertionStatus.done;

    return this.insertionStatus.inserting;
  }

  /**
   * @description Get current constraint of partition
   * @param partitionName
   */
  public async getCurrentConstrainInfo(partitionName: string): Promise<string> {
    const constraintResult = await knex.raw(`
        SELECT
            connamespace::regnamespace "schema",
            conrelid::regclass "table",
            conname "constraint",
            pg_get_constraintdef(oid) "definition"
        FROM pg_constraint
        WHERE conrelid = '${partitionName}'::regclass and conname like 'tx_ct%'
    `);
    const result = constraintResult.rows.map(
      (constraint: { constraint: string }) => constraint.constraint
    );
    return result[0];
  }

  /**
   * @description Prepare constraint, decide to create new, drop or do nothing
   * @param partitionName
   * @param currentConstraintName
   * @param insertionStatus (done, inserting, empty)
   * @public
   */
  public async prepareConstrainInformation(
    partitionName: string,
    insertionStatus: string,
    currentConstraintName: string
  ): Promise<{
    createConstraint: { fromHeight: number; toHeight: number | null };
    dropConstraint: string | null;
  } | null> {
    // Don't need to create constraint because current partition is empty
    if (insertionStatus === this.insertionStatus.empty) return null;
    if (
      insertionStatus === this.insertionStatus.inserting &&
      currentConstraintName
    )
      return null;
    // Already has done constraint
    if (currentConstraintName) {
      // Naming like constraintName_status, so pop() will get current status of constraint
      const constraintStatus = currentConstraintName.split('_').pop();
      // Current done and having full constraint so do nothing
      if (constraintStatus === this.insertionStatus.done) return null;
    }

    const maxMinIdAndHeight = await this.getMaxMinIdAndHeightByPartition(
      partitionName
    );

    if (insertionStatus === this.insertionStatus.inserting) {
      return {
        createConstraint: {
          fromHeight: maxMinIdAndHeight.min_height,
          toHeight: null,
        },
        dropConstraint: null,
      };
    }

    return {
      createConstraint: {
        fromHeight: maxMinIdAndHeight.min_height,
        toHeight: maxMinIdAndHeight.max_height,
      },
      dropConstraint: currentConstraintName,
    };
  }

  /**
   * @description Get list partition of transaction table
   */
  public async getTransactionPartitionInfo(): Promise<
    {
      name: string;
      fromId: string;
      toId: string;
    }[]
  > {
    const partitionTable = await knex.raw(`
        SELECT
            parent.relname AS parent,
            child.relname AS child,
            pg_get_expr(child.relpartbound, child.oid) AS bounds
        FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
        WHERE parent.relname = 'transaction';
    `);
    return partitionTable.rows.map(
      (partition: { bounds: string; child: string }) => ({
        name: partition.child,
        fromId: partition.bounds?.match(/\d+/g)?.[0],
        toId: partition.bounds?.match(/\d+/g)?.[1],
      })
    );
  }

  public async createConstraint(
    partitionName: string,
    fromHeight: number,
    toHeight: number | null,
    currentConstraintName: string | null
  ): Promise<void> {
    let constraintName: string;
    let checkConstraint: string;

    if (toHeight === null) {
      constraintName = `tx_ct_${partitionName}_${this.insertionStatus.inserting}`;
      checkConstraint = `(height >= ${fromHeight})`;
    } else {
      constraintName = `tx_ct_${partitionName}_${this.insertionStatus.done}`;
      checkConstraint = `(height >= ${fromHeight} AND height <= ${toHeight})`;
    }

    await knex.transaction(async (trx) => {
      if (currentConstraintName) {
        this.logger.info(`DROP constraint ${currentConstraintName}`);
        await knex
          .raw(
            `ALTER TABLE ${partitionName} DROP CONSTRAINT ${currentConstraintName}`
          )
          .transacting(trx);
      }
      await knex
        .raw(
          `
        ALTER TABLE ${partitionName}
        ADD CONSTRAINT ${constraintName} check ${checkConstraint} not valid
      `
        )
        .transacting(trx);
      await knex
        .raw(
          `
        ALTER TABLE ${partitionName} validate constraint ${constraintName}
      `
        )
        .transacting(trx);
      this.logger.info(`Constraint created with name ${constraintName}`);
    });
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.JOB_CREATE_TRANSACTION_CONSTRAINT,
    jobName: BULL_JOB_NAME.JOB_CREATE_TRANSACTION_CONSTRAINT,
  })
  public async createTransactionConstraint(_payload: {
    name: string;
    fromId: string;
    toId: string;
  }): Promise<string> {
    const partitionInsertionStatus = await this.getPartitionInsertionInfo(
      _payload.name,
      _payload.toId
    );
    if (partitionInsertionStatus === this.insertionStatus.empty) {
      this.logger.info(
        "Current partition is empty, so don't need to create constraint",
        _payload.name
      );
      return this.createConstraintTransactionStatus.currentPartitionEmpty;
    }

    const currentConstraint = await this.getCurrentConstrainInfo(_payload.name);
    const prepareConstraintCreation = await this.prepareConstrainInformation(
      _payload.name,
      partitionInsertionStatus,
      currentConstraint
    );

    if (!prepareConstraintCreation) {
      this.logger.info(
        "Current partition is not done and already having constraint or already having full constraint, so don't need to create constraint",
        _payload.name
      );
      return this.createConstraintTransactionStatus
        .currentPartitionDoneOrInserting;
    }

    await this.createConstraint(
      _payload.name,
      prepareConstraintCreation.createConstraint.fromHeight,
      prepareConstraintCreation.createConstraint.toHeight,
      prepareConstraintCreation.dropConstraint
    );
    return this.createConstraintTransactionStatus.constraintUpdated;
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.JOB_CHECK_TRANSACTION_CONSTRAINT,
    jobName: BULL_JOB_NAME.JOB_CHECK_TRANSACTION_CONSTRAINT,
  })
  public async createConstraintInTransactionPartition() {
    const listPartition = await this.getTransactionPartitionInfo();
    listPartition.forEach(
      (partition: { name: string; fromId: string; toId: string }) => {
        this.createJob(
          BULL_JOB_NAME.JOB_CREATE_TRANSACTION_CONSTRAINT,
          BULL_JOB_NAME.JOB_CREATE_TRANSACTION_CONSTRAINT,
          partition,
          {
            jobId: partition.name,
            removeOnComplete: true,
            removeOnFail: {
              count: 3,
            },
          }
        );
      }
    );
  }

  public async _start(): Promise<void> {
    this.createJob(
      BULL_JOB_NAME.JOB_CHECK_TRANSACTION_CONSTRAINT,
      BULL_JOB_NAME.JOB_CHECK_TRANSACTION_CONSTRAINT,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every:
            config.jobCreateConstraintInTransactionPartition
              .jobRepeatCheckNeedCreateConstraint.millisecondRepeatJob,
        },
      }
    );
    return super._start();
  }
}
