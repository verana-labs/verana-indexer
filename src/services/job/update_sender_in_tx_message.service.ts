/* eslint-disable no-await-in-loop */
import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import { Transaction, BlockCheckpoint } from '../../models';
import BullableService, { QueueHandler } from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE } from '../../common';
import config from '../../../config.json' assert { type: 'json' };
import knex from '../../common/utils/db_connection';

@Service({
  name: SERVICE.V1.JobService.UpdateSenderInTxMessages.key,
  version: 1,
})
export default class UpdateSenderInTxMessages extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
    jobName: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
  })
  async updateSender(_payload: { lastBlockCrawled: number }) {
    const blockCheckpoint = await BlockCheckpoint.query().findOne({
      job_name: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
    });
    this.logger.info(
      `Update sender in transaction_message table start from block ${blockCheckpoint?.height}`
    );
    if (blockCheckpoint?.height === _payload.lastBlockCrawled) {
      return;
    }

    let lastBlock =
      (blockCheckpoint?.height ?? 0) +
      config.jobUpdateSenderInTxMessages.blocksPerCall;
    if (lastBlock > _payload.lastBlockCrawled) {
      lastBlock = _payload.lastBlockCrawled;
    }
    const listTx = await Transaction.query()
      .withGraphFetched('events.[attributes]')
      .modifyGraph('events', (builder) => {
        builder.orderBy('id', 'asc');
      })
      .modifyGraph('events.[attributes]', (builder) => {
        builder.orderBy('index', 'asc');
      })
      .modifyGraph('messages', (builder) => {
        builder.orderBy('id', 'asc');
      })
      .orderBy('id', 'asc')
      .where('height', '>=', blockCheckpoint?.height ?? 0)
      .andWhere('height', '<', lastBlock);
    const listUpdates = listTx.map((tx) => {
      try {
        const sender = this._findFirstAttribute(tx.events, 'message', 'sender');
        return {
          tx_id: tx.id,
          sender,
        };
      } catch (error) {
        this.logger.warn('Tx error not has message.sender: ', tx.hash);
        return {
          tx_id: tx.id,
          sender: '',
        };
      }
    });

    await knex.transaction(async (trx) => {
      if (listUpdates.length > 0) {
        const stringListUpdates = listUpdates
          .map((update) => `(${update.tx_id}, '${update.sender}')`)
          .join(',');

        await knex
          .raw(
            `UPDATE transaction_message SET sender = temp.sender from (VALUES ${stringListUpdates}) as temp(tx_id, sender) where temp.tx_id = transaction_message.tx_id`
          )
          .transacting(trx);
      }
      await BlockCheckpoint.query()
        .update(
          BlockCheckpoint.fromJson({
            job_name: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
            height: lastBlock,
          })
        )
        .where({
          job_name: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
        })
        .transacting(trx);
    });
  }

  private _findFirstAttribute(
    events: any,
    eventType: string,
    attributeKey: string
  ): string {
    let result = '';
    const foundEvent = events.find(
      (event: any) =>
        event.type === eventType &&
        event.attributes.some(
          (attribute: any) => attribute.key === attributeKey
        )
    );
    if (foundEvent) {
      const foundAttribute = foundEvent.attributes.find(
        (attribute: any) => attribute.key === attributeKey
      );
      result = foundAttribute.value;
    }
    if (!result.length) {
      throw new Error(
        `Could not find attribute ${attributeKey} in event type ${eventType}`
      );
    }
    return result;
  }

  async _start(): Promise<void> {
    const blockCheckpoint = await BlockCheckpoint.query().findOne({
      job_name: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
    });
    if (!blockCheckpoint) {
      await BlockCheckpoint.query().insert({
        job_name: BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
        height: config.crawlBlock.startBlock,
      });
      const crawlBlockCheckpoint = await BlockCheckpoint.query().findOne({
        job_name: BULL_JOB_NAME.CRAWL_BLOCK,
      });

      this.createJob(
        BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
        BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
        {
          lastBlockCrawled: crawlBlockCheckpoint?.height ?? 0,
        },
        {
          removeOnComplete: true,
          removeOnFail: {
            count: 3,
          },
          repeat: {
            every: config.jobUpdateSenderInTxMessages.millisecondCrawl,
          },
        }
      );
    }
    return super._start();
  }
}
