import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import _ from 'lodash';
import { parseCoins } from '@cosmjs/proto-signing';
import { GetNodeInfoResponseSDKType } from '@aura-nw/aurajs/types/codegen/cosmos/base/tendermint/v1beta1/query';
import { SemVer } from 'semver';
import knex from '../../common/utils/db_connection';
import { BULL_JOB_NAME, SERVICE, getLcdClient } from '../../common';
import {
  PowerEvent,
  Validator,
  Event,
  EventAttribute,
  BlockCheckpoint,
} from '../../models';
import BullableService, { QueueHandler } from '../../base/bullable.service';
import config from '../../config.json' with { type: 'json' };

@Service({
  name: SERVICE.V1.HandleStakeEventService.key,
  version: 1,
})
export default class HandleStakeEventService extends BullableService {
  private eventStakes = [
    Event.EVENT_TYPE.DELEGATE,
    Event.EVENT_TYPE.REDELEGATE,
    Event.EVENT_TYPE.UNBOND,
    Event.EVENT_TYPE.CREATE_VALIDATOR,
  ];

  private cosmosSdkVersion!: SemVer;

  // map to get index of amount attribute inside event stake based on cosmos sdk version
  private mapIndexAmountWithEventStakes = {
    '< 0.47.8': {
      [Event.EVENT_TYPE.DELEGATE]: 1,
      [Event.EVENT_TYPE.REDELEGATE]: 2,
      [Event.EVENT_TYPE.UNBOND]: 1,
      [Event.EVENT_TYPE.CREATE_VALIDATOR]: 1,
    },
    '>= 0.47.8': {
      [Event.EVENT_TYPE.DELEGATE]: 2,
      [Event.EVENT_TYPE.REDELEGATE]: 2,
      [Event.EVENT_TYPE.UNBOND]: 1,
      [Event.EVENT_TYPE.CREATE_VALIDATOR]: 1,
    },
  };

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.HANDLE_STAKE_EVENT,
    jobName: BULL_JOB_NAME.HANDLE_STAKE_EVENT,
    // prefix: `horoscope-v2-${config.chainId}`,
  })
  public async handleJob(_payload: object): Promise<void> {
    const [startHeight, endHeight, updateBlockCheckpoint] =
      await BlockCheckpoint.getCheckpoint(
        BULL_JOB_NAME.HANDLE_STAKE_EVENT,
        [BULL_JOB_NAME.HANDLE_TRANSACTION, BULL_JOB_NAME.CRAWL_VALIDATOR],
        config.handleStakeEvent.key
      );
    this.logger.info(`startHeight: ${startHeight}, endHeight: ${endHeight}`);
    if (startHeight >= endHeight) return;

    const resultEvents = await Event.query()
      .select('event.id as event_id', 'event.type', 'event.block_height')
      .withGraphFetched('transaction')
      .modifyGraph('transaction', (builder) => {
        builder.select('id', 'timestamp');
      })
      .withGraphFetched('attributes')
      .modifyGraph('attributes', (builder) => {
        builder.select('key', 'value', 'index').orderBy('index');
      })
      .whereIn('event.type', this.eventStakes)
      .andWhere('event.block_height', '>', startHeight)
      .andWhere('event.block_height', '<=', endHeight);

    const validators: Validator[] = await Validator.query();
    const validatorKeys = _.keyBy(validators, 'operator_address');

    const powerEvents: PowerEvent[] = [];
    resultEvents.forEach((stakeEvent) => {
      try {
        let validatorSrcId;
        let validatorDstId;
        let amount;
        let amountRaw;
        const firstValidator = stakeEvent.attributes.find(
          (attr: any) =>
            attr.key === EventAttribute.ATTRIBUTE_KEY.VALIDATOR ||
            attr.key === EventAttribute.ATTRIBUTE_KEY.SOURCE_VALIDATOR
        );
        if (!firstValidator) {
          this.logger.warn(
            `stake event ${stakeEvent.id} doesn't have first validator`
          );
          return;
        }
        const destValidator = stakeEvent.attributes.find(
          (attr: any) =>
            attr.key === EventAttribute.ATTRIBUTE_KEY.DESTINATION_VALIDATOR
        );
        switch (stakeEvent.type) {
          case PowerEvent.TYPES.DELEGATE: {
            validatorDstId = validatorKeys[firstValidator.value].id;
            amountRaw = this.getRawAmountByFirstIndex(
              stakeEvent.attributes,
              PowerEvent.TYPES.DELEGATE,
              firstValidator.index
            );
            break;
          }
          case PowerEvent.TYPES.REDELEGATE:
            validatorSrcId = validatorKeys[firstValidator.value].id;
            if (!destValidator) {
              this.logger.warn(
                `stake event ${stakeEvent.id} doesn't have destination validator`
              );
              return;
            }
            validatorDstId = validatorKeys[destValidator.value].id;
            amountRaw = this.getRawAmountByFirstIndex(
              stakeEvent.attributes,
              PowerEvent.TYPES.REDELEGATE,
              firstValidator.index
            );
            break;
          case PowerEvent.TYPES.UNBOND:
            validatorSrcId = validatorKeys[firstValidator.value].id;
            amountRaw = this.getRawAmountByFirstIndex(
              stakeEvent.attributes,
              PowerEvent.TYPES.UNBOND,
              firstValidator.index
            );
            break;
          case PowerEvent.TYPES.CREATE_VALIDATOR:
            validatorDstId = validatorKeys[firstValidator.value].id;
            amountRaw = this.getRawAmountByFirstIndex(
              stakeEvent.attributes,
              PowerEvent.TYPES.CREATE_VALIDATOR,
              firstValidator.index
            );
            break;
          default:
            break;
        }
        if (amountRaw) {
          amount = parseCoins(amountRaw?.value)[0].amount;
        }
        const powerEvent: PowerEvent = PowerEvent.fromJson({
          tx_id: stakeEvent.transaction.id,
          height: stakeEvent.block_height,
          type: stakeEvent.type,
          validator_src_id: validatorSrcId,
          validator_dst_id: validatorDstId,
          amount,
          time: stakeEvent.transaction.timestamp.toISOString(),
        });

        powerEvents.push(powerEvent);
      } catch (error) {
        this.logger.error(
          `Error create power event: ${JSON.stringify(stakeEvent)}`
        );
        this.logger.error(error);
        throw error;
      }
    });

    await knex.transaction(async (trx) => {
      if (powerEvents.length > 0) {
        const chunkSize = config.handleStakeEvent.chunkSize || 5000;
        for (let i = 0; i < powerEvents.length; i += chunkSize) {
          const chunk = powerEvents.slice(i, i + chunkSize);
          await PowerEvent.query()
            .insert(chunk)
            .transacting(trx)
            .catch((error) => {
              this.logger.error(
                `Error insert validator's power events: ${JSON.stringify(
                  chunk
                )}`
              );
              this.logger.error(error);
            });
        }
      }

      updateBlockCheckpoint.height = endHeight;
      await BlockCheckpoint.query()
        .insert(updateBlockCheckpoint)
        .onConflict('job_name')
        .merge()
        .returning('id')
        .transacting(trx);
    });
  }

  public getRawAmountByFirstIndex(
    attributes: EventAttribute[],
    action: string,
    firstIndex: number
  ) {
    let indexRule;
    if (this.cosmosSdkVersion.compare('0.47.8') === -1) {
      indexRule = this.mapIndexAmountWithEventStakes['< 0.47.8'];
    } else {
      indexRule = this.mapIndexAmountWithEventStakes['>= 0.47.8'];
    }

    const amountRaw = attributes.find(
      (attr: any) =>
        attr.key === EventAttribute.ATTRIBUTE_KEY.AMOUNT &&
        attr.index === firstIndex + indexRule[action]
    );
    return amountRaw;
  }

  public async _start() {
    try {
      const lcdClient = await getLcdClient();
      if (!lcdClient?.provider) {
        this.logger.warn('LCD client not available during startup, skipping SDK version detection. Will retry later.');
      } else {
        // set version cosmos sdk to registry
        try {
          const nodeInfo: GetNodeInfoResponseSDKType =
            await lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo();
          if (nodeInfo.application_version?.cosmos_sdk_version) {
            this.cosmosSdkVersion = new SemVer(
              nodeInfo.application_version.cosmos_sdk_version
            );
          } else {
            this.logger.warn('Cannot find cosmos sdk version, using default');
          }
        } catch (error: any) {
          const errorMessage = error?.message || String(error);
          if (errorMessage.includes('timeout') || error?.code === 'ECONNABORTED') {
            this.logger.warn(`LCD client timeout during startup (non-critical): ${errorMessage}. Service will continue.`);
          } else {
            this.logger.warn(`Failed to get node info during startup (non-critical): ${errorMessage}. Service will continue.`);
          }
        }
      }
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      this.logger.warn(`LCD client initialization failed during startup (non-critical): ${errorMessage}. Service will continue.`);
    }

    this.createJob(
      BULL_JOB_NAME.HANDLE_STAKE_EVENT,
      BULL_JOB_NAME.HANDLE_STAKE_EVENT,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every: config.handleStakeEvent.millisecondCrawl,
        },
      }
    );

    return super._start();
  }
}
