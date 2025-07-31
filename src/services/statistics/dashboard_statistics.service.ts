/* eslint-disable no-await-in-loop */
/* eslint-disable import/no-extraneous-dependencies */
import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import { DecCoinSDKType } from '@aura-nw/aurajs/types/codegen/cosmos/base/v1beta1/coin';
import BigNumber from 'bignumber.js';
import {
  BlockCheckpoint,
  Transaction,
  Validator,
  Statistic,
  StatisticKey,
} from '../../models';
import {
  BULL_JOB_NAME,
  IProviderJSClientFactory,
  REDIS_KEY,
  SERVICE,
  chainIdConfigOnServer,
  getLcdClient,
} from '../../common';
import config from '../../../config.json' with { type: 'json' };
import BullableService, { QueueHandler } from '../../base/bullable.service';

@Service({
  name: SERVICE.V1.DashboardStatisticsService.key,
  version: 1,
})
export default class DashboardStatisticsService extends BullableService {
  private _lcdClient!: IProviderJSClientFactory;

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  /**
   * @description: update statistic transaction and return current total transaction counted
   * @private
   */
  private async statisticTotalTransaction(): Promise<number> {
    // Select and make sure that have statistic
    const totalTxStatistic: Statistic | undefined =
      await Statistic.query().findOne('key', StatisticKey.TotalTransaction);

    // Count transaction and get max height from height to height
    const crawlTxJobInfo = await BlockCheckpoint.query().findOne(
      'job_name',
      BULL_JOB_NAME.HANDLE_TRANSACTION
    );
    if (!crawlTxJobInfo) return 0;

    if (!totalTxStatistic) {
      const transactionsInfo = await Transaction.query()
        .where('height', '<=', crawlTxJobInfo.height)
        .count();
      this.logger.warn(transactionsInfo, 'transactionsInfo');
      const totalTransaction = transactionsInfo ? transactionsInfo[0].count : 0;
      await Statistic.query().insert({
        key: StatisticKey.TotalTransaction,
        value: totalTransaction,
        statistic_since: `${crawlTxJobInfo.height}`,
      });
      return totalTransaction;
    }
    let totalTx = Number(totalTxStatistic?.value);

    // Count tx and find max height determine by range of statistic
    const fromHeight = Number(totalTxStatistic?.statistic_since);
    const toHeight = crawlTxJobInfo.height;

    if (fromHeight >= toHeight) return totalTx;

    const txStatistic = await Transaction.query()
      .where('height', '>', fromHeight)
      .andWhere('height', '<=', toHeight)
      .count();

    // If having new tx, then update total tx and update counter since for next time statistic
    if (txStatistic[0]) {
      totalTx += Number(txStatistic[0].count);
      await Statistic.query()
        .update(
          Statistic.fromJson({
            key: StatisticKey.TotalTransaction,
            value: totalTx,
            statistic_since: toHeight,
          })
        )
        .where({
          key: StatisticKey.TotalTransaction,
        });
    }

    return totalTx;
  }

  private async getStatistic(): Promise<{
    communityPool: any;
    inflation: any;
    distribution: any;
    supply: any;
  }> {
    let communityPool;
    let inflation;
    let distribution;
    let supply;
    switch (config.chainId) {
      case chainIdConfigOnServer.Atlantic2:
      case chainIdConfigOnServer.Pacific1:
      case chainIdConfigOnServer.Evmos90004:
        [communityPool, supply] = await Promise.all([
          this._lcdClient.provider.cosmos.distribution.v1beta1.communityPool(),
          this._lcdClient.provider.cosmos.bank.v1beta1.supplyOf({
            denom: config.networkDenom,
          }),
        ]);
        break;
      case chainIdConfigOnServer.Euphoria:
      case chainIdConfigOnServer.SerenityTestnet001:
      case chainIdConfigOnServer.AuraTestnetEVM:
      case chainIdConfigOnServer.Xstaxy1:
      default:
        [communityPool, inflation, distribution, supply] = await Promise.all([
          this._lcdClient.provider.cosmos.distribution.v1beta1.communityPool(),
          this._lcdClient.provider.cosmos.mint.v1beta1.inflation(),
          this._lcdClient.provider.cosmos.distribution.v1beta1.params(),
          this._lcdClient.provider.cosmos.bank.v1beta1.supplyOf({
            denom: config.networkDenom,
          }),
        ]);
        break;
    }
    return {
      communityPool,
      supply,
      inflation,
      distribution,
    };
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.HANDLE_DASHBOARD_STATISTICS,
    jobName: BULL_JOB_NAME.HANDLE_DASHBOARD_STATISTICS,
    // prefix: `horoscope-v2-${config.chainId}`,
  })
  public async handleJob(_payload: object): Promise<void> {
    this.logger.info('Update AuraScan dashboard statistics');
    this._lcdClient = await getLcdClient();

    const [totalBlocks, totalTxs, totalValidators] = await Promise.all([
      BlockCheckpoint.query().findOne(
        'job_name',
        BULL_JOB_NAME.HANDLE_TRANSACTION
      ),
      this.statisticTotalTransaction(),
      Validator.query(),
    ]);

    const { communityPool, inflation, distribution, supply } =
      await this.getStatistic();
    let bondedTokens = BigInt(0);
    totalValidators
      .filter(
        (val) => val.status === Validator.STATUS.BONDED && val.jailed === false
      )
      .forEach((val) => {
        bondedTokens += BigInt(val.tokens);
      });
    const totalAura = supply?.amount?.amount;

    const dashboardStatistics = {
      total_blocks: totalBlocks?.height,
      community_pool: communityPool.pool.find(
        (pool: DecCoinSDKType) => pool.denom === config.networkDenom
      )?.amount || '0',
      total_transactions: Number(totalTxs),
      total_validators: totalValidators.length,
      total_active_validators: totalValidators.filter(
        (val) => val.status === Validator.STATUS.BONDED
      ).length,
      total_inactive_validators: totalValidators.filter(
        (val) => val.status === Validator.STATUS.UNBONDED
      ).length,
      bonded_tokens: bondedTokens.toString(),
      inflation: inflation ? inflation.inflation : 0,
      total_aura: totalAura,
      staking_apr: inflation
        ? Number(
          BigNumber(inflation.inflation)
            .multipliedBy(
              BigNumber(1 - Number(distribution.params.community_tax))
            )
            .multipliedBy(BigNumber(totalAura))
            .dividedBy(BigNumber(bondedTokens.toString()))
            .multipliedBy(100)
        )
        : 0,
    };

    await this.broker.cacher?.set(
      REDIS_KEY.DASHBOARD_STATISTICS,
      dashboardStatistics
    );
  }

  public async _start() {
    this.createJob(
      BULL_JOB_NAME.HANDLE_DASHBOARD_STATISTICS,
      BULL_JOB_NAME.HANDLE_DASHBOARD_STATISTICS,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every: config.dashboardStatistics.millisecondCrawl,
        },
      }
    );

    return super._start();
  }
}
