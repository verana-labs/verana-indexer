import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import axios from 'axios';
import config from '../../../config.json' with { type: 'json' };
import BullableService, { QueueHandler } from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE } from '../../common';
import { BlockCheckpoint } from '../../models';
import knex from '../../common/utils/db_connection';

interface DidDocument {
    did: string;
    controller: string;
    created: string;
    modified: string;
    exp: string;
    deposit: string;
}

interface DidListResponse {
    dids: DidDocument[];
    pagination?: {
        next_key: string | null;
        total: string;
    };
}

@Service({
    name: SERVICE.V1.DidInitialCrawlerService.key,
    version: 1,
    dependencies: [SERVICE.V1.DidDatabaseService.key]
})
export default class DidInitialCrawlerService extends BullableService {
    private _apiEndpoint: string;

    private _batchSize: number;
   
    private _concurrency: number;
    
    private _initialSyncCompleted: boolean = false;

    public constructor(public broker: ServiceBroker) {
        super(broker);
        this._apiEndpoint = config.veranaApi || 'https://api.testnet.verana.network';
        this._batchSize = config.didCrawler?.batchSize || 50;
        this._concurrency = config.didCrawler?.concurrency || 5;
    }

    public async _start() {
        this.logger.info('Starting DID Initial Crawler Service...');
        await knex.raw('SELECT 1');
        await this.initializeCheckpoint();

        this.createJob(
            BULL_JOB_NAME.JOB_CRAWL_DID,
            BULL_JOB_NAME.JOB_CRAWL_DID,
            {},
            {
                repeat: { every: config.didCrawler?.pollInterval || 30000 },
                removeOnComplete: true,
                removeOnFail: { count: 3 }
            }
        );

        return super._start();
    }

    private async initializeCheckpoint(): Promise<void> {
        const existing = await BlockCheckpoint.query()
            .where('job_name', BULL_JOB_NAME.CP_CRAWL_DID)
            .first();

        if (!existing) {
            await BlockCheckpoint.query().insert({
                job_name: BULL_JOB_NAME.CP_CRAWL_DID,
                height: config.crawlBlock.startBlock || 0
            });
        }
    }

    @QueueHandler({
        queueName: BULL_JOB_NAME.JOB_CRAWL_DID,
        jobName: BULL_JOB_NAME.JOB_CRAWL_DID,
    })
    public async crawlDids(): Promise<void> {
        if (this._initialSyncCompleted) {
            this.logger.debug('Initial sync already completed, skipping');
            return;
        }

        const checkpoint = await BlockCheckpoint.query()
            .where('job_name', BULL_JOB_NAME.CP_CRAWL_DID)
            .first();

        if (!checkpoint) {
            this.logger.error('DID checkpoint not found');
            return;
        }

        this.logger.info(`Syncing DIDs from height ${checkpoint.height}`);
        let currentOffset = checkpoint.height;
        let hasMore = true;
        let processedAny = false;
        let emptyResponses = 0;
        const maxEmptyResponses = 3;

        try {
            while (hasMore && emptyResponses < maxEmptyResponses) {
                const response = await this.fetchDids(currentOffset, this._batchSize);
                const dids = response.dids || [];

                if (dids.length === 0) {
                    emptyResponses++;
                    this.logger.debug(`Empty response #${emptyResponses}`);
                    if (emptyResponses >= maxEmptyResponses) {
                        hasMore = false;
                        this._initialSyncCompleted = true;
                        this.logger.info('Initial sync completed (no more DIDs found)');
                    }
                    continue;
                }

                const existingDids = await knex('dids')
                    .whereIn('did', dids.map(d => d.did))
                    .select('did');
                const existingSet = new Set(existingDids.map(d => d.did));
                const newDids = dids.filter(d => !existingSet.has(d.did));

                if (newDids.length > 0) {
                    for (let i = 0; i < newDids.length; i += this._concurrency) {
                        const batch = newDids.slice(i, i + this._concurrency);
                        await Promise.all(batch.map(did => this.saveDid(did, i)));
                        processedAny = true;
                    }
                    this.logger.info(`Processed ${newDids.length} new DIDs`);
                }

                if (newDids.length > 0) {
                    currentOffset += dids.length;
                    await this.updateCheckpoint(currentOffset);
                    this.logger.debug(`Updated checkpoint to height ${currentOffset}`);
                } else {
                    this.logger.debug(`No new DIDs found, checkpoint not updated.`);
                }


                if (dids.length < this._batchSize) {
                    hasMore = false;
                    this._initialSyncCompleted = true;
                    this.logger.info('Initial sync completed (end of available DIDs)');
                }

                emptyResponses = 0;
            }
        } catch (error) {
            this.logger.error('DID sync failed:', error);
        }
    }

    private async fetchDids(offset: number, limit: number): Promise<DidListResponse> {
        const url = `${this._apiEndpoint}/verana/dd/v1/list?offset=${offset}&limit=${limit}`;
        const response = await axios.get(url, { timeout: 10000 });
        return response.data;
    }

    private async saveDid(didDoc: DidDocument, height: number): Promise<void> {
        try {
            await this.broker.call('v1.DidDatabaseService.upsert', {
                did: didDoc.did,
                height: height,
                controller: didDoc.controller,
                created: didDoc.created,
                modified: didDoc.modified,
                exp: didDoc.exp,
                deposit: didDoc.deposit
            });
        } catch (err) {
            this.logger.error(`Failed to save Did ${didDoc.did}:`, err);
            throw err;
        }
    }

    private async updateCheckpoint(height: number): Promise<void> {
        await BlockCheckpoint.query()
            .where('job_name', BULL_JOB_NAME.CP_CRAWL_DID)
            .patch({ height });
    }
}
