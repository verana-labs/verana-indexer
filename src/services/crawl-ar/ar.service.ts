import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import config from '../../config.json' with { type: 'json' };
import { Network } from '../../network';
import BullableService from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE } from '../../common';
import knex from '../../common/utils/db_connection';
import { Block } from '../../models';
import { Account } from '../../models/account';
import { BlockCheckpoint } from '../../models/block_checkpoint';
import { detectStartMode } from '../../common/utils/start_mode_detector';
import { getDbQueryTimeoutMs } from '../../common/utils/db_query_helper';
import { applySpeedToDelay, applySpeedToBatchSize, getCrawlSpeedMultiplier } from '../../common/utils/crawl_speed_config';
import { tableExists, isTableMissingError } from '../../common/utils/db_health';
import { indexerStatusManager } from '../manager/indexer_status.manager';
import { throwIfHeapCriticalDuringCrawl } from '../../common/utils/memory_crawl_guard';
import { isValidAccountAddress, fetchAccountBalance } from '../../common/utils/account_balance_utils';

interface Balance {
    denom: string;
    amount: string;
}

interface AccountInsertData {
    address: string;
    type: string;
    balances: unknown[];
    spendable_balances: unknown[];
    account_number: number;
    sequence: number;
    created_at: string;
    [key: string]: unknown;
}

@Service({
    name: SERVICE.V1.HANDLE_ACCOUNTS.key,
    version: 1,
})
export default class CrawlNewAccountsService extends BullableService {
    private timer: NodeJS.Timeout | null = null;
    private isProcessingBlocks = false;
    private readonly JOB_NAME = BULL_JOB_NAME.JOB_HANDLE_ACCOUNTS;
    private BATCH_SIZE = config?.crawlAccounts?.freshStart?.chunkSize || 100;
    private CRAWL_INTERVAL = config?.crawlAccounts?.freshStart?.millisecondCrawl || 10000;
    private ENABLE_RECONCILE = (config as any)?.crawlAccounts?.reconcile?.enabled || false;
    private accountCache = new Map<string, Account>();
    private cacheSize = 1000;
    private _isFreshStart: boolean = false;

    private readonly ACCOUNT_INSERT_BATCH = Math.min(500, Math.max(50, Number(process.env.ACCOUNT_INSERT_BATCH) || 200));
    private readonly ACCOUNT_EXISTING_QUERY_CHUNK = Math.min(5000, Math.max(500, Number(process.env.ACCOUNT_EXISTING_QUERY_CHUNK) || 2000));
    private readonly BALANCE_REFRESH_CONCURRENCY = Math.min(20, Math.max(2, Number(process.env.ACCOUNT_BALANCE_CONCURRENCY) || 8));
    private readonly BALANCE_REFRESH_CHUNK = Math.min(1000, Math.max(50, Number(process.env.ACCOUNT_BALANCE_CHUNK) || 200));
    private readonly BALANCE_FETCH_RETRIES = Math.min(5, Math.max(0, Number(process.env.ACCOUNT_BALANCE_FETCH_RETRIES) || 2));
    private readonly BALANCE_FETCH_BACKOFF_MS = Math.min(5000, Math.max(100, Number(process.env.ACCOUNT_BALANCE_BACKOFF_MS) || 400));
    private readonly DELAY_BETWEEN_BALANCE_CHUNKS_MS = Math.min(500, Math.max(0, Number(process.env.ACCOUNT_BALANCE_CHUNK_DELAY_MS) || 50));

    constructor(public broker: ServiceBroker) {
        super(broker);
    }

    async started() {
        try {

            const genesisCheckpoint = await BlockCheckpoint.query().findOne({
                job_name: BULL_JOB_NAME.CRAWL_GENESIS,
            });
            
            if (!genesisCheckpoint || genesisCheckpoint.height !== 1) {
                this.logger.warn(`[CrawlNewAccountsService]  Waiting for genesis crawl to complete. Current genesis height: ${genesisCheckpoint?.height || 0}. Will retry in 10 seconds...`);

                setTimeout(() => this.started(), 10000);
                return;
            }

            this.logger.info(`[CrawlNewAccountsService] Genesis crawl completed. Starting account service...`);

            const checkpoint = await this.ensureCheckpoint();
            const startMode = await detectStartMode();
            this._isFreshStart = startMode.isFreshStart;

            this.logger.info(`[CrawlNewAccountsService] Starting - Block count: ${startMode.totalBlocks}, Current checkpoint: ${startMode.currentBlock}, Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`);

            if (this._isFreshStart && config.crawlAccounts.freshStart) {
                const baseBatchSize = config.crawlAccounts.freshStart.chunkSize || 100;
                const baseInterval = config.crawlAccounts.freshStart.millisecondCrawl || 1000;
                this.BATCH_SIZE = applySpeedToBatchSize(baseBatchSize, false);
                this.CRAWL_INTERVAL = applySpeedToDelay(baseInterval, false);
            } else if (!this._isFreshStart && config.crawlAccounts.reindexing) {
                const baseBatchSize = config.crawlAccounts.reindexing.chunkSize || 1000;
                const baseInterval = config.crawlAccounts.reindexing.millisecondCrawl || 1000;
                this.BATCH_SIZE = applySpeedToBatchSize(baseBatchSize, true);
                this.CRAWL_INTERVAL = applySpeedToDelay(baseInterval, true);
            }

            const speedMultiplier = getCrawlSpeedMultiplier(!this._isFreshStart);
            this.logger.info(`[CrawlNewAccountsService] Config - Batch: ${this.BATCH_SIZE}, Interval: ${this.CRAWL_INTERVAL}ms, Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'} | Speed Multiplier: ${speedMultiplier}x`);


            await this.processBlocks();


            this.timer = setInterval(
                () => this.processBlocks(),
                this.CRAWL_INTERVAL
            );
        } catch (err) {
            this.logger.error(`[CrawlNewAccountsService] Startup error:`, err);
        }
    }

    async stopped() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        await this.flushAccountBatch();
        if (this.accountCreateTimeout) {
            clearTimeout(this.accountCreateTimeout);
            this.accountCreateTimeout = null;
        }
        this.accountCache.clear();
    }

    private async ensureCheckpoint() {
        const { CheckpointManager } = await import('../../common/utils/checkpoint_manager');
        const checkpointManager = new CheckpointManager(this.logger);
        return await checkpointManager.ensureCheckpoint(this.JOB_NAME, 0);
    }

    private async updateCheckpoint(newHeight: number) {
        try {
            const { CheckpointManager } = await import('../../common/utils/checkpoint_manager');
            const checkpointManager = new CheckpointManager(this.logger);
            await checkpointManager.updateCheckpoint(this.JOB_NAME, newHeight);
        } catch (err) {
            this.logger.error(`[CrawlNewAccountsService] Error updating checkpoint to height ${newHeight}:`, err);
            throw err;
        }
    }

    @Action({ name: 'processBlocks' })
    async processBlocks() {
        if (this.isProcessingBlocks) {
            this.logger.debug('[CrawlNewAccountsService] processBlocks skipped (previous run still in progress)');
            return;
        }
        this.isProcessingBlocks = true;
        try {
            if (!indexerStatusManager.isCrawlingActive()) {
                this.logger.debug('[CrawlNewAccountsService] Crawling is stopped, skipping account processing cycle');
                return;
            }

            const checkpoint = await BlockCheckpoint.query(knex).findOne({ job_name: this.JOB_NAME });
            let lastHeight = checkpoint ? checkpoint.height : 0;
            let totalBlocksProcessed = 0;

            this.logger.info(`[CrawlNewAccountsService] Processing blocks starting from height: ${lastHeight}`);

            while (true) {
                if (!indexerStatusManager.isCrawlingActive()) {
                    this.logger.warn('[CrawlNewAccountsService] Crawling stopped while processing. Exiting current cycle.');
                    break;
                }
                await throwIfHeapCriticalDuringCrawl('crawl-new-accounts:loop', this.logger);
                let nextBlocks: Block[];
                try {
                    nextBlocks = await Block.query()
                        .where('height', '>', lastHeight)
                        .orderBy('height', 'asc')
                        .limit(this.BATCH_SIZE)
                        .timeout(getDbQueryTimeoutMs(120000));
                } catch (queryError: any) {
                    const errorCode = queryError?.code;
                    const errorMessage = queryError?.message || String(queryError);
                    
                    if (errorCode === '57014' || 
                        errorMessage.includes('statement timeout') || 
                        errorMessage.includes('canceling statement') ||
                        errorMessage.includes('query timeout')) {
                        this.logger.warn(`[CrawlNewAccountsService] Query timeout at height ${lastHeight}, waiting before retry...`);
                        const { delay } = await import('../../common/utils/db_query_helper');
                        await delay(5000);
                        continue;
                    }
                    throw queryError;
                }

                if (!nextBlocks.length) {
                    if (totalBlocksProcessed > 0) {
                        this.logger.info(`[CrawlNewAccountsService] Processed ${totalBlocksProcessed} blocks, reached height: ${lastHeight}`);
                    }
                    break;
                }

                try {
                    const changedAddresses = new Set<string>();
                    let batchTransfers = 0;
                    let batchSuccess = true;
                    let stoppedMidBatch = false;

                    const batchStartHeight = nextBlocks[0].height;
                    const batchEndHeight = nextBlocks[nextBlocks.length - 1].height;

                    for (const block of nextBlocks) {
                        if (!indexerStatusManager.isCrawlingActive()) {
                            this.logger.warn('[CrawlNewAccountsService] Crawling stopped mid-batch. Exiting current cycle.');
                            batchSuccess = false;
                            stoppedMidBatch = true;
                            break;
                        }
                        const result = await this.processBlockTransactions(block, changedAddresses);
                        batchSuccess = batchSuccess && result.success;
                        batchTransfers += result.transfersProcessed;
                        if (!result.success) {
                            this.logger.error(`[CrawlNewAccountsService] Failed processing block ${block.height}${result.errorMessage ? `: ${result.errorMessage}` : ''}`);
                            break;
                        }
                    }

                    if (!batchSuccess) {
                        if (stoppedMidBatch) {
                            this.logger.warn(`[CrawlNewAccountsService] Batch interrupted because crawling is paused (up to height ${batchEndHeight})`);
                        } else {
                            this.logger.error(`[CrawlNewAccountsService] Batch processing failed at height ${batchEndHeight}`);
                        }
                        break;
                    }

                    if (this.ENABLE_RECONCILE && Network?.LCD) {
                        await this.reconcileChangedAddresses(Array.from(changedAddresses), batchEndHeight);
                    }

                    await this.flushAccountBatch();

                    totalBlocksProcessed += nextBlocks.length;
                    const newHeight = batchEndHeight;
                    await this.updateCheckpoint(newHeight);
                    lastHeight = newHeight;

                    if (this.accountCache.size > this.cacheSize * 0.8) {
                        const entries = Array.from(this.accountCache.entries());
                        entries.slice(0, Math.floor(this.accountCache.size * 0.3)).forEach(([key]) => {
                            this.accountCache.delete(key);
                        });
                    }

                    if (totalBlocksProcessed % 100 === 0 && global.gc) {
                        global.gc();
                    }

                    this.logger.info(`[CrawlNewAccountsService] ✅ Processed batch: heights ${batchStartHeight}-${batchEndHeight} (${nextBlocks.length} blocks, ${batchTransfers} transfers), checkpoint updated to ${newHeight}`);

                    if (nextBlocks.length < this.BATCH_SIZE) {
                        this.logger.info(`[CrawlNewAccountsService] Reached end of available blocks at height: ${newHeight}`);
                        break;
                    }

                    const processDelay = this._isFreshStart ? 1000 : 200;
                    if (processDelay > 0) {
                        const { delay } = await import('../../common/utils/db_query_helper');
                        await delay(processDelay);
                    }
                } catch (err: any) {
                    if (err?.name === 'CrawlSkipError') {
                        this.logger.warn(`[CrawlNewAccountsService] Skipping cycle: ${err?.message || 'memory pressure or crawler paused'}`);
                        break;
                    }
                    if (err?.code === '57014' || err?.message?.includes('statement timeout') || err?.message?.includes('canceling statement')) {
                        this.logger.warn(`[CrawlNewAccountsService] Statement timeout in batch processing at height ${lastHeight + 1}, waiting before retry...`);
                        const { delay } = await import('../../common/utils/db_query_helper');
                        await delay(5000);
                        continue;
                    }
                    this.logger.error(`[CrawlNewAccountsService] Error processing batch starting at height ${lastHeight + 1}:`, err);
                    break;
                }
            }

            if (totalBlocksProcessed === 0) {
                this.logger.debug(`[CrawlNewAccountsService] No new blocks to process (current checkpoint: ${lastHeight})`);
            }
        } catch (err) {
            if ((err as any)?.name === 'CrawlSkipError') {
                this.logger.warn(`[CrawlNewAccountsService] Skipping cycle: ${(err as any)?.message || 'memory pressure or crawler paused'}`);
                return;
            }
            this.logger.error(`[CrawlNewAccountsService] Error in processBlocks:`, err);
        } finally {
            this.isProcessingBlocks = false;
        }
    }

    private parseAmountString(amountStr: string): Balance[] {
        if (!amountStr) return [];
        const parts = amountStr.split(',').map(p => p.trim()).filter(Boolean);
        const results: Balance[] = [];

        for (const p of parts) {
            const m = p.match(/^(\d+(?:\.\d+)?)([a-zA-Z][a-zA-Z0-9]*)$/);
            if (!m) continue;
            const num = m[1];
            const denom = m[2];
            const intStr = num.includes('.') ? num.split('.')[0] : num;
            if (intStr === '' || /^0+$/.test(intStr)) continue;
            results.push({ denom, amount: intStr });
        }
        return results;
    }

    private async processBlockTransactions(block: Block, changedAddresses: Set<string>): Promise<{ success: boolean; transfersProcessed: number; errorMessage?: string }> {
        try {
            const blockData = typeof block.data === 'string' ? JSON.parse(block.data) : block.data;
            const blockResult = blockData?.block_result;
            if (!blockResult) return { success: true, transfersProcessed: 0 };

            const transferEvents: Array<{ type: string; attributes?: Array<{ key: string; value: string }>; source: string }> = [];

            const extractEvents = (events: unknown[], source: string) => {
                if (Array.isArray(events)) {
                    for (const event of events) {
                        const evt = event as { type?: string };
                        if (evt?.type === 'transfer') {
                            transferEvents.push({ ...(event as { type: string; attributes?: Array<{ key: string; value: string }> }), source });
                        }
                    }
                }
            };

            extractEvents(blockResult.finalize_block_events, 'finalize');
            extractEvents(blockResult.end_block_events, 'finalize');

            if (Array.isArray(blockResult.txs_results)) {
                for (const tx of blockResult.txs_results) {
                    if (!tx || tx.code !== 0) continue;
                    extractEvents(tx.events, 'tx');
                }
            }

            let processed = 0;

            for (const event of transferEvents) {
                try {
                    const attrs = Object.fromEntries(
                        (event.attributes || []).map((a: { key: string; value: string }) => [a.key, a.value])
                    ) as Record<string, string>;

                    const sender = attrs.sender;
                    const recipient = attrs.recipient;
                    const amountStr = attrs.amount || '';

                    if (!amountStr) continue;

                    const parsed = this.parseAmountString(amountStr);
                    if (!parsed.length) continue;

                    if (sender) {
                        await this.ensureAccountExists(sender);
                        await this.decreaseBalance(sender, parsed);
                        changedAddresses.add(sender);
                        processed++;
                    }

                    if (recipient) {
                        await this.ensureAccountExists(recipient);
                        await this.saveOrUpdateAccount(recipient, parsed, block.height);
                        changedAddresses.add(recipient);
                        processed++;
                    }
                } catch (err) {
                    const errorMsg = (err as any)?.message || String(err);
                    this.logger.error(`[TRANSFER] Failed: ${errorMsg}`);
                }
            }

            return { success: true, transfersProcessed: processed };
        } catch (err: any) {
            const errorMsg = err?.message || String(err);
            this.logger.error(`[PROCESS] Error block ${block.height}: ${errorMsg}`);
            return { success: false, transfersProcessed: 0, errorMessage: err?.message || String(err) };
        }
    }

    private pendingAccountCreates = new Set<string>();
    private accountCreateBatch: Array<{ address: string; accountData: AccountInsertData }> = [];
    private accountCreateTimeout: NodeJS.Timeout | null = null;
    private readonly ACCOUNT_BATCH_SIZE = 50;
    private readonly ACCOUNT_BATCH_DELAY = 100; // ms

    private async ensureAccountExists(address: string): Promise<void> {
        if (!address) return;

        const existing = await this.getAccountFromCacheOrDB(address);
        if (existing) return;

        if (this.pendingAccountCreates.has(address)) return;
        this.pendingAccountCreates.add(address);

        const accountData: AccountInsertData = {
            address,
            type: 'user-accounts',
            balances: [],
            spendable_balances: [],
            account_number: 0,
            sequence: 0,
            created_at: new Date().toISOString(),
        };

        this.accountCreateBatch.push({ address, accountData });

        if (this.accountCreateBatch.length >= this.ACCOUNT_BATCH_SIZE) {
            await this.flushAccountBatch();
        } else {
            if (this.accountCreateTimeout) {
                clearTimeout(this.accountCreateTimeout);
            }
            this.accountCreateTimeout = setTimeout(() => {
                this.flushAccountBatch().catch(err => {
                    this.logger.error('[ENSURE_ACCOUNT] Batch flush error:', err);
                });
            }, this.ACCOUNT_BATCH_DELAY);
        }
    }

    private async flushAccountBatch(): Promise<void> {
        if (this.accountCreateBatch.length === 0) return;

        const batch = this.accountCreateBatch.splice(0, this.ACCOUNT_BATCH_SIZE);
        const addresses = batch.map(b => b.address);
        
        if (this.accountCreateTimeout) {
            clearTimeout(this.accountCreateTimeout);
            this.accountCreateTimeout = null;
        }

        try {
            if (!(await tableExists("account"))) {
                this.logger.warn("[ENSURE_ACCOUNT] Account table does not exist yet, waiting for migrations...");
                return;
            }

            const existing = await Account.query(knex)
                .whereIn('address', addresses)
                .select('address');
            const existingSet = new Set(existing.map(a => a.address));
            
            const toCreate = batch.filter(b => !existingSet.has(b.address));
            
            if (toCreate.length > 0) {
                const accountDataList = toCreate.map(b => b.accountData);
                try {
                    await Account.query(knex).insert(accountDataList);
                } catch (err: unknown) {
                    const e = err as { nativeError?: { code?: string; column?: string } };
                    if (e?.nativeError?.code === '42703' && e?.nativeError?.column === 'pub_key') {
                        const retryData = accountDataList.map((data) => {
                            const rest = { ...data };
                            delete rest.pub_key;
                            return rest;
                        });
                        await Account.query(knex).insert(retryData);
                    } else if (isTableMissingError(err)) {
                        this.logger.warn("[ENSURE_ACCOUNT] Account table does not exist yet, waiting for migrations...");
                    } else {
                        throw err;
                    }
                }
                try {
                    await this.doBulkRefreshAccountBalances(toCreate.map(b => b.address));
                } catch {
                    //
                }
            }
        } catch (err: unknown) {
            const e = err as { code?: string; nativeError?: { code?: string } };
            if (isTableMissingError(err)) {
                this.logger.warn("[ENSURE_ACCOUNT] Account table does not exist yet, waiting for migrations...");
            } else if (e?.code === '23505' || e?.nativeError?.code === '23505') {
                // Duplicate key error - ignore
            } else {
                this.logger.error(`[ENSURE_ACCOUNT] Batch insert error:`, err);
            }
        } finally {
            addresses.forEach(addr => {
                this.pendingAccountCreates.delete(addr);
                this.accountCache.delete(addr);
            });
        }
    }

    @Action({ name: 'upsertAccount', params: { address: { type: 'string', min: 5 } } })
    async upsertAccount(ctx: { params: { address: string } }): Promise<{ success: boolean; created?: boolean }> {
        const address = String(ctx.params.address || '').trim();
        if (!address || !isValidAccountAddress(address)) {
            return { success: false };
        }
        try {
            if (!(await tableExists('account'))) {
                return { success: false };
            }
            const existing = await Account.query(knex).findOne({ address }).select('address');
            if (existing) {
                return { success: true, created: false };
            }
            const accountData: AccountInsertData = {
                address,
                type: 'user-accounts',
                balances: [],
                spendable_balances: [],
                account_number: 0,
                sequence: 0,
                created_at: new Date().toISOString(),
            };
            await Account.query(knex).insert(accountData);
            try {
                const balances = await fetchAccountBalance(address);
                if (balances !== null) {
                    await Account.query(knex)
                        .patch({
                            balances: balances as any,
                            spendable_balances: balances as any,
                            updated_at: new Date().toISOString(),
                        })
                        .where({ address });
                    this.accountCache.delete(address);
                }
            } catch {
                //
            }
            return { success: true, created: true };
        } catch (err: unknown) {
            const e = err as { code?: string; nativeError?: { code?: string } };
            if (e?.code === '23505' || e?.nativeError?.code === '23505') {
                return { success: true, created: false };
            }
            this.logger.warn('[upsertAccount] Failed:', err);
            return { success: false };
        }
    }

    @Action({ name: 'bulkEnsureAccounts', params: { addresses: { type: 'array', items: 'string' } } })
    async bulkEnsureAccounts(ctx: { params: { addresses: string[] } }): Promise<{ success: boolean; inserted: number }> {
        const raw = ctx.params.addresses;
        if (!Array.isArray(raw) || raw.length === 0) return { success: true, inserted: 0 };
        const unique = [...new Set(raw.map((a) => String(a).trim()).filter((a) => a.length > 0 && isValidAccountAddress(a)))];
        if (unique.length === 0) return { success: true, inserted: 0 };
        try {
            if (!(await tableExists('account'))) return { success: false, inserted: 0 };
            const existingSet = new Set<string>();
            for (let q = 0; q < unique.length; q += this.ACCOUNT_EXISTING_QUERY_CHUNK) {
                const qChunk = unique.slice(q, q + this.ACCOUNT_EXISTING_QUERY_CHUNK);
                const rows = await Account.query(knex).whereIn('address', qChunk).select('address');
                rows.forEach((r) => existingSet.add(r.address));
            }
            const toInsert = unique.filter((a) => !existingSet.has(a));
            if (toInsert.length === 0) return { success: true, inserted: 0 };
            const { delay } = await import('../../common/utils/db_query_helper');
            let inserted = 0;
            for (let i = 0; i < toInsert.length; i += this.ACCOUNT_INSERT_BATCH) {
                const chunk = toInsert.slice(i, i + this.ACCOUNT_INSERT_BATCH);
                const accountDataList: AccountInsertData[] = chunk.map((address) => ({
                    address,
                    type: 'user-accounts',
                    balances: [],
                    spendable_balances: [],
                    account_number: 0,
                    sequence: 0,
                    created_at: new Date().toISOString(),
                }));
                let done = false;
                for (let attempt = 0; attempt <= 1 && !done; attempt++) {
                    try {
                        if (attempt > 0) await delay(this.BALANCE_FETCH_BACKOFF_MS);
                        await Account.query(knex).insert(accountDataList);
                        inserted += chunk.length;
                        done = true;
                    } catch (err: unknown) {
                        const e = err as { code?: string; nativeError?: { code?: string; column?: string } };
                        if (e?.nativeError?.code === '42703' && e?.nativeError?.column === 'pub_key') {
                            const retry = accountDataList.map((d) => {
                                const row = { ...(d as Record<string, unknown>) };
                                delete row.pub_key;
                                return row;
                            });
                            await Account.query(knex).insert(retry);
                            inserted += chunk.length;
                            done = true;
                        } else if (e?.code === '23505' || e?.nativeError?.code === '23505') {
                            inserted += chunk.length;
                            done = true;
                        } else if (attempt === 1) throw err;
                    }
                }
            }
            if (toInsert.length > 0) {
                try {
                    await this.doBulkRefreshAccountBalances(toInsert);
                } catch {
                    //
                }
            }
            return { success: true, inserted };
        } catch (err: unknown) {
            this.logger.warn('[bulkEnsureAccounts] Failed:', err);
            return { success: false, inserted: 0 };
        }
    }

    private async getAccountFromCacheOrDB(address: string): Promise<Account | null> {
        if (this.accountCache.has(address)) {
            return this.accountCache.get(address)!;
        }

        try {
            if (!(await tableExists("account"))) {
                return null;
            }

            const account = await Account.query(knex).findOne({ address });
            if (account && this.accountCache.size < this.cacheSize) {
                this.accountCache.set(address, account);
            }
            return account || null;
        } catch (err: unknown) {
            if (isTableMissingError(err)) {
                return null;
            }
            this.logger.error(`[TRANSFER] Failed:`, err);
            return null;
        }
    }

    async saveOrUpdateAccount(address: string, msgAmount: Balance[], height: number) {
        if (!address) {
            this.logger.warn('Address is required');
            return;
        }

        await this.ensureAccountExists(address);

        const existing = await this.getAccountFromCacheOrDB(address);
        const balances = msgAmount.map(a => ({
            denom: a.denom || 'uvna',
            amount: a.amount || '0',
        }));

        if (!balances.length) {
            return;
        }

        const updatedBalances = this.combineBalances(existing?.balances || [], balances);
        try {
            if (!(await tableExists("account"))) {
                this.logger.warn("[SAVE_OR_UPDATE] Account table does not exist yet, waiting for migrations...");
                return;
            }

            await Account.query(knex)
                .patch({
                    balances: updatedBalances,
                    spendable_balances: updatedBalances,
                    updated_at: new Date().toISOString(),
                })
                .where({ address });

            this.accountCache.delete(address);
        } catch (err: unknown) {
            if (isTableMissingError(err)) {
                this.logger.warn("[SAVE_OR_UPDATE] Account table does not exist yet, waiting for migrations...");
                return;
            }
            throw err;
        }
    }

    async decreaseBalance(address: string, amountArray: Balance[]) {
        if (!address) return;

        await this.ensureAccountExists(address);

        const acc = await this.getAccountFromCacheOrDB(address);
        if (!acc) return;

        const baseBalances = [...(acc.balances || [])];
        const newBalances = [...baseBalances];

        for (const amountObj of amountArray) {
            const denom = amountObj.denom || 'uvna';
            const amt = BigInt(amountObj.amount || '0');
            const idx = newBalances.findIndex((b: any) => b.denom === denom);

            if (idx >= 0) {
                const remaining = BigInt(newBalances[idx].amount) - amt;
                newBalances[idx].amount = remaining > 0 ? remaining.toString() : '0';
            } else {
                newBalances.push({ denom, amount: '0' });
            }
        }

        try {
            if (!(await tableExists("account"))) {
                this.logger.warn("[DECREASE_BALANCE] Account table does not exist yet, waiting for migrations...");
                return;
            }

            await Account.query(knex)
                .patch({
                    balances: newBalances,
                    spendable_balances: newBalances,
                    updated_at: new Date().toISOString(),
                })
                .where({ address });

            this.accountCache.delete(address);
        } catch (err: unknown) {
            if (isTableMissingError(err)) {
                this.logger.warn("[DECREASE_BALANCE] Account table does not exist yet, waiting for migrations...");
                return;
            }
            throw err;
        }
    }

    private combineBalances(oldBalances: any[], newBalances: any[]) {
        const combined: Record<string, bigint> = {};
        for (const b of oldBalances || []) {
            combined[b.denom] = (combined[b.denom] || BigInt(0)) + BigInt(b.amount);
        }
        for (const b of newBalances || []) {
            combined[b.denom] = (combined[b.denom] || BigInt(0)) + BigInt(b.amount);
        }
        return Object.entries(combined).map(([denom, amount]) => ({ denom, amount: amount.toString() }));
    }

    private async reconcileChangedAddresses(addresses: string[], height: number) {
        const lcd: string | undefined = Network?.LCD;
        if (!lcd || !addresses.length) return;


        const batchSize = 20; // Increased from 10 for faster processing
        const uniqueAddresses = Array.from(new Set(addresses)); // Remove duplicates
        
        for (let i = 0; i < uniqueAddresses.length; i += batchSize) {
            const batch = uniqueAddresses.slice(i, i + batchSize);
            const promises = batch.map(address => this.reconcileSingleAddress(address, height, lcd));
            await Promise.allSettled(promises);
            

            if (i + batchSize < uniqueAddresses.length) {
                const { delay } = await import('../../common/utils/db_query_helper');
                await delay(50);
            }
        }
        
        if (uniqueAddresses.length > 0) {
            this.logger.debug(`[CrawlNewAccountsService] Reconciled ${uniqueAddresses.length} addresses at height ${height}`);
        }
    }

    private async reconcileSingleAddress(address: string, height: number, lcd: string) {
        try {
            const url = `${lcd.replace(/\/$/, '')}/cosmos/bank/v1beta1/balances/${address}`;
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000); // 3 second timeout
            
            const res = await fetch(url, { signal: controller.signal });
            clearTimeout(timeoutId);
            
            if (!res.ok) return;

            const data: any = await res.json();
            const onChainBalances: Balance[] = Array.isArray(data?.balances)
                ? data.balances.map((b: any) => ({ denom: b.denom, amount: b.amount || '0' }))
                : [];
            await Account.query(knex)
                .patch({
                    balances: onChainBalances as any,
                    spendable_balances: onChainBalances as any,
                    updated_at: new Date().toISOString(),
                })
                .where({ address });

            this.accountCache.delete(address);
        } catch (err: any) {

            if (err.name !== 'AbortError') {
                this.logger.error(`[RECONCILE] Error ${address}:`, err);
            }
        }
    }

    @Action({ name: 'updateAccountBalanceFromChain', params: { address: { type: 'string', min: 5 } } })
    async updateAccountBalanceFromChain(ctx: { params: { address: string } }): Promise<{ success: boolean }> {
        const address = String(ctx.params.address || '').trim();
        if (!address || !isValidAccountAddress(address)) return { success: false };
        try {
            if (!(await tableExists('account'))) return { success: false };
            const balances = await fetchAccountBalance(address);
            if (balances === null) return { success: false };
            await Account.query(knex)
                .patch({
                    balances: balances as any,
                    spendable_balances: balances as any,
                    updated_at: new Date().toISOString(),
                })
                .where({ address });
            this.accountCache.delete(address);
            return { success: true };
        } catch (err: unknown) {
            this.logger.warn('[updateAccountBalanceFromChain] Failed:', err);
            return { success: false };
        }
    }

    private async doBulkRefreshAccountBalances(addresses: string[]): Promise<{ updated: number }> {
        if (addresses.length === 0) return { updated: 0 };
        const unique = [...new Set(addresses.filter((a) => isValidAccountAddress(String(a).trim())))];
        if (unique.length === 0) return { updated: 0 };
        if (!(await tableExists('account'))) return { updated: 0 };
        const { delay } = await import('../../common/utils/db_query_helper');
        let updated = 0;
        for (let outer = 0; outer < unique.length; outer += this.BALANCE_REFRESH_CHUNK) {
            const chunk = unique.slice(outer, outer + this.BALANCE_REFRESH_CHUNK);
            for (let i = 0; i < chunk.length; i += this.BALANCE_REFRESH_CONCURRENCY) {
                const batch = chunk.slice(i, i + this.BALANCE_REFRESH_CONCURRENCY);
                const results = await Promise.allSettled(
                    batch.map(async (address) => {
                        let balances: { denom: string; amount: string }[] | null = null;
                        for (let r = 0; r <= this.BALANCE_FETCH_RETRIES; r++) {
                            balances = await fetchAccountBalance(address);
                            if (balances !== null) break;
                            if (r < this.BALANCE_FETCH_RETRIES) await delay(this.BALANCE_FETCH_BACKOFF_MS);
                        }
                        if (balances === null) return;
                        await Account.query(knex)
                            .patch({
                                balances: balances as any,
                                spendable_balances: balances as any,
                                updated_at: new Date().toISOString(),
                            })
                            .where({ address });
                        this.accountCache.delete(address);
                    })
                );
                updated += results.filter((r): r is PromiseFulfilledResult<void> => r.status === 'fulfilled').length;
                for (const r of results) {
                    if (r.status === 'rejected' && (r.reason as any)?.name !== 'AbortError') {
                        this.logger.debug('[bulkRefreshAccountBalances] Single address failed:', r.reason);
                    }
                }
            }
            if (outer + this.BALANCE_REFRESH_CHUNK < unique.length && this.DELAY_BETWEEN_BALANCE_CHUNKS_MS > 0) {
                await delay(this.DELAY_BETWEEN_BALANCE_CHUNKS_MS);
            }
        }
        return { updated };
    }

    @Action({ name: 'bulkRefreshAccountBalances', params: { addresses: { type: 'array', items: 'string' } } })
    async bulkRefreshAccountBalances(ctx: { params: { addresses: string[] } }): Promise<{ success: boolean; updated: number }> {
        const raw = ctx.params.addresses;
        if (!Array.isArray(raw) || raw.length === 0) return { success: true, updated: 0 };
        const unique = [...new Set(raw.map((a) => String(a).trim()).filter((a) => a.length > 0 && isValidAccountAddress(a)))];
        try {
            const { updated } = await this.doBulkRefreshAccountBalances(unique);
            return { success: true, updated };
        } catch (err: unknown) {
            this.logger.warn('[bulkRefreshAccountBalances] Failed:', err);
            return { success: false, updated: 0 };
        }
    }
}
