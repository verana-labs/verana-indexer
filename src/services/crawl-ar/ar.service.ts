import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import config from '../../../config.json' with { type: 'json' };
import { Network } from '../../../network';
import BullableService from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE } from '../../common';
import knex from '../../common/utils/db_connection';
import { Block } from '../../models';
import { Account } from '../../models/account';
import { BlockCheckpoint } from '../../models/block_checkpoint';

interface Balance {
    denom: string;
    amount: string;
}

@Service({
    name: SERVICE.V1.HANDLE_ACCOUNTS.key,
    version: 1,
})
export default class CrawlNewAccountsService extends BullableService {
    private timer: NodeJS.Timeout | null = null;
    private readonly JOB_NAME = (BULL_JOB_NAME as any).JOB_HANDLE_ACCOUNTS || 'crawl:new-accounts';
    private readonly BATCH_SIZE = config?.crawlAccounts?.chunkSize || 100;
    private readonly CRAWL_INTERVAL = config?.crawlAccounts?.millisecondCrawl || 10000;
    private readonly ENABLE_RECONCILE = (config as any)?.crawlAccounts?.reconcile?.enabled || false;
    private accountCache = new Map<string, Account>();
    private cacheSize = 1000;

    constructor(public broker: ServiceBroker) {
        super(broker);
    }

    async started() {
        this.logger.info(`[CrawlNewAccountsService] Starting with batch ${this.BATCH_SIZE}`);

        try {
            const checkpoint = await this.ensureCheckpoint();
            await this.processBlocks(checkpoint);

            this.timer = setInterval(
                () => this.processBlocks(checkpoint),
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
        this.accountCache.clear();
    }

    private async ensureCheckpoint() {
        let checkpoint = await BlockCheckpoint.query(knex).findOne({ job_name: this.JOB_NAME });

        if (!checkpoint) {
            checkpoint = await BlockCheckpoint.query(knex).insertAndFetch({
                job_name: this.JOB_NAME,
                height: 0,
            });
        }
        return checkpoint;
    }

    private async updateCheckpoint(newHeight: number) {
        try {
            await BlockCheckpoint.query(knex)
                .patch({
                    height: newHeight,
                })
                .where({ job_name: this.JOB_NAME });
        } catch (err) {
            this.logger.error(`[Checkpoint] Error:`, err);
        }
    }

    @Action({ name: 'processBlocks' })
    async processBlocks(blockCheckpointRow?: any) {
        const [startBlock] = await BlockCheckpoint.getCheckpoint(this.JOB_NAME, []);
        let lastHeight = startBlock || 0;
        let totalBlocksProcessed = 0;

        while (true) {
            const nextBlocks = await Block.query()
                .where('height', '>', lastHeight)
                .orderBy('height', 'asc')
                .limit(this.BATCH_SIZE);

            if (!nextBlocks.length) {
                break;
            }

            try {
                const changedAddresses = new Set<string>();
                let batchTransfers = 0;
                let batchSuccess = true;

                for (const block of nextBlocks) {
                    const result = await this.processBlockTransactions(block, changedAddresses);
                    batchSuccess = batchSuccess && result.success;
                    batchTransfers += result.transfersProcessed;
                    if (!result.success) break;
                }

                if (!batchSuccess) {
                    break;
                }

                if (this.ENABLE_RECONCILE && Network?.LCD) {
                    await this.reconcileChangedAddresses(Array.from(changedAddresses), nextBlocks[nextBlocks.length - 1].height);
                }

                totalBlocksProcessed += nextBlocks.length;
                const newHeight = nextBlocks[nextBlocks.length - 1].height;
                await this.updateCheckpoint(newHeight);
                lastHeight = newHeight;

                if (nextBlocks.length < this.BATCH_SIZE) break;
            } catch (err) {
                this.logger.error(`[PROCESS] Error:`, err);
                break;
            }
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

    private async processBlockTransactions(block: Block, changedAddresses: Set<string>): Promise<{ success: boolean; transfersProcessed: number }> {
        try {
            const blockData = typeof block.data === 'string' ? JSON.parse(block.data) : block.data;
            const blockResult = blockData?.block_result;
            if (!blockResult) return { success: true, transfersProcessed: 0 };

            const transferEvents: any[] = [];

            const extractEvents = (events: any[], source: string) => {
                if (Array.isArray(events)) {
                    for (const event of events) {
                        if (event?.type === 'transfer') {
                            transferEvents.push({ ...event, source });
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
                        (event.attributes || []).map((a: any) => [a.key, a.value])
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
                    this.logger.error(`[TRANSFER] Failed:`, err);
                }
            }

            return { success: true, transfersProcessed: processed };
        } catch (err) {
            this.logger.error(`[PROCESS] Error block ${block.height}:`, err);
            return { success: false, transfersProcessed: 0 };
        }
    }

    private async ensureAccountExists(address: string): Promise<void> {
        if (!address) return;

        const existing = await this.getAccountFromCacheOrDB(address);
        if (existing) return;

        const accountData: any = {
            address,
            type: 'user-accounts',
            balances: [],
            spendable_balances: [],
            account_number: 0,
            sequence: 0,
            created_at: new Date().toISOString(),
        };

        try {
            await Account.query(knex).insert(accountData);
            this.accountCache.delete(address);
        } catch (err: any) {
            if (err?.nativeError?.code === '42703' && err?.nativeError?.column === 'pub_key') {
                delete accountData.pub_key;
                await Account.query(knex).insert(accountData);
                this.accountCache.delete(address);
            } else if (err?.code === '23505' || err?.nativeError?.code === '23505') {
            } else {
                this.logger.error(`[ENSURE_ACCOUNT] Error creating account ${address}:`, err);
            }
        }
    }

    private async getAccountFromCacheOrDB(address: string): Promise<Account | null> {
        if (this.accountCache.has(address)) {
            return this.accountCache.get(address)!;
        }

        const account = await Account.query(knex).findOne({ address });
        if (account && this.accountCache.size < this.cacheSize) {
            this.accountCache.set(address, account);
        }
        return account || null;
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
        await Account.query(knex)
            .patch({
                balances: updatedBalances,
                spendable_balances: updatedBalances,
                updated_at: new Date().toISOString(),
            })
            .where({ address });

        this.accountCache.delete(address);
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

        await Account.query(knex)
            .patch({
                balances: newBalances,
                spendable_balances: newBalances,
                updated_at: new Date().toISOString(),
            })
            .where({ address });

        this.accountCache.delete(address);
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

        const batchSize = 10;
        for (let i = 0; i < addresses.length; i += batchSize) {
            const batch = addresses.slice(i, i + batchSize);
            const promises = batch.map(address => this.reconcileSingleAddress(address, height, lcd));
            await Promise.allSettled(promises);
        }
    }

    private async reconcileSingleAddress(address: string, height: number, lcd: string) {
        try {
            const url = `${lcd.replace(/\/$/, '')}/cosmos/bank/v1beta1/balances/${address}`;
            const res = await fetch(url);
            if (!res.ok) return;

            const data: any = await res.json();
            const onChainBalances: Balance[] = Array.isArray(data?.balances)
                ? data.balances.map((b: any) => ({ denom: b.denom, amount: b.amount || '0' }))
                : [];
            console.log(onChainBalances, "RECONCILE")
            await Account.query(knex)
                .patch({
                    spendable_balances: onChainBalances as any,
                    updated_at: new Date().toISOString(),
                })
                .where({ address });

            this.accountCache.delete(address);
        } catch (err) {
            this.logger.error(`[RECONCILE] Error ${address}:`, err);
        }
    }
}