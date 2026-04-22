import type { Context } from "moleculer";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";
import { BlockCheckpoint } from "../../models";
import {
  findHeightsWithTrustModuleMessages,
  getResolverRuntimeConfig,
  getResolverTuning,
  getDeclaredPollObjectCachingRetryDays,
  getTrustEvaluationTtlSeconds,
  listDueReattemptables,
  markReattemptableAttempt,
  trustTxPrefilterEnabled,
  resolveTrustForDidAtHeight,
  resolveTrustForBlock,
} from "./trust-resolve";

const checkCrawlingStatusPromise = import("../../common/utils/error_handler");

type ResolverCheckpointRow = { height: number };

const ResolverPollService = {
  name: SERVICE.V1.ResolverPollService.key,
  version: 1,

  created(this: { pollTimer?: ReturnType<typeof setInterval> }) {
    this.pollTimer = undefined;
  },

  methods: {
    async refreshExpiredTrustResults(this: any): Promise<void> {
      const cfg = getResolverRuntimeConfig();
      if (!cfg?.enabled) return;

      const trustTtlSeconds = getTrustEvaluationTtlSeconds();
      if (!Number.isFinite(trustTtlSeconds) || trustTtlSeconds <= 0) return;

      const lastTrust = await this.getOrCreateResolverCheckpointHeight();
      if (lastTrust <= 0) return;

      // Refresh a small batch per poll cycle to avoid overwhelming the DB.
      const now = new Date();
      const rows = (await knex("trust_results")
        .select("did", "height", "expires_at")
        .whereNotNull("expires_at")
        .andWhere("expires_at", "<=", now)
        .andWhere("height", "<=", lastTrust)
        .orderBy("expires_at", "asc")
        .limit(50)) as Array<{ did: string; height: number; expires_at: Date | string }>;

      if (rows.length === 0) return;

      for (const r of rows) {
        const did = String(r.did ?? "");
        const height = Number(r.height ?? 0);
        if (!did || !Number.isInteger(height) || height < 0) continue;
        try {
          await resolveTrustForDidAtHeight(did, height);
        } catch {
          this.logger.warn(`[RESOLVER] Failed to refresh trust for ${did}@${height}, will retry again later if still eligible.`);
        }
      }
    },
    async initialSyncIfNeeded(this: any): Promise<void> {
      const cfg = getResolverRuntimeConfig();
      if (!cfg?.enabled) return;

      const currentHeight = await this.getOrCreateResolverCheckpointHeight();
      if (currentHeight > 0) return;

      const indexedHeight = await this.getIndexedHeight();
      if (indexedHeight <= 0) return;

      await this.processBlock(indexedHeight);
      await this.advanceResolverCheckpoint(indexedHeight);
    },
    async retryDueFailures(this: any): Promise<void> {
      const retryDays = getDeclaredPollObjectCachingRetryDays() ?? 0;
      if (retryDays <= 0) return;

      const due = await listDueReattemptables(200);
      if (due.length === 0) return;

      for (const item of due) {
        const rid = String(item.resource_id ?? "");
        if (!rid) continue;
        await markReattemptableAttempt(rid);

        if (String(item.resource_type) !== "did-evaluation") continue;
        const m = /^(did:.+)@(\d+)$/.exec(rid);
        if (!m) continue;
        const did = m[1];
        const height = Number(m[2]);
        if (!Number.isInteger(height) || height < 0) continue;
        try {
          await resolveTrustForDidAtHeight(did, height);
        } catch {
          console.error(`[RESOLVER] Reattempt for ${rid} failed, will retry again later if still eligible.`);
        }
      }
    },
    async getIndexedHeight(this: any): Promise<number> {
      const checkpoint = await knex("block_checkpoint").where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION).first();
      const height = Number((checkpoint as ResolverCheckpointRow | null)?.height ?? 0);
      return Number.isFinite(height) ? height : 0;
    },

    async getOrCreateResolverCheckpointHeight(this: any): Promise<number> {
      const existing = await BlockCheckpoint.query().where("job_name", BULL_JOB_NAME.HANDLE_TRUST_RESOLVE).first();
      if (existing) {
        const h = Number(existing.height ?? 0);
        return Number.isFinite(h) ? Math.trunc(h) : 0;
      }

      try {
        await BlockCheckpoint.query().insert({ job_name: BULL_JOB_NAME.HANDLE_TRUST_RESOLVE, height: 0 });
      } catch (err: unknown) {
        const code =
          (err as { nativeError?: { code?: string }; code?: string })?.nativeError?.code ??
          (err as { code?: string })?.code;
        if (code === "23505") {
          const row = await BlockCheckpoint.query().where("job_name", BULL_JOB_NAME.HANDLE_TRUST_RESOLVE).first();
          const h = Number(row?.height ?? 0);
          return Number.isFinite(h) ? Math.trunc(h) : 0;
        }
        throw err;
      }

      const created = await BlockCheckpoint.query().where("job_name", BULL_JOB_NAME.HANDLE_TRUST_RESOLVE).first();
      return Number(created?.height ?? 0);
    },

    async advanceResolverCheckpoint(this: any, blockHeight: number): Promise<void> {
      const now = new Date();
      await BlockCheckpoint.query()
        .insert({ job_name: BULL_JOB_NAME.HANDLE_TRUST_RESOLVE, height: blockHeight, updated_at: now })
        .onConflict("job_name")
        .merge({ height: blockHeight, updated_at: now });
    },

    async processBlock(this: any, blockHeight: number): Promise<void> {
      if (!Number.isInteger(blockHeight) || blockHeight < 0) return;
      await resolveTrustForBlock(blockHeight);
      await this.broker.call(`${SERVICE.V1.IndexerEventsService.path}.broadcastBlockResolved`, {
        height: blockHeight,
        timestamp: new Date().toISOString(),
      });
    },

    async pollOnce(this: any): Promise<void> {
      const { checkCrawlingStatus } = await checkCrawlingStatusPromise;

      try {
        checkCrawlingStatus();
      } catch {
        this.logger.warn("[RESOLVER] Crawling stopped, skipping resolver poll");
        return;
      }

      const cfg = getResolverRuntimeConfig();
      if (!cfg?.enabled) return;

      await this.initialSyncIfNeeded();

      await this.retryDueFailures();

      await this.refreshExpiredTrustResults();

      const currentHeight = await this.getOrCreateResolverCheckpointHeight();
      const indexedHeight = await this.getIndexedHeight();
      if (indexedHeight <= currentHeight) return;

      const tuning = await getResolverTuning(this.logger);
      const targetEnd = Math.min(indexedHeight, currentHeight + tuning.blocksPerCall);

      if (trustTxPrefilterEnabled()) {
        const activeHeights = await findHeightsWithTrustModuleMessages(currentHeight, targetEnd);
        try {
          for (const height of activeHeights) await this.processBlock(height);
          await this.advanceResolverCheckpoint(targetEnd);
        } catch (err) {
          this.logger.warn(`[RESOLVER] Failed to resolve batch up to ${targetEnd}:`, err);
        }
        return;
      }

      for (let height = currentHeight + 1; height <= targetEnd; height++) {
        try {
          await this.processBlock(height);
          await this.advanceResolverCheckpoint(height);
        } catch (err) {
          this.logger.warn(`[RESOLVER] Failed to resolve block ${height}:`, err);
          break;
        }
      }
    },
  },

  actions: {
    handleTrustResolveJob: {
      params: { height: { type: "number", integer: true, positive: true, convert: true } },
      async handler(this: any, ctx: Context<{ height: number }>): Promise<{ accepted: boolean; height: number }> {
        const height = Number(ctx.params?.height ?? 0);
        if (!Number.isInteger(height) || height < 0) return { accepted: false, height: 0 };

        const cfg = getResolverRuntimeConfig();
        if (!cfg?.enabled) return { accepted: false, height };

        const checkpointHeight = await this.getOrCreateResolverCheckpointHeight();
        if (height <= checkpointHeight) return { accepted: false, height };

        if (trustTxPrefilterEnabled()) {
          const active = await findHeightsWithTrustModuleMessages(height - 1, height);
          if (active.length === 0) {
            await this.advanceResolverCheckpoint(height);
            return { accepted: true, height };
          }
        }

        await this.processBlock(height);
        await this.advanceResolverCheckpoint(height);
        return { accepted: true, height };
      },
    },
  },

  async started(this: any) {
    const cfg = getResolverRuntimeConfig();
    if (process.env.NODE_ENV === "test" || !cfg?.enabled) return;

    const tuning = await getResolverTuning(this.logger);
    this.logger.info(
      `[RESOLVER] poll=${tuning.pollIntervalMs}ms reindex=${tuning.isReindexing} ` +
        `blocks/batch=${tuning.blocksPerCall} conc=${tuning.didConcurrency} maxDids=${tuning.maxDidsPerBlock}`
    );
    this.pollTimer = setInterval(() => {
      this.pollOnce().catch((err: unknown) => this.logger.warn("[RESOLVER] pollOnce failed:", err));
    }, tuning.pollIntervalMs);
  },

  async stopped(this: any) {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = undefined;
    }
  },
};

export { ResolverPollService };
export default ResolverPollService;
