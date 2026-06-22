import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import { GetNodeInfoResponseSDKType } from "@aura-nw/aurajs/types/codegen/cosmos/base/tendermint/v1beta1/query";
import BaseService from "../../base/base.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { getIndexerVersion } from "../../common/utils/version";
import { getLcdClient } from "../../common/utils/verana_client";
import { Network } from "../../network";
import { toIsoSeconds } from "../api/api_shared";
import {
  buildVtChangesEnvelope,
  parseVtChangesQuery,
  type VtChange,
} from "../api/vt_subscribe_protocol";
import { buildVtChangesForBlock, listVtChangeHeights } from "../resolver/vt_change_detection";

const VT_CHANGES_SCAN_PAGE = 500;

@Service({
  name: SERVICE.V1.IndexerMetaService.key,
  version: 1,
})
export default class IndexerMetaService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private async getNodeInfoWithTimeout(timeoutMs: number): Promise<GetNodeInfoResponseSDKType | null> {
    try {
      const lcdClient = await getLcdClient();
      const nodeInfo = await Promise.race([
        lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo(),
        new Promise<null>((resolve) => {
          setTimeout(() => resolve(null), timeoutMs);
        }),
      ]);
      return nodeInfo as GetNodeInfoResponseSDKType | null;
    } catch {
      return null;
    }
  }

  @Action()
  public async getVersion(ctx: Context) {
    try {
      const includeRuntimeInfo = process.env.VERSION_INCLUDE_RUNTIME_NETWORK_INFO === "true";
      const nodeInfo = includeRuntimeInfo
        ? await this.getNodeInfoWithTimeout(Number(process.env.VERSION_NODE_INFO_TIMEOUT_MS || 250))
        : null;

      const networkInfo = {
        chain_id: nodeInfo?.default_node_info?.network || Network.chainId || "unknown",
        rpc_endpoint: Network.RPC || "unknown",
        lcd_endpoint: Network.LCD || "unknown",
        cosmos_sdk_version: nodeInfo?.application_version?.cosmos_sdk_version || "unknown",
        node_version: nodeInfo?.application_version?.version || "unknown",
        app_name: "verana-indexer",
      };

      return ApiResponder.success(
        ctx,
        {
          app_version: getIndexerVersion(),
          environment: {
            network: networkInfo,
          },
        },
        200
      );
    } catch (error) {
      return ApiResponder.success(
        ctx,
        {
          app_version: getIndexerVersion(),
          environment: {
            network: {
              chain_id: Network.chainId || "unknown",
              rpc_endpoint: Network.RPC || "unknown",
              lcd_endpoint: Network.LCD || "unknown",
              cosmos_sdk_version: "unknown",
              node_version: "unknown",
              app_name: "verana-indexer",
            },
          },
        },
        200
      );
    }
  }

  @Action()
  public async getBlockHeight(ctx: Context) {
    const checkpoint = await knex("block_checkpoint")
      .where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION)
      .first();

    if (!checkpoint) {
      return ApiResponder.success(
        ctx,
        {
          type: "block-indexed",
          height: 0,
          timestamp: new Date().toISOString(),
        },
        200
      );
    }

    const updatedAt =
      checkpoint.updated_at instanceof Date
        ? checkpoint.updated_at
        : new Date(checkpoint.updated_at);
    const iso = updatedAt.toISOString();
    const timestamp = iso.replace(/\.\d{3}Z$/, "Z");

    return ApiResponder.success(
      ctx,
      {
        type: "block-indexed",
        height: checkpoint.height,
        timestamp,
      },
      200
    );
  }

  @Action({
    params: {
      fromBlock: { type: "any", optional: true },
      dids: { type: "any", optional: true },
      corporation_id: { type: "any", optional: true },
      channels: { type: "any", optional: true },
      includeParticipantCounts: { type: "any", optional: true },
      includeIssuedCredentials: { type: "any", optional: true },
      includeVerifiedCredentials: { type: "any", optional: true },
      limit: { type: "any", optional: true },
    },
  })
  public async listVtChanges(ctx: Context<Record<string, unknown>>) {
    const parsed = parseVtChangesQuery(ctx.params ?? {});
    if (!parsed.ok) return ApiResponder.error(ctx, parsed.error, 400);

    const { fromBlock, dids, corporationId, channels, limit } = parsed.value;
    const currentBlock = await this.getLastIndexedBlockHeight();
    const didFilter = dids === null ? null : new Set(dids);

    const blocks: Array<{ block: number; blockTime: string; changes: VtChange[] }> = [];
    let nextFromBlock: number | null = null;
    let cursor = fromBlock;

    while (blocks.length < limit) {
      const heights = await listVtChangeHeights(cursor, currentBlock, VT_CHANGES_SCAN_PAGE);
      if (heights.length === 0) {
        nextFromBlock = null;
        break;
      }

      const blockTimeByHeight = await this.blockTimesForHeights(heights);
      let stopped = false;

      for (let i = 0; i < heights.length; i++) {
        const height = heights[i];
        const raw = await buildVtChangesForBlock(height);
        const blockTime = blockTimeByHeight.get(height) ?? toIsoSeconds(new Date());
        const envelope = buildVtChangesEnvelope(
          height,
          blockTime,
          raw,
          didFilter,
          corporationId,
          channels
        );
        if (envelope.changes.length === 0) continue;

        blocks.push({ block: height, blockTime, changes: envelope.changes });
        if (blocks.length >= limit) {
          if (i + 1 < heights.length) {
            nextFromBlock = heights[i + 1];
          } else {
            const more = await listVtChangeHeights(height + 1, currentBlock, 1);
            nextFromBlock = more.length > 0 ? more[0] : null;
          }
          stopped = true;
          break;
        }
      }

      if (stopped) break;
      if (heights.length < VT_CHANGES_SCAN_PAGE) {
        nextFromBlock = null;
        break;
      }
      cursor = heights[heights.length - 1] + 1;
    }

    return ApiResponder.success(
      ctx,
      { currentBlock, fromBlock, blocks, nextFromBlock },
      200
    );
  }

  private async getLastIndexedBlockHeight(): Promise<number> {
    const checkpoint = await knex("block_checkpoint")
      .where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION)
      .first();
    const height = Number((checkpoint as { height?: number } | undefined)?.height ?? 0);
    return Number.isFinite(height) ? Math.trunc(height) : 0;
  }

  private async blockTimesForHeights(heights: number[]): Promise<Map<number, string>> {
    const map = new Map<number, string>();
    if (heights.length === 0) return map;
    const rows = (await knex("block")
      .select("height", "time")
      .whereIn("height", heights)) as Array<{ height: number; time: Date | string }>;
    for (const row of rows) {
      map.set(Number(row.height), toIsoSeconds(row.time));
    }
    return map;
  }
}

