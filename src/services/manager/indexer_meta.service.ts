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
import {
  VeranaTrustRegistryMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaPermissionMessageTypes,
} from "../../common/verana-message-types";

const MSG_TYPE_TO_ACTION: Record<string, string> = {
  [VeranaTrustRegistryMessageTypes.CreateTrustRegistry]: "CreateTrustRegistry",
  [VeranaTrustRegistryMessageTypes.CreateTrustRegistryLegacy]: "CreateTrustRegistry",
  [VeranaTrustRegistryMessageTypes.UpdateTrustRegistry]: "UpdateTrustRegistry",
  [VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry]: "ArchiveTrustRegistry",
  [VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc]: "AddGovernanceFrameworkDocument",
  [VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion]: "IncreaseGovernanceFrameworkVersion",
  [VeranaCredentialSchemaMessageTypes.CreateCredentialSchema]: "CreateCredentialSchema",
  [VeranaCredentialSchemaMessageTypes.CreateCredentialSchemaLegacy]: "CreateCredentialSchema",
  [VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema]: "UpdateCredentialSchema",
  [VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema]: "ArchiveCredentialSchema",
  [VeranaPermissionMessageTypes.CreateRootPermission]: "CreateRootPermission",
  [VeranaPermissionMessageTypes.CreatePermission]: "CreatePermission",
  [VeranaPermissionMessageTypes.StartPermissionVP]: "StartPermissionVP",
  [VeranaPermissionMessageTypes.RenewPermissionVP]: "RenewPermissionVP",
  [VeranaPermissionMessageTypes.RevokePermission]: "RevokePermission",
  [VeranaPermissionMessageTypes.SetPermissionVPToValidated]: "SetPermissionVPToValidated",
  [VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession]: "CreateOrUpdatePermissionSession",
  [VeranaPermissionMessageTypes.SlashPermissionTrustDeposit]: "SlashPermissionTrustDeposit",
  [VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit]: "RepayPermissionSlashedTrustDeposit",
  [VeranaPermissionMessageTypes.CancelPermissionVPLastRequest]: "CancelPermissionVPLastRequest",
};

function normalizeEventType(eventType: string): string {
  let normalized = eventType.replace(/^.*\./, "");

  if (normalized.includes("_")) {
    normalized = normalized
      .split("_")
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("");
  } else if (normalized === normalized.toUpperCase()) {
    normalized = normalized
      .split(/(?=[A-Z])/)
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("");
  }

  return normalized;
}

function getActionFromMessageType(msgType: string, eventType?: string, action?: string): string {
  if (msgType && MSG_TYPE_TO_ACTION[msgType]) {
    return MSG_TYPE_TO_ACTION[msgType];
  }
  if (eventType) {
    return normalizeEventType(eventType);
  }
  if (action) {
    return normalizeEventType(action);
  }
  return "Unknown";
}

function filterChangedValues(changes: any): any {
  if (!changes || typeof changes !== "object") {
    return changes;
  }

  const filtered: any = {};
  for (const [key, value] of Object.entries(changes)) {
    if (value !== null && value !== undefined) {
      filtered[key] = value;
    }
  }
  return Object.keys(filtered).length > 0 ? filtered : null;
}

interface ChangesActivityItem {
  timestamp: string | null;
  block_height: string;
  entity_type: string;
  entity_id: string;
  msg: string;
  changes: Record<string, unknown> | null;
  account?: string;
}

interface ChangesResponse {
  block_height: number;
  next_change_at: number | null;
  activity: ChangesActivityItem[];
}

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

  private async getNextChangeAt(blockHeight: number): Promise<number | null> {
    // Query each history table with index-friendly pattern: WHERE height > ? ORDER BY height ASC LIMIT 1.
    const tables = [
      "trust_registry_history",
      "governance_framework_version_history",
      "governance_framework_document_history",
      "credential_schema_history",
      "permission_history",
      "permission_session_history",
      "trust_deposit_history",
      "module_params_history",
    ];

    const heights = await Promise.all(
      tables.map(async (tableName) => {
        try {
          const row = await knex(tableName)
            .select("height")
            .where("height", ">", blockHeight)
            .orderBy("height", "asc")
            .limit(1)
            .first();
          const parsed = Number(row?.height);
          return Number.isFinite(parsed) ? parsed : null;
        } catch (error: any) {
          const errorMsg = error?.message || String(error);
          if (errorMsg.includes("does not exist")) return null;
          throw error;
        }
      })
    );

    const validHeights = heights.filter((h): h is number => h !== null);
    return validHeights.length > 0 ? Math.min(...validHeights) : null;
  }

  @Action()
  public async getVersion(ctx: Context) {
    try {
      const includeRuntimeInfo = process.env.VERSION_INCLUDE_RUNTIME_NETWORK_INFO === "true";
      const nodeInfo = includeRuntimeInfo
        ? await this.getNodeInfoWithTimeout(Number(process.env.VERSION_NODE_INFO_TIMEOUT_MS || 250))
        : null;

      const networkInfo = {
        chainId: nodeInfo?.default_node_info?.network || Network.chainId || "unknown",
        rpcEndpoint: Network.RPC || "unknown",
        lcdEndpoint: Network.LCD || "unknown",
        cosmosSdkVersion: nodeInfo?.application_version?.cosmos_sdk_version || "unknown",
        nodeVersion: nodeInfo?.application_version?.version || "unknown",
        appName: "verana-indexer",
      };

      return ApiResponder.success(
        ctx,
        {
          appVersion: getIndexerVersion(),
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
          appVersion: getIndexerVersion(),
          environment: {
            network: {
              chainId: Network.chainId || "unknown",
              rpcEndpoint: Network.RPC || "unknown",
              lcdEndpoint: Network.LCD || "unknown",
              cosmosSdkVersion: "unknown",
              nodeVersion: "unknown",
              appName: "verana-indexer",
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
          type: "block-processed",
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
        type: "block-processed",
        height: checkpoint.height,
        timestamp,
      },
      200
    );
  }

  @Action({
    params: {
      block_height: { type: "number", integer: true, positive: true, convert: true },
    },
  })
  public async listChanges(ctx: Context<{ block_height: number }>) {
    const blockHeight = ctx.params.block_height;

    if (!Number.isInteger(blockHeight) || blockHeight < 0) {
      return ApiResponder.error(
        ctx,
        "block_height parameter is required and must be a positive integer",
        400
      );
    }

    const heightTimestampPromise = knex("transaction")
      .select("timestamp")
      .where("height", blockHeight)
      .orderBy("index", "asc")
      .orderBy("id", "asc")
      .first()
      .then((row: any) => (row?.timestamp ? new Date(row.timestamp).toISOString() : null));

    const heightMessagesPromise = knex("transaction_message")
      .innerJoin("transaction", "transaction.id", "transaction_message.tx_id")
      .where("transaction.height", blockHeight)
      .orderBy("transaction_message.index", "asc")
      .select("transaction_message.type as msg_type", "transaction_message.sender as sender");

    const txMetaByPrefixesCache = new Map<string, { msg_type: string | null; sender: string | null }>();
    const getMsgMetaForPrefixes = (
      allMessagesAtHeight: Array<{ msg_type?: string | null; sender?: string | null }>,
      msgTypePrefixes: string[]
    ): { msg_type: string | null; sender: string | null } => {
      const cacheKey = (msgTypePrefixes || []).join(",");
      const cached = txMetaByPrefixesCache.get(cacheKey);
      if (cached) return cached;

      let selected: { msg_type: string | null; sender: string | null };
      if (!msgTypePrefixes || msgTypePrefixes.length === 0) {
        const first = allMessagesAtHeight[0];
        selected = {
          msg_type: first?.msg_type ?? null,
          sender: first?.sender ?? null,
        };
      } else {
        const match = allMessagesAtHeight.find((m) => {
          const type = String(m?.msg_type ?? "");
          return msgTypePrefixes.some((prefix) => type.startsWith(prefix));
        });
        selected = {
          msg_type: match?.msg_type ?? null,
          sender: match?.sender ?? null,
        };
      }

      txMetaByPrefixesCache.set(cacheKey, selected);
      return selected;
    };

    const queryHistoryWithTx = async (
      tableName: string,
      height: number,
      _entityType: string,
      _idField: string,
      _entityIdField: string,
      msgTypePrefixes: string[],
      timestampAtHeight: string | null,
      allMessagesAtHeight: Array<{ msg_type?: string | null; sender?: string | null }>
    ) => {
      try {
        const rows = await knex(tableName)
          .select(`${tableName}.*`)
          .where(`${tableName}.height`, height);

        if (rows.length === 0) return [];

        const txMeta = getMsgMetaForPrefixes(allMessagesAtHeight, msgTypePrefixes || []);

        return rows.map((row: any) => ({
          ...row,
          timestamp: timestampAtHeight,
          msg_type: txMeta.msg_type,
          sender: txMeta.sender,
        }));
      } catch (error: any) {
        const errorMsg = error?.message || String(error);
        if (errorMsg.includes("does not exist") && errorMsg.includes("height")) {
          this.logger.warn(
            `Table ${tableName} is missing 'height' column. This usually means migrations need to run. Returning empty results.`
          );
          return [];
        }
        throw error;
      }
    };

    const [timestampAtHeight, allMessagesAtHeight] = await Promise.all([
      heightTimestampPromise,
      heightMessagesPromise,
    ]);

    const [
      trHistory,
      gfvHistory,
      gfdHistory,
      csHistory,
      permHistory,
      permSessionHistory,
      tdHistory,
      moduleParamsHistory,
    ] = await Promise.all([
      queryHistoryWithTx("trust_registry_history", blockHeight, "TrustRegistry", "tr_id", "tr_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("governance_framework_version_history", blockHeight, "GovernanceFrameworkVersion", "tr_id", "gfv_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("governance_framework_document_history", blockHeight, "GovernanceFrameworkDocument", "tr_id", "gfd_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("credential_schema_history", blockHeight, "CredentialSchema", "credential_schema_id", "credential_schema_id", ["/verana.cs.v1", "/veranablockchain.credentialschema"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("permission_history", blockHeight, "Permission", "permission_id", "permission_id", ["/verana.perm.v1"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("permission_session_history", blockHeight, "PermissionSession", "session_id", "session_id", ["/verana.perm.v1"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("trust_deposit_history", blockHeight, "TrustDeposit", "corporation", "corporation", ["/verana.td.v1"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("module_params_history", blockHeight, "GlobalVariables", "module", "module", [], timestampAtHeight, allMessagesAtHeight as any[]),
    ]);

    const activityItems: ChangesActivityItem[] = [];

    const toActivityItem = (record: any, entityType: string, entityId: string) => {
      let changes = record.changes;
      if (typeof changes === "string") {
        try {
          changes = JSON.parse(changes);
        } catch {
          changes = null;
        }
      }

      if (!changes || Object.keys(changes).length === 0) {
        const computedChanges: Record<string, any> = {};
        const excludeFields = ["id", "created_at", "event_type", "height", "changes", "msg_type", "sender", "timestamp"];
        for (const [key, value] of Object.entries(record)) {
          if (value !== null && value !== undefined && !excludeFields.includes(key)) {
            computedChanges[key] = value;
          }
        }
        changes = Object.keys(computedChanges).length > 0 ? computedChanges : null;
      }

      changes = filterChangedValues(changes);

      const action = getActionFromMessageType(
        record.msg_type,
        record.event_type,
        record.action
      );

      const activityItem: ChangesActivityItem = {
        timestamp: record.timestamp ? new Date(record.timestamp).toISOString() : null,
        block_height: String(record.height || blockHeight),
        entity_type: entityType,
        entity_id: entityId,
        msg: action,
        changes: changes,
      };

      if (record.sender) {
        activityItem.account = record.sender;
      }

      return activityItem;
    };

    for (const record of trHistory) {
      activityItems.push(toActivityItem(record, "TrustRegistry", String(record.tr_id)));
    }

    for (const record of gfvHistory) {
      activityItems.push(toActivityItem(record, "GovernanceFrameworkVersion", String(record.gfv_id ?? record.id)));
    }

    for (const record of gfdHistory) {
      activityItems.push(toActivityItem(record, "GovernanceFrameworkDocument", String(record.gfd_id ?? record.id)));
    }

    for (const record of csHistory) {
      activityItems.push(toActivityItem(record, "CredentialSchema", String(record.credential_schema_id ?? record.id)));
    }

    for (const record of permHistory) {
      activityItems.push(toActivityItem(record, "Permission", String(record.permission_id)));
    }

    for (const record of permSessionHistory) {
      activityItems.push(toActivityItem(record, "PermissionSession", record.session_id));
    }

    for (const record of tdHistory) {
      const tdId = String(record.corporation ?? record.account ?? "");
      activityItems.push(toActivityItem(record, "TrustDeposit", tdId));
    }

    for (const record of moduleParamsHistory) {
      activityItems.push(toActivityItem(record, "GlobalVariables", record.module));
    }

    activityItems.sort((a, b) => {
      if (!a.timestamp && !b.timestamp) return 0;
      if (!a.timestamp) return 1;
      if (!b.timestamp) return -1;
      const timeDiff = new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
      if (timeDiff !== 0) return timeDiff;
      const entityDiff = a.entity_type.localeCompare(b.entity_type);
      if (entityDiff !== 0) return entityDiff;
      return a.entity_id.localeCompare(b.entity_id);
    });

    const nextChangeAt = await this.getNextChangeAt(blockHeight);
    const response: ChangesResponse = {
      block_height: blockHeight,
      next_change_at: nextChangeAt,
      activity: activityItems,
    };

    return ApiResponder.success(
      ctx,
      response,
      200
    );
  }
}

