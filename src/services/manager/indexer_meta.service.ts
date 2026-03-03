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
import { indexerStatusManager } from "./indexer_status.manager";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";
import {
  VeranaTrustRegistryMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaDidMessageTypes,
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
  [VeranaDidMessageTypes.AddDid]: "AddDID",
  [VeranaDidMessageTypes.RenewDid]: "RenewDID",
  [VeranaDidMessageTypes.TouchDid]: "TouchDID",
  [VeranaDidMessageTypes.RemoveDid]: "RemoveDID",
  [VeranaPermissionMessageTypes.CreateRootPermission]: "CreateRootPermission",
  [VeranaPermissionMessageTypes.CreatePermission]: "CreatePermission",
  [VeranaPermissionMessageTypes.StartPermissionVP]: "StartPermissionVP",
  [VeranaPermissionMessageTypes.RenewPermissionVP]: "RenewPermissionVP",
  [VeranaPermissionMessageTypes.RevokePermission]: "RevokePermission",
  [VeranaPermissionMessageTypes.ExtendPermission]: "ExtendPermission",
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

type ChangeOperation = "create" | "update" | "delete";

interface IndexerChange {
  entity_type: string;
  entity_id: string;
  operation: ChangeOperation;
  payload: Record<string, unknown>;
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

function toOperation(eventType?: string, isDelete?: boolean): ChangeOperation {
  if (isDelete) return "delete";
  const label = eventType?.toLowerCase() ?? "";

  // Explicit "create" operations - only actual creation of new entities
  const createPatterns = [
    "create",
    "add_did", // legacy DID creation
    "adddid", // AddDid message type
  ];

  // Explicit "delete" operations - only actual deletions
  const deletePatterns = [
    "remove_did",
    "removedid",
    "delete",
  ];

  // Check for explicit create patterns (must be actual creation, not just contains "create")
  for (const pattern of createPatterns) {
    if (label.includes(pattern)) {
      return "create";
    }
  }

  // Check for explicit delete patterns
  for (const pattern of deletePatterns) {
    if (label.includes(pattern)) {
      return "delete";
    }
  }

  // All other operations are updates:
  // - START_PERMISSION_VP (starts a validation process on existing permission chain)
  // - RENEW_PERMISSION_VP (renews an existing permission)
  // - EXTEND_PERMISSION (extends an existing permission)
  // - REVOKE_PERMISSION (marks as revoked, doesn't delete)
  // - SET_VALIDATE_PERMISSION_VP, CANCEL_PERMISSION_VP
  // - SLASH_PERMISSION_TRUST_DEPOSIT, REPAY_PERMISSION_SLASHED_TRUST_DEPOSIT
  // - AddGFV, AddGFD (adds to existing TR, not a new entity creation)
  // - ActivateGFV, IncreaseGFV
  // - Archive (marks as archived, doesn't delete)
  // - RenewDid, TouchDid (updates existing DID)
  // - update, Update
  return "update";
}

function safeJsonParse(value: unknown) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

@Service({
  name: SERVICE.V1.IndexerMetaService.key,
  version: 1,
})
export default class IndexerMetaService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private async getNextChangeAt(blockHeight: number): Promise<number | null> {
    // Query each history table with index-friendly pattern: WHERE height > ? ORDER BY height ASC LIMIT 1.
    const tables = [
      "did_history",
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
      const lcdClient = await getLcdClient();
      const nodeInfo: GetNodeInfoResponseSDKType = await lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo();

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
      didHistory,
      trHistory,
      gfvHistory,
      gfdHistory,
      csHistory,
      permHistory,
      permSessionHistory,
      tdHistory,
      moduleParamsHistory,
    ] = await Promise.all([
      queryHistoryWithTx("did_history", blockHeight, "DID", "did", "did", ["/verana.dd.v1", "/veranablockchain.diddirectory"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("trust_registry_history", blockHeight, "TrustRegistry", "tr_id", "tr_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("governance_framework_version_history", blockHeight, "GovernanceFrameworkVersion", "tr_id", "gfv_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("governance_framework_document_history", blockHeight, "GovernanceFrameworkDocument", "tr_id", "gfd_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("credential_schema_history", blockHeight, "CredentialSchema", "credential_schema_id", "credential_schema_id", ["/verana.cs.v1", "/veranablockchain.credentialschema"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("permission_history", blockHeight, "Permission", "permission_id", "permission_id", ["/verana.perm.v1"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("permission_session_history", blockHeight, "PermissionSession", "session_id", "session_id", ["/verana.perm.v1"], timestampAtHeight, allMessagesAtHeight as any[]),
      queryHistoryWithTx("trust_deposit_history", blockHeight, "TrustDeposit", "account", "account", ["/verana.td.v1"], timestampAtHeight, allMessagesAtHeight as any[]),
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

    for (const record of didHistory) {
      activityItems.push(toActivityItem(record, "DID", record.did));
    }

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
      activityItems.push(toActivityItem(record, "TrustDeposit", record.account));
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
  // Used only for TR weight calculation. Will be removed in the future.
  @Action({
    rest: "POST backfill/trust-registry-stats",
  })
  public async backfillTrustRegistryStats(ctx: Context) {
    try {
      this.logger.info("Starting Trust Registry stats backfill via API...");

      const trustRegistries = await knex("trust_registry")
        .select("id", "did", "controller")
        .orderBy("id", "asc");

      const total = trustRegistries.length;
      if (total === 0) {
        return ApiResponder.success(ctx, {
          message: "No Trust Registries found",
          total: 0,
          updated: 0,
          errors: 0,
        }, 200);
      }

      let successCount = 0;
      let errorCount = 0;
      let schemaIdsFixed = 0;
      let csSyncedFromLedger = 0;
      const errors: Array<{ id: number; error: string }> = [];
      const { overrideSchemaIdInString } = await import('../../common/utils/schema_id_normalizer');
      const { getCredentialSchema } = await import('../../modules/cs-height-sync/ledger_client');

      for (let i = 0; i < trustRegistries.length; i++) {
        const tr = trustRegistries[i];
        const trId = Number(tr.id);

        try {
          const stats = await calculateTrustRegistryStats(trId);

          await knex("trust_registry")
            .where("id", trId)
            .update({
              participants: stats.participants,
              active_schemas: stats.active_schemas,
              archived_schemas: stats.archived_schemas,
              weight: stats.weight,
              issued: stats.issued,
              verified: stats.verified,
              ecosystem_slash_events: stats.ecosystem_slash_events,
              ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
              ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
              network_slash_events: stats.network_slash_events,
              network_slashed_amount: stats.network_slashed_amount,
              network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
            });

          const schemas = await knex("credential_schemas")
            .where("tr_id", trId)
            .select("id", "json_schema");

          for (const schema of schemas) {
            const currentStr = typeof schema.json_schema === "string"
              ? schema.json_schema
              : JSON.stringify(schema.json_schema);
            const normalizedStr = overrideSchemaIdInString(currentStr, schema.id);
            if (currentStr !== normalizedStr) {
              await knex("credential_schemas")
                .where({ id: schema.id })
                .update({ json_schema: normalizedStr });
              schemaIdsFixed++;
            }
            const ledgerResponse = await getCredentialSchema(schema.id);
            if (!ledgerResponse?.schema) {
              this.logger.warn(`Backfill: ledger API returned no schema for CS id=${schema.id}; check LCD_ENDPOINT and path /verana/cs/v1/get/${schema.id}`);
              continue;
            }
            try {
              const result = await this.broker.call(
                `${SERVICE.V1.CredentialSchemaDatabaseService.path}.syncFromLedger`,
                {
                  ledgerResponse: { schema: ledgerResponse.schema },
                  blockHeight: 0,
                }
              ) as { success?: boolean; data?: { success?: boolean } };
              const ok = result?.success === true || result?.data?.success === true;
              if (ok) csSyncedFromLedger++;
              else this.logger.warn(`Backfill: syncFromLedger did not report success for CS id=${schema.id}`);
            } catch (err: any) {
              this.logger.warn(`Backfill: syncFromLedger failed for CS id=${schema.id}: ${err?.message ?? err}`);
            }
            await new Promise<void>((resolve) => {
              setTimeout(() => {
                resolve();
              }, 30);
            });
          }

          successCount++;
        } catch (err: any) {
          errorCount++;
          const errorMsg = err?.message || String(err);
          errors.push({ id: trId, error: errorMsg });
          this.logger.warn(`Failed to update TR ${trId}: ${errorMsg}`);
        }

        if (i < trustRegistries.length - 1) {
          await new Promise<void>((resolve) => {
            setTimeout(() => {
              resolve();
            }, 50);
          });
        }
      }

      this.logger.info(`Backfill completed: ${successCount}/${total} updated, ${errorCount} errors, ${schemaIdsFixed} schema IDs fixed, ${csSyncedFromLedger} CS synced from ledger`);

      const totalSchemas = await knex("credential_schemas").count("* as c").first().then((r: any) => Number(r?.c ?? 0));
      const payload: Record<string, unknown> = {
        message: "Trust Registry stats backfill completed",
        total,
        updated: successCount,
        errors: errorCount,
        schema_ids_fixed: schemaIdsFixed,
        cs_synced_from_ledger: csSyncedFromLedger,
        error_details: errors.length > 0 ? errors : undefined,
      };
      if (csSyncedFromLedger === 0 && totalSchemas > 0) {
        payload.hint = "No CS records were synced from the ledger. If you expect CS data to come from the chain, set LEDGER_LCD_URL to the chain LCD (e.g. https://api.testnet.verana.network) and ensure the chain exposes GET /verana/cs/v1/get/{id}. Check logs for ledger API or syncFromLedger failures.";
      }
      return ApiResponder.success(ctx, payload, 200);
    } catch (err: any) {
      this.logger.error(" Fatal error during backfill:", err);
      return ApiResponder.error(ctx, `Backfill failed: ${err?.message || err}`, 500);
    }
  }
}

