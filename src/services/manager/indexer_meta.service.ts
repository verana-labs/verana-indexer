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
    if (value && typeof value === "object" && ("old" in value || "new" in value)) {
      const val = value as { old?: any; new?: any };
      if (JSON.stringify(val.old) !== JSON.stringify(val.new)) {
        filtered[key] = value;
      }
    } else if (value !== null && value !== undefined) {
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

      const queryHistoryWithTx = async (
      tableName: string,
      height: number,
      entityType: string,
      idField: string,
      entityIdField: string,
      msgTypePrefixes: string[]
    ) => {
      try {
        const quotedTable = `"${tableName}"`;
        const prefixes = msgTypePrefixes || [];
        const msgTypeCondition = prefixes.length > 0 
          ? `AND (${prefixes.map((p, idx) => {
              const escapedPrefix = p.replace(/'/g, "''");
              return idx === 0 ? `transaction_message.type LIKE '${escapedPrefix}%'` : `OR transaction_message.type LIKE '${escapedPrefix}%'`;
            }).join(' ')})`
          : '';

        return await knex(tableName)
          .select(
            `${tableName}.*`,
            "transaction.timestamp",
            knex.raw('? as activity_entity_type', [entityType]),
            knex.raw(`COALESCE(CAST(${tableName}.${entityIdField} AS TEXT), CAST(${tableName}.id AS TEXT)) as activity_entity_id`),
            knex.raw(`(
              SELECT transaction_message.type 
              FROM transaction_message 
              INNER JOIN transaction t ON t.id = transaction_message.tx_id
              WHERE t.height = ${quotedTable}.height
              ${msgTypeCondition}
              ORDER BY transaction_message.index ASC
              LIMIT 1
            ) as msg_type`),
            knex.raw(`(
              SELECT transaction_message.sender 
              FROM transaction_message 
              INNER JOIN transaction t ON t.id = transaction_message.tx_id
              WHERE t.height = ${quotedTable}.height
              ${msgTypeCondition}
              ORDER BY transaction_message.index ASC
              LIMIT 1
            ) as sender`)
          )
          .leftJoin("transaction", function () {
            this.on(`${tableName}.height`, "=", "transaction.height");
          })
          .where(`${tableName}.height`, height);
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
      queryHistoryWithTx("did_history", blockHeight, "DID", "did", "did", ["/verana.dd.v1", "/veranablockchain.diddirectory"]),
      queryHistoryWithTx("trust_registry_history", blockHeight, "TrustRegistry", "tr_id", "tr_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"]),
      queryHistoryWithTx("governance_framework_version_history", blockHeight, "GovernanceFrameworkVersion", "tr_id", "gfv_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"]),
      queryHistoryWithTx("governance_framework_document_history", blockHeight, "GovernanceFrameworkDocument", "tr_id", "gfd_id", ["/verana.tr.v1", "/veranablockchain.trustregistry"]),
      queryHistoryWithTx("credential_schema_history", blockHeight, "CredentialSchema", "credential_schema_id", "credential_schema_id", ["/verana.cs.v1", "/veranablockchain.credentialschema"]),
      queryHistoryWithTx("permission_history", blockHeight, "Permission", "permission_id", "permission_id", ["/verana.perm.v1"]),
      queryHistoryWithTx("permission_session_history", blockHeight, "PermissionSession", "session_id", "session_id", ["/verana.perm.v1"]),
      queryHistoryWithTx("trust_deposit_history", blockHeight, "TrustDeposit", "account", "account", ["/verana.td.v1"]),
      queryHistoryWithTx("module_params_history", blockHeight, "GlobalVariables", "module", "module", []),
    ]);

    const activityItems: any[] = [];

    const toActivityItem = (record: any, entityType: string, entityId: string) => {
      let changes = record.changes;
      if (typeof changes === "string") {
        try {
          changes = JSON.parse(changes);
        } catch {
          changes = null;
        }
      }
      changes = filterChangedValues(changes);

      const action = getActionFromMessageType(
        record.msg_type,
        record.event_type,
        record.action
      );

      const activityItem: any = {
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

    return ApiResponder.success(
      ctx,
      {
        block_height: blockHeight,
        activity: activityItems,
      },
      200
    );
  }
}

