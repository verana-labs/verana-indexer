import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";

type ChangeOperation = "create" | "update" | "delete";

interface IndexerChange {
  entity_type: string;
  entity_id: string;
  operation: ChangeOperation;
  payload: Record<string, any>;
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

function safeJsonParse(value: any) {
  if (!value) return null;
  if (typeof value === "object") return value;
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
  public async getBlockHeight(ctx: Context) {
    const checkpoint = await knex("block_checkpoint")
      .where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION)
      .first();

    if (!checkpoint) {
      return ApiResponder.error(ctx, "Block checkpoint not found", 404);
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
      knex("did_history").where("height", blockHeight),
      knex("trust_registry_history").where("height", blockHeight),
      knex("governance_framework_version_history").where("height", blockHeight),
      knex("governance_framework_document_history").where("height", blockHeight),
      knex("credential_schema_history").where("height", blockHeight),
      knex("permission_history").where("height", blockHeight),
      knex("permission_session_history").where("height", blockHeight),
      knex("trust_deposit_history").where("height", blockHeight),
      knex("module_params_history").where("height", blockHeight),
    ]);

    const changeEntries: IndexerChange[] = [];

    for (const record of didHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "DidDirectory",
        entity_id: record.did,
        operation: toOperation(eventType, record.is_deleted),
        payload: {
          ...snapshot,
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    for (const record of trHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "TrustRegistry",
        entity_id: String(record.tr_id),
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    for (const record of gfvHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "GovernanceFrameworkVersion",
        entity_id: String(record.gfv_id ?? record.id),
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    for (const record of gfdHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "GovernanceFrameworkDocument",
        entity_id: String(record.gfd_id ?? record.id),
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    for (const record of csHistory) {
      const {
        action: actionType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "CredentialSchema",
        entity_id: String(record.credential_schema_id ?? record.id),
        operation: toOperation(actionType),
        payload: {
          ...snapshot,
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    // Permission
    for (const record of permHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "Permission",
        entity_id: String(record.permission_id),
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    // PermissionSession
    for (const record of permSessionHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "PermissionSession",
        entity_id: record.session_id,
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          authz: safeJsonParse(record.authz),
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    // TrustDeposit
    for (const record of tdHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "TrustDeposit",
        entity_id: record.account,
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          amount: record.amount?.toString(),
          share: record.share?.toString(),
          claimable: record.claimable?.toString(),
          slashed_deposit: record.slashed_deposit?.toString(),
          repaid_deposit: record.repaid_deposit?.toString(),
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    // GlobalVariables (module_params)
    for (const record of moduleParamsHistory) {
      const {
        event_type: eventType,
        changes: recordChanges,
        ...snapshot
      } = record;
      changeEntries.push({
        entity_type: "GlobalVariables",
        entity_id: record.module,
        operation: toOperation(eventType),
        payload: {
          ...snapshot,
          params: safeJsonParse(record.params),
          changes: safeJsonParse(recordChanges),
        },
      });
    }

    return ApiResponder.success(
      ctx,
      {
        block_height: blockHeight,
        changes: changeEntries,
      },
      200
    );
  }
}

