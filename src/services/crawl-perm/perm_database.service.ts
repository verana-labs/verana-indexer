import { Service, ServiceBroker } from "moleculer";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { SERVICE, ModulesParamsNamesTypes } from "../../common";
import getGlobalVariables from "../../common/utils/global_variables";
import { mapPermissionType } from "../../common/utils/utils";
import { requireController } from "../../common/utils/extract_controller";
import { calculatePermState } from "./perm_state_utils";
import { calculateCredentialSchemaStats } from "../crawl-cs/cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";
import { getModuleParams } from "../../common/utils/params_service";
import {
  getPermissionTypeString,
  MsgCancelPermissionVPLastRequest,
  MsgCreateOrUpdatePermissionSession,
  MsgCreatePermission,
  MsgCreateRootPermission,
  MsgExtendPermission,
  MsgRenewPermissionVP,
  MsgRepayPermissionSlashedTrustDeposit,
  MsgRevokePermission,
  MsgSetPermissionVPToValidated,
  MsgSlashPermissionTrustDeposit,
  MsgStartPermissionVP,
} from "./perm_types";

const PERMISSION_HISTORY_FIELDS = [
  "schema_id",
  "type",
  "did",
  "grantee",
  "created_by",
  "created",
  "modified",
  "extended",
  "extended_by",
  "slashed",
  "slashed_by",
  "repaid",
  "repaid_by",
  "effective_from",
  "effective_until",
  "revoked",
  "revoked_by",
  "country",
  "validation_fees",
  "issuance_fees",
  "verification_fees",
  "deposit",
  "slashed_deposit",
  "repaid_deposit",
  "validator_perm_id",
  "vp_state",
  "vp_last_state_change",
  "participants",
  "weight",
  "ecosystem_slash_events",
  "ecosystem_slashed_amount",
  "ecosystem_slashed_amount_repaid",
  "network_slash_events",
  "network_slashed_amount",
  "network_slashed_amount_repaid",
  "vp_current_fees",
  "vp_current_deposit",
  "vp_summary_digest_sri",
  "vp_exp",
  "vp_validator_deposit",
  "vp_term_requested",
  "issued",
  "verified",
];

const PERMISSION_SESSION_HISTORY_FIELDS = [
  "controller",
  "agent_perm_id",
  "wallet_agent_perm_id",
  "authz",
  "created",
  "modified",
];

function normalizeValue(value: any) {
  if (value === undefined) return null;
  return value;
}

function computeChanges(
  oldRecord: any,
  newRecord: any,
  fields: string[]
): Record<string, any> | null {
  const changes: Record<string, any> = {};
  
  if (!oldRecord) {
    for (const field of fields) {
      const newValue = normalizeValue(newRecord?.[field]);
      if (newValue !== null && newValue !== undefined) {
        changes[field] = newValue;
      }
    }
  } else {
    for (const field of fields) {
      const oldValue = normalizeValue(oldRecord?.[field]);
      const newValue = normalizeValue(newRecord?.[field]);
      if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
        changes[field] = newValue;
      }
    }
  }
  return Object.keys(changes).length ? changes : null;
}

function parseJson<T = any>(value: any): T | any {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value as any;
  }
}

let permissionHistoryIssuedColumnExistsCache: boolean | null = null;
let permissionHistoryVerifiedColumnExistsCache: boolean | null = null;

async function checkPermissionHistoryColumnExists(columnName: string): Promise<boolean> {
  if (columnName === "issued") {
    if (permissionHistoryIssuedColumnExistsCache !== null) {
      return permissionHistoryIssuedColumnExistsCache;
    }
  } else if (columnName === "verified") {
    if (permissionHistoryVerifiedColumnExistsCache !== null) {
      return permissionHistoryVerifiedColumnExistsCache;
    }
  }
  
  try {
    const result = await knex.schema.hasColumn('permission_history', columnName);
    if (columnName === "issued") {
      permissionHistoryIssuedColumnExistsCache = result;
    } else if (columnName === "verified") {
      permissionHistoryVerifiedColumnExistsCache = result;
    }
    return result;
  } catch (error) {
    if (columnName === "issued") {
      permissionHistoryIssuedColumnExistsCache = false;
    } else if (columnName === "verified") {
      permissionHistoryVerifiedColumnExistsCache = false;
    }
    return false;
  }
}

async function pickPermissionSnapshot(record: any) {
  const snapshot: Record<string, any> = {
    permission_id: String(record.permission_id ?? record.id ?? ""),
  };
  
  const hasIssuedColumn = await checkPermissionHistoryColumnExists("issued");
  const hasVerifiedColumn = await checkPermissionHistoryColumnExists("verified");
  const hasParticipantsColumn = await checkPermissionHistoryColumnExists("participants");
  const hasWeightColumn = await checkPermissionHistoryColumnExists("weight");
  const hasEcosystemSlashEventsColumn = await checkPermissionHistoryColumnExists("ecosystem_slash_events");
  
  for (const field of PERMISSION_HISTORY_FIELDS) {
    if (field === "expire_soon") {
      continue;
    }
    if (field === "issued" && !hasIssuedColumn) {
      continue;
    }
    if (field === "verified" && !hasVerifiedColumn) {
      continue;
    }
    if (field === "participants" && !hasParticipantsColumn) {
      continue;
    }
    if (field === "weight" && !hasWeightColumn) {
      continue;
    }
    if ((field === "ecosystem_slash_events" || field === "ecosystem_slashed_amount" || 
         field === "ecosystem_slashed_amount_repaid" || field === "network_slash_events" ||
         field === "network_slashed_amount" || field === "network_slashed_amount_repaid") && !hasEcosystemSlashEventsColumn) {
      continue;
    }
    if (field === "schema_id") {
      const schemaIdValue = record[field];
      snapshot[field] = schemaIdValue !== null && schemaIdValue !== undefined ? Number(schemaIdValue) : null;
    } else {
      snapshot[field] = normalizeValue(record[field]);
    }
  }
  return snapshot;
}

async function recordPermissionHistory(
  db: any,
  permissionRecord: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!permissionRecord) return;
  
  const hasIssuedColumn = await checkPermissionHistoryColumnExists("issued");
  const hasVerifiedColumn = await checkPermissionHistoryColumnExists("verified");
  const hasParticipantsColumn = await checkPermissionHistoryColumnExists("participants");
  const hasWeightColumn = await checkPermissionHistoryColumnExists("weight");
  const hasEcosystemSlashEventsColumn = await checkPermissionHistoryColumnExists("ecosystem_slash_events");
  
  const fieldsToUse = PERMISSION_HISTORY_FIELDS.filter(field => {
    // Exclude expire_soon - it's a computed field based on current date, not a historical attribute
    if (field === "expire_soon") return false;
    if (field === "issued" && !hasIssuedColumn) return false;
    if (field === "verified" && !hasVerifiedColumn) return false;
    if (field === "participants" && !hasParticipantsColumn) return false;
    if (field === "weight" && !hasWeightColumn) return false;
    if ((field === "ecosystem_slash_events" || field === "ecosystem_slashed_amount" || 
         field === "ecosystem_slashed_amount_repaid" || field === "network_slash_events" ||
         field === "network_slashed_amount" || field === "network_slashed_amount_repaid") && !hasEcosystemSlashEventsColumn) {
      return false;
    }
    return true;
  });
  
  const snapshot = await pickPermissionSnapshot(permissionRecord);
  const changes = computeChanges(previousRecord, permissionRecord, fieldsToUse);
  
  if (previousRecord && !changes) {
    return; 
  }
  
  await db("permission_history").insert({
    ...snapshot,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
  });
}

function pickPermissionSessionSnapshot(record: any) {
  const snapshot: Record<string, any> = {
    session_id: String(record.session_id ?? record.id ?? ""),
  };
  for (const field of PERMISSION_SESSION_HISTORY_FIELDS) {
    if (field === "authz") {
      const authz = record[field];
      if (authz === null || authz === undefined) {
        snapshot[field] = null;
      } else if (typeof authz === "string") {
        snapshot[field] = authz; 
      } else {
        snapshot[field] = JSON.stringify(authz); 
      }
    } else {
      snapshot[field] = normalizeValue(record[field]);
    }
  }
  return snapshot;
}

async function recordPermissionSessionHistory(
  db: any,
  sessionRecord: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!sessionRecord) return;
  const snapshot = pickPermissionSessionSnapshot(sessionRecord);
  const changes = computeChanges(
    previousRecord,
    sessionRecord,
    PERMISSION_SESSION_HISTORY_FIELDS
  );
  
  if (previousRecord && !changes) {
    return; 
  }
  
  await db("permission_session_history").insert({
    ...snapshot,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
  });
}

interface QueuedPermission {
  message: any;
  reason: string;
  retryCount: number;
  queuedAt: Date;
  nextRetryAt: Date;
}

export default class PermIngestService extends Service {
  private retryQueue: Map<string, QueuedPermission> = new Map();
  private retryInterval: NodeJS.Timeout | null = null;
  private readonly MAX_RETRY_ATTEMPTS = 10;
  private readonly RETRY_INTERVAL_MS = 30000; // 30 seconds
  private readonly RETRY_BACKOFF_MULTIPLIER = 2;

  /**
   * Calculate expire_soon for a permission based on its state and effective_until
   * Returns: null if not active, false if no expiration or not soon, true if expiring soon
   */
  private async calculateExpireSoon(
    perm: any,
    now: Date = new Date(),
    blockHeight?: number
  ): Promise<boolean | null> {
    // Check if permission is active
    const effectiveFrom = perm.effective_from ? new Date(perm.effective_from) : null;
    const effectiveUntil = perm.effective_until ? new Date(perm.effective_until) : null;
    
    if (effectiveFrom && now < effectiveFrom) return null;
    if (effectiveUntil && now > effectiveUntil) return null;
    if (perm.revoked) return null;
    if (perm.slashed && !perm.repaid) return null;
    if (perm.vp_state !== 'VALIDATED' && perm.type !== 'ECOSYSTEM') return null;

    if (!effectiveUntil) {
      return false;
    }

    let nDaysBefore = 0;
    try {
      const moduleParams = await getModuleParams(ModulesParamsNamesTypes.PERM, blockHeight);
      if (moduleParams?.params) {
        nDaysBefore = moduleParams.params.PERMISSION_SET_EXPIRE_SOON_N_DAYS_BEFORE || 0;
      }
    } catch (error) {
      this.logger.warn(`Failed to get PERMISSION module params for expire_soon calculation:`, error);
      nDaysBefore = 0;
    }

    // Calculate expiration check date (now + nDaysBefore)
    const expirationCheckDate = new Date(now);
    expirationCheckDate.setDate(expirationCheckDate.getDate() + nDaysBefore);
    
    // If expiration check date is greater than effective_until, it's expiring soon
    return expirationCheckDate > effectiveUntil;
  }

  public constructor(broker: ServiceBroker) {
    super(broker);
    this.parseServiceSchema({
      name: "permIngest",
      actions: {
        handleMsgCreateRootPermission: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateRootPermission(ctx.params.data),
        },
        handleMsgCreatePermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleCreatePermission(ctx.params.data),
        },
        handleMsgExtendPermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleExtendPermission(ctx.params.data),
        },
        handleMsgRevokePermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRevokePermission(ctx.params.data),
        },
        handleMsgStartPermissionVP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleStartPermissionVP(ctx.params.data),
        },
        handleMsgSetPermissionVPToValidated: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSetPermissionVPToValidated(ctx.params.data),
        },
        handleMsgRenewPermissionVP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRenewPermissionVP(ctx.params.data),
        },
        handleMsgCancelPermissionVPLastRequest: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCancelPermissionVPLastRequest(ctx.params.data),
        },
        handleMsgCreateOrUpdatePermissionSession: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateOrUpdatePermissionSession(ctx.params.data),
        },
        handleMsgSlashPermissionTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSlashPermissionTrustDeposit(ctx.params.data),
        },
        handleMsgRepayPermissionSlashedTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleRepayPermissionSlashedTrustDeposit(ctx.params.data),
        },
        getPermission: {
          params: { schema_id: "number", grantee: "string", type: "string" },
          handler: async (ctx) => {
            const { schema_id: schemaId, grantee, type } = ctx.params;
            return await knex("permissions")
              .where({ schema_id: schemaId, grantee, type })
              .first();
          },
        },
        listPermissions: {
          params: {
            schema_id: { type: "number", optional: true },
            grantee: { type: "string", optional: true },
            type: { type: "string", optional: true },
          },
          handler: async (ctx) => {
            let query = knex("permissions");
            if (ctx.params.schema_id)
              query = query.where("schema_id", ctx.params.schema_id);
            if (ctx.params.grantee)
              query = query.where("grantee", ctx.params.grantee);
            if (ctx.params.type) query = query.where("type", ctx.params.type);
            return await query;
          },
        },
      },
    });
  }

  private async handleCreateRootPermission(msg: MsgCreateRootPermission & { height?: number }) {
    let permission: any = null;
    try {
      this.logger.info(`🔐 handleCreateRootPermission called with msg:`, JSON.stringify(msg, null, 2));
      const schemaId = (msg as any).schemaId ?? (msg as any).schema_id ?? null;
      this.logger.info(`🔐 Extracted schemaId: ${schemaId}`);
      if (!schemaId) {
        this.logger.error(
          "CRITICAL: Missing schema_id in MsgCreateRootPermission, cannot create root permission. Msg keys:", Object.keys(msg)
        );
        return;
        
      }

      const creator = requireController(msg, `PERM CREATE_ROOT ${schemaId}`);
      const timestamp = msg?.timestamp ? formatTimestamp(msg.timestamp) : null;
      const effectiveFrom = msg.effective_from ? formatTimestamp(msg.effective_from) : null;
      const effectiveUntil = msg.effective_until ? formatTimestamp(msg.effective_until) : null;
      
      // Calculate expire_soon for the new permission
      const newPermData = {
        type: "ECOSYSTEM",
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const height = Number((msg as any)?.height) || 0;
      const expireSoon = await this.calculateExpireSoon(newPermData, new Date(timestamp || new Date()), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");
      
      this.logger.info(`[handleCreateRootPermission] expire_soon calculated: ${expireSoon}, column exists: ${hasExpireSoonColumn}`);
      
      const insertData: any = {
        schema_id: schemaId,
        type: "ECOSYSTEM",
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        did: msg.did,
        grantee: creator,
        created_by: creator,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        country: msg.country ?? null,
        validation_fees: String(
          (msg as any).validation_fees ?? (msg as any).validation_fees ?? 0
        ),
        issuance_fees: String(
          (msg as any).issuance_fees ?? (msg as any).issuance_fees ?? 0
        ),
        verification_fees: String(
          (msg as any).verification_fees ?? (msg as any).verification_fees ?? 0
        ),
        deposit: "0",
        modified: timestamp,
        created: timestamp,
      };

      if (hasExpireSoonColumn) {
        insertData.expire_soon = expireSoon;
        this.logger.info(`[handleCreateRootPermission] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleCreateRootPermission] expire_soon column does not exist, skipping. Run migration 20260128000000_add_permission_expire_soon.ts`);
      }

      let insertedPermission;
      try {
        [insertedPermission] = await knex("permissions")
          .insert(insertData)
          .returning("*");
      } catch (insertError: any) {
        if (insertError?.code === '42703' && insertError?.message?.includes('expire_soon')) {
          this.logger.warn(`[handleCreateRootPermission] expire_soon column error detected, clearing cache and retrying without expire_soon`);
          this.permissionsColumnExistsCache = null;
          delete insertData.expire_soon;
          [insertedPermission] = await knex("permissions")
            .insert(insertData)
            .returning("*");
        } else {
          throw insertError;
        }
      }

      if (!insertedPermission) {
        this.logger.error(
          "CRITICAL: Failed to create root permission - insert returned no record"
        );
        return; 
        
      }

      permission = insertedPermission;

      try {
        await recordPermissionHistory(
          knex,
          permission,
          "CREATE_ROOT_PERMISSION",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for root permission:",
          historyErr
        );
       
      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, String(permission.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for root permission ${permission.id}:`, participantsErr?.message || participantsErr);
      }
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCreateRootPermission:", err);
      console.error("FATAL PERM CREATE ROOT ERROR:", err);
      
    }
  }

  private async handleCreatePermission(msg: MsgCreatePermission & { height?: number }) {
    try {
      this.logger.info(`🔐 handleCreatePermission called with msg:`, JSON.stringify(msg, null, 2));
      const schemaId = (msg as any).schemaId ?? (msg as any).schema_id ?? null;
      this.logger.info(`🔐 Extracted schemaId: ${schemaId}`);
      if (!schemaId) {
        this.logger.warn(
          "Missing schema_id in MsgCreatePermission, skipping insert. Msg keys:", Object.keys(msg)
        );
        return;
        
      }

      const type = mapPermissionType((msg as any).type);

      const ecosystemPerm = await knex("permissions")
        .where({ schema_id: schemaId, type: "ECOSYSTEM" })
        .first();

      if (!ecosystemPerm) {
        this.logger.warn(
          `No root ECOSYSTEM permission found for schema_id=${schemaId}, cannot create ${type}`
        );
      }

      const creator = requireController(msg, `PERM CREATE ${schemaId}`);
      const timestamp = msg?.timestamp ? formatTimestamp(msg.timestamp) : null;
      const effectiveFrom = msg.effective_from ? formatTimestamp(msg.effective_from) : null;
      const effectiveUntil = msg.effective_until ? formatTimestamp(msg.effective_until) : null;
      const height = Number((msg as any)?.height) || 0;
      
      // Calculate expire_soon for the new permission
      const newPermData = {
        type,
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const expireSoon = await this.calculateExpireSoon(newPermData, new Date(timestamp || new Date()), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");
      
      this.logger.info(`[handleCreatePermission] expire_soon calculated: ${expireSoon}, column exists: ${hasExpireSoonColumn}`);
      
      const insertData: any = {
        schema_id: schemaId,
        type,
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        did: msg.did,
        grantee: creator,
        created_by: creator,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        country: msg.country ?? null,
        verification_fees: String(
          (msg as any).verification_fees ?? (msg as any).verification_fees ?? 0
        ),
        validation_fees: "0",
        issuance_fees: "0",
        deposit: "0",
        validator_perm_id: ecosystemPerm?.id ?? null,
        modified: timestamp,
        created: timestamp,
      };

      if (hasExpireSoonColumn) {
        insertData.expire_soon = expireSoon;
        this.logger.info(`[handleCreatePermission] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleCreatePermission] expire_soon column does not exist, skipping. Run migration 20260128000000_add_permission_expire_soon.ts`);
      }

      let permission;
      try {
        [permission] = await knex("permissions")
          .insert(insertData)
          .returning("*");
      } catch (insertError: any) {
        // If error is about missing column, clear cache and retry without expire_soon
        if (insertError?.code === '42703' && insertError?.message?.includes('expire_soon')) {
          this.logger.warn(`[handleCreatePermission] expire_soon column error detected, clearing cache and retrying without expire_soon`);
          this.permissionsColumnExistsCache = null;
          delete insertData.expire_soon;
          [permission] = await knex("permissions")
            .insert(insertData)
            .returning("*");
        } else {
          throw insertError;
        }
      }

      if (!permission) {
        this.logger.error(
          "CRITICAL: Failed to create permission - insert returned no record"
        );
        return;
        

      }

      try {
        await recordPermissionHistory(
          knex,
          permission,
          "CREATE_PERMISSION",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history:",
          historyErr
        );
       
      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, String(permission.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for permission ${permission.id}:`, participantsErr?.message || participantsErr);
      }
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCreatePermission:", err);
      console.error("FATAL PERM CREATE ERROR:", err);
      
    }
  }
  private async handleExtendPermission(msg: MsgExtendPermission & { height?: number }) {
    try {
      if (!msg.id || !msg.effective_until) {
        this.logger.warn("Missing mandatory parameter: id or effective_until");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be a valid uint64.`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const newEffectiveUntil = new Date(msg.effective_until);
      if (Number.isNaN(newEffectiveUntil.getTime())) {
        this.logger.warn(
          `Invalid effective_until timestamp: ${msg.effective_until}`
        );
        return { success: false, reason: "Invalid effective_until timestamp" };
      }

      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const currentEffectiveUntil = applicantPerm.effective_until
        ? new Date(applicantPerm.effective_until)
        : null;

      if (currentEffectiveUntil && newEffectiveUntil <= currentEffectiveUntil) {
        this.logger.warn(
          `New effective_until ${newEffectiveUntil.toISOString()} must be greater than current ${currentEffectiveUntil.toISOString()}`
        );
        return {
          success: false,
          reason: "effective_until must be greater than current",
        };
      }

      const caller = msg.creator;

      if (
        !applicantPerm.validator_perm_id &&
        applicantPerm.type === "ECOSYSTEM"
      ) {
        if (caller !== applicantPerm.grantee) {
          this.logger.warn("Only grantee can extend ECOSYSTEM permission");
          return { success: false, reason: "Unauthorized caller" };
        }
      } else if (applicantPerm.validator_perm_id) {
        const validatorPerm = await knex("permissions")
          .where({ id: applicantPerm.validator_perm_id })
          .first();

        if (!validatorPerm) {
          this.logger.warn(
            `Validator permission ${applicantPerm.validator_perm_id} not found`
          );
          return { success: false, reason: "Validator permission not found" };
        }

        if (validatorPerm.type !== "ECOSYSTEM") {
          this.logger.warn("Validator permission must be of type ECOSYSTEM");
          return {
            success: false,
            reason: "Invalid validator permission type",
          };
        }

        if (caller !== applicantPerm.grantee) {
          this.logger.warn("Only grantee can extend self-created permission");
          return { success: false, reason: "Unauthorized caller" };
        }
      } else {
        this.logger.warn("Invalid permission structure");
        return { success: false, reason: "Invalid permission structure" };
      }

      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;
      await knex.transaction(async (trx) => {
        // Calculate expire_soon for the updated permission
        const updatedPermData = {
          ...applicantPerm,
          effective_until: newEffectiveUntil.toISOString(),
        };
        const expireSoon = await this.calculateExpireSoon(updatedPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");
        
        const updateData: any = {
          effective_until: newEffectiveUntil.toISOString(),
          extended: now,
          modified: now,
          extended_by: caller,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to extend permission ${msg.id} - update returned no record`
          );
         
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "EXTEND_PERMISSION",
            height,
            applicantPerm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for extend:",
            historyErr
          );
         
        }

        try {
          await this.updateParticipants(trx, String(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }
      });

      this.logger.info(
        `✅ Permission ${msg.id
        } extended until ${newEffectiveUntil.toISOString()} by ${caller}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleExtendPermission:", err);
      console.error("FATAL PERM EXTEND ERROR:", err);
      return { success: false, reason: "Internal error extending permission" };
    }
  }

  private async handleRevokePermission(msg: MsgRevokePermission & { height?: number }) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const now = formatTimestamp(msg.timestamp);
      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const caller = msg.creator;

      let authorized = false;
      if (applicantPerm.validator_perm_id) {
        let validatorPermId = applicantPerm.validator_perm_id;
        while (validatorPermId) {
          const validatorPerm = await knex("permissions")
            .where({ id: validatorPermId })
            .first();
          if (!validatorPerm) break;
          if (validatorPerm.grantee === caller) {
            authorized = true;
            break;
          }
          validatorPermId = validatorPerm.validator_perm_id;
        }
      }

      if (!authorized) {
        const cs = await knex("credential_schemas")
          .where({ id: applicantPerm.schema_id })
          .first();
        if (cs) {
          const tr = await knex("trust_registry")
            .where({ id: cs.tr_id })
            .first();
          if (tr?.controller === caller) {
            authorized = true;
          }
        }
      }

      if (!authorized) {
        if (applicantPerm.grantee === caller) {
          authorized = true;
        }
      }

      if (!authorized) {
        this.logger.warn("Caller is not authorized to revoke this permission");
        return { success: false, reason: "Unauthorized caller" };
      }

      const height = Number((msg as any)?.height) || 0;
      await knex.transaction(async (trx) => {
        // Revoked permissions are not active, so expire_soon should be null
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          revoked: now,
          revoked_by: caller,
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = null;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to revoke permission ${msg.id} - update returned no record`
          );
         
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "REVOKE_PERMISSION",
            height,
            applicantPerm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for revoke:",
            historyErr
          );
         
        }

        try {
          await this.updateParticipants(trx, String(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }
      });

      this.logger.info(
        `Permission ${msg.id} successfully revoked by ${caller}`
      );
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleRevokePermission:", err);
      console.error("FATAL PERM REVOKE ERROR:", err);
      return { success: false, reason: "Internal error revoking permission" };
    }
  }

  private async handleStartPermissionVP(msg: MsgStartPermissionVP & { height?: number }) {
    try {
      const typeStr = getPermissionTypeString(msg);
      const now = formatTimestamp(msg.timestamp);

      const perm = await knex("permissions")
        .where({ id: msg.validator_perm_id })
        .first();

      if (!perm) {
        this.logger.warn(
          `Permission ${msg.validator_perm_id} not found, skipping VP start`
        );
        return;
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      let validationFeesDenom = 0;
      let validationTDDenom = 0;

      if (typeStr !== "HOLDER") {
        const trustUnitPrice = Number(
          globalVariables?.tr?.trust_unit_price ?? 0
        );
        const trustDepositRate = Number(
          globalVariables?.td?.trust_deposit_rate ?? 0
        );

        validationFeesDenom =
          perm?.validation_fees && trustUnitPrice
            ? Number(perm.validation_fees) * trustUnitPrice
            : 0;

        validationTDDenom =
          validationFeesDenom && trustDepositRate
            ? validationFeesDenom * trustDepositRate
            : 0;
      }

      const creator = requireController(msg, `PERM START_VP ${msg.validator_perm_id}`);
      const effectiveFrom = msg.effective_from ? formatTimestamp(msg.effective_from) : null;
      const effectiveUntil = msg.effective_until ? formatTimestamp(msg.effective_until) : null;
      const height = Number((msg as any)?.height) || 0;
      
      // Calculate expire_soon for the new permission (PENDING state means not active, so null)
      const newPermData = {
        type: typeStr,
        vp_state: "PENDING",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const expireSoon = await this.calculateExpireSoon(newPermData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      const Entry: any = {
        schema_id: perm?.schema_id,
        type: typeStr,
        did: msg.did,
        grantee: creator,
        created_by: creator,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        country: msg.country ?? null,
        verification_fees: String((msg as any).verification_fees ?? "0"),
        validation_fees: "0",
        issuance_fees: "0",
        deposit: String(validationTDDenom),
        vp_current_deposit: String(validationTDDenom),
        vp_current_fees: String(validationFeesDenom),
        validator_perm_id: msg.validator_perm_id,
        vp_state: "PENDING",
        vp_last_state_change: now,
        vp_validator_deposit: "0",
        vp_summary_digest_sri: null,
        vp_term_requested: null,
        modified: now,
        created: now,
      };

      if (hasExpireSoonColumn) {
        Entry.expire_soon = expireSoon;
        this.logger.info(`[handleStartPermissionVP] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleStartPermissionVP] expire_soon column does not exist, skipping. Run migration 20260128000000_add_permission_expire_soon.ts`);
      }
      
      let newPermission;
      try {
        [newPermission] = await knex("permissions").insert(Entry).returning("*");
      } catch (insertError: any) {
        if (insertError?.code === '42703' && insertError?.message?.includes('expire_soon')) {
          this.logger.warn(`[handleStartPermissionVP] expire_soon column error detected, clearing cache and retrying without expire_soon`);
          this.permissionsColumnExistsCache = null;
          delete Entry.expire_soon;
          [newPermission] = await knex("permissions").insert(Entry).returning("*");
        } else {
          throw insertError;
        }
      }
      
      if (!newPermission) {
        this.logger.error(
          "CRITICAL: Failed to create permission via VP start - insert returned no record"
        );
       
      }

      this.logger.info(
        `Inserted new VP entry handleStartPermissionVP: ${JSON.stringify(
          Entry
        )}`
      );

      try {
        await recordPermissionHistory(
          knex,
          newPermission,
          "START_PERMISSION_VP",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for VP start:",
          historyErr
        );
       
      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateWeight(trx, String(newPermission.id));
          await this.updateParticipants(trx, String(newPermission.id));
        });
      } catch (updateErr: any) {
        this.logger.warn(`Failed to update weight/participants for new permission ${newPermission.id}:`, updateErr?.message || updateErr);
      }
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleStartPermissionVP:", err);
      console.error("FATAL PERM START VP ERROR:", err);
      // No structured response expected; handler returns void
    }
  }

  public async computeVpExp(perm: any, knex: any): Promise<string | null> {
    const cs = await knex("credential_schemas")
      .where({ id: perm.schema_id })
      .first();

    if (!cs) {
      const schemaExists = await knex("credential_schemas")
        .where({ id: perm.schema_id })
        .first();

      if (!schemaExists) {
        this.logger.warn(`CredentialSchema ${perm.schema_id} not found for permission ${perm.id} - schema may not exist yet, queuing for retry`);
      } else {
        this.logger.warn(`CredentialSchema ${perm.schema_id} exists but not visible in current transaction for permission ${perm.id} - queuing for retry`);
      }
      return null;
    }

    let validityPeriodField: number | null = null;
    let validityPeriodFieldName = "";

    switch (perm.type) {
      case "ISSUER_GRANTOR":
        validityPeriodField = cs.issuer_grantor_validation_validity_period;
        validityPeriodFieldName = "issuer_grantor_validation_validity_period";
        break;
      case "VERIFIER_GRANTOR":
        validityPeriodField = cs.verifier_grantor_validation_validity_period;
        validityPeriodFieldName = "verifier_grantor_validation_validity_period";
        break;
      case "ISSUER":
        validityPeriodField = cs.issuer_validation_validity_period;
        validityPeriodFieldName = "issuer_validation_validity_period";
        break;
      case "VERIFIER":
        validityPeriodField = cs.verifier_validation_validity_period;
        validityPeriodFieldName = "verifier_validation_validity_period";
        break;
      case "HOLDER":
        validityPeriodField = cs.holder_validation_validity_period;
        validityPeriodFieldName = "holder_validation_validity_period";
        break;
      default:
        this.logger.warn(`Unknown permission type '${perm.type}' for permission ${perm.id} - cannot compute vp_exp`);
        return null;
    }

    if (validityPeriodField === null || validityPeriodField === undefined) {
      this.logger.warn(
        `CredentialSchema ${perm.schema_id} exists but validity period field '${validityPeriodFieldName}' is null/undefined ` +
        `for permission ${perm.id} (type: ${perm.type}). Schema data: ${JSON.stringify({
          id: cs.id,
          issuer_grantor_validation_validity_period: cs.issuer_grantor_validation_validity_period,
          verifier_grantor_validation_validity_period: cs.verifier_grantor_validation_validity_period,
          issuer_validation_validity_period: cs.issuer_validation_validity_period,
          verifier_validation_validity_period: cs.verifier_validation_validity_period,
          holder_validation_validity_period: cs.holder_validation_validity_period,
        })}`
      );
      return null;
    }

    const validitySeconds = Number(validityPeriodField);
    if (Number.isNaN(validitySeconds)) {
      this.logger.warn(
        `CredentialSchema ${perm.schema_id} has invalid validity period value '${validityPeriodField}' ` +
        `for field '${validityPeriodFieldName}' (permission ${perm.id}, type: ${perm.type})`
      );
      return null;
    }

    const now = new Date();

    let vpExp: Date;

    if (!perm.vp_exp) {
      vpExp = new Date(now.getTime() + validitySeconds * 1000);
    } else {
      vpExp = new Date(
        new Date(perm.vp_exp).getTime() + validitySeconds * 1000
      );
    }

    return vpExp.toISOString();
  }

  private async handleSetPermissionVPToValidated(
    msg: MsgSetPermissionVPToValidated & { height?: number }
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      this.logger.info(`[SetVPToValidated] Processing permission id=${msg.id}, height=${height}`);

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.error(`[SetVPToValidated] Permission ${msg.id} not found in database`);
        throw new Error(`Permission ${msg.id} not found`);
      }

      if (perm.vp_state !== "PENDING") {
        this.logger.warn(`[SetVPToValidated] Permission ${msg.id} is not PENDING (current state: ${perm.vp_state})`);
        return { success: false, reason: `Permission not pending, current state: ${perm.vp_state}` };
      }

      const isFirstValidation = !perm.effective_from;

      if (msg.country && msg.country.length !== 2) {
        this.logger.warn(`Invalid country code: ${msg.country}`);
        return { success: false, reason: "Invalid country code" };
      }

      if (
        msg.validation_fees < 0 ||
        msg.issuance_fees < 0 ||
        msg.verification_fees < 0
      ) {
        this.logger.warn(`Fees must be >= 0`);
        return { success: false, reason: "Invalid fees" };
      }

      const schemaExists = await knex("credential_schemas")
        .where({ id: perm.schema_id })
        .first();

      if (!schemaExists) {
        await this.queuePermissionForRetry(msg, "SCHEMA_NOT_FOUND");
        this.logger.info(`Permission ${msg.id} queued for retry - waiting for CredentialSchema ${perm.schema_id}`);
        return { success: true, message: "Permission queued for retry - schema not ready" };
      }

      const vpExp = await this.computeVpExp(perm, knex);
      if (vpExp === null) {
        const validityPeriodFieldName = 
          perm.type === "ISSUER_GRANTOR" ? "issuer_grantor_validation_validity_period" :
          perm.type === "VERIFIER_GRANTOR" ? "verifier_grantor_validation_validity_period" :
          perm.type === "ISSUER" ? "issuer_validation_validity_period" :
          perm.type === "VERIFIER" ? "verifier_validation_validity_period" :
          perm.type === "HOLDER" ? "holder_validation_validity_period" : "unknown";
        await this.queuePermissionForRetry(msg, "VALIDITY_PERIOD_MISSING");
        this.logger.error(
          `Permission ${msg.id} queued for retry - CredentialSchema ${perm.schema_id} exists but ` +
          `validity period field '${validityPeriodFieldName}' is missing/null for permission type '${perm.type}'. ` +
          `This indicates a data integrity issue. Schema data: ${JSON.stringify({
            id: schemaExists.id,
            issuer_grantor_validation_validity_period: schemaExists.issuer_grantor_validation_validity_period,
            verifier_grantor_validation_validity_period: schemaExists.verifier_grantor_validation_validity_period,
            issuer_validation_validity_period: schemaExists.issuer_validation_validity_period,
            verifier_validation_validity_period: schemaExists.verifier_validation_validity_period,
            holder_validation_validity_period: schemaExists.holder_validation_validity_period,
          })}`
        );
        return { success: true, message: "Permission queued for retry - validity period missing" };
      }

      const effectiveUntil =
        msg.effective_until ?? perm.effective_until ?? vpExp ?? null;

      if (
        effectiveUntil &&
        vpExp &&
        new Date(effectiveUntil) > new Date(vpExp)
      ) {
        this.logger.warn(
          `effective_until ${effectiveUntil} exceeds vp_exp ${vpExp}`
        );
        return { success: false, reason: "effective_until exceeds vp_exp" };
      }

      const vpValidatorDeposit = isFirstValidation
        ? perm.vp_current_deposit
        : perm.vp_validator_deposit;

      const updatedPermData = {
        ...perm,
        vp_state: "VALIDATED",
        effective_until: effectiveUntil,
        effective_from: isFirstValidation ? now : perm.effective_from,
      };
      const expireSoon = await this.calculateExpireSoon(updatedPermData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");
      
      const entry: any = {
        vp_state: "VALIDATED",
        vp_last_state_change: now,
        vp_current_fees: "0",
        vp_current_deposit: "0",
        vp_validator_deposit: vpValidatorDeposit,
        vp_summary_digest_sri:
          msg.vp_summary_digest_sri ?? perm.vp_summary_digest_sri ?? null,
        vp_exp: vpExp,
        effective_until: effectiveUntil,
        modified: now,
      };

      if (hasExpireSoonColumn) {
        entry.expire_soon = expireSoon;
      }

      if (isFirstValidation) {
        entry.validation_fees = msg.validation_fees ?? "0";
        entry.issuance_fees = msg.issuance_fees ?? "0";
        entry.verification_fees = msg.verification_fees ?? "0";
        entry.country = msg.country ?? null;
        entry.effective_from = now;

        this.logger.info(
          `Setting initial vp_validator_deposit: ${vpValidatorDeposit} for permission ${msg.id}`
        );
      } else {
        const feesChanged =
          (msg.validation_fees &&
            msg.validation_fees !== perm.validation_fees) ||
          (msg.issuance_fees && msg.issuance_fees !== perm.issuance_fees) ||
          (msg.verification_fees &&
            msg.verification_fees !== perm.verification_fees);

        const countryChanged = msg.country && msg.country !== perm.country;

        if (feesChanged || countryChanged) {
          this.logger.warn("Cannot change fees or country during renewal");
          return {
          success: false,
          reason: "Cannot change fees/country on renewal",
          };
        }

        this.logger.info(
          `Preserving existing vp_validator_deposit: ${vpValidatorDeposit} for permission ${msg.id} renewal`
        );
      }

      this.logger.info(`[SetVPToValidated] Updating permission ${msg.id} to VALIDATED`);
      const [updated] = await knex("permissions")
        .where({ id: msg.id })
        .update(entry)
        .returning("*");

      if (!updated) {
        this.logger.error(
          `[SetVPToValidated] CRITICAL: Failed to update permission ${msg.id} - update returned no record`
        );
        throw new Error(`Failed to update permission ${msg.id}`);
      }

      this.logger.info(`[SetVPToValidated] Permission ${msg.id} updated successfully, vp_state=${updated.vp_state}`);
      try {
        await recordPermissionHistory(
          knex,
          updated,
          "SET_VALIDATE_PERMISSION_VP",
          height,
          perm
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for VP validation:",
          historyErr
        );
       
      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, String(msg.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
      }

      this.logger.info(`Permission ${msg.id} successfully validated`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSetPermissionVPToValidated:", err);
      console.error("FATAL PERM VP VALIDATED ERROR:", err);
      return { success: false, reason: "Internal error validating permission VP" };
    }
  }

  private async handleRenewPermissionVP(msg: MsgRenewPermissionVP) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      if (
        msg.creator &&
        applicantPerm.grantee &&
        msg.creator !== applicantPerm.grantee
      ) {
        this.logger.warn(`Caller ${msg.creator} is not the permission grantee`);
        return { success: false, reason: "Caller is not grantee" };
      }

      const validatorPerm = await knex("permissions")
        .where({ id: applicantPerm.validator_perm_id })
        .first();
      if (!validatorPerm) {
        this.logger.warn(
          `Validator permission ${applicantPerm.validator_perm_id} not found`
        );
        return { success: false, reason: "Validator permission not found" };
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      const trustUnitPrice = globalVariables?.tr?.trust_unit_price;
      const trustDepositRate = globalVariables?.td?.trust_deposit_rate;

      if (trustUnitPrice === undefined || trustDepositRate === undefined) {
        this.logger.warn("Global variables not set for fee calculation");
        return { success: false, reason: "Invalid global variables" };
      }

      const validationFeesInDenom =
        Number(validatorPerm.validation_fees) * trustUnitPrice;
      const validationTrustDepositInDenom =
        validationFeesInDenom * trustDepositRate;
      if (
        Number.isNaN(validationFeesInDenom) ||
        Number.isNaN(validationTrustDepositInDenom)
      ) {
        this.logger.warn("Error calculating fees/deposit");
        return { success: false, reason: "Error calculating fees/deposit" };
      }
      const height = Number((msg as any)?.height) || 0;
      await knex.transaction(async (trx) => {
        // Calculate expire_soon for renewed permission (PENDING state means not active)
        const renewedPermData = {
          ...applicantPerm,
          vp_state: "PENDING",
        };
        const expireSoon = await this.calculateExpireSoon(renewedPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          vp_state: "PENDING",
          vp_last_state_change: now,
          vp_current_fees: validationFeesInDenom.toString(),
          vp_current_deposit: validationTrustDepositInDenom.toString(),
          deposit: (
            Number(applicantPerm.deposit) + validationTrustDepositInDenom
          ).toString(),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to update permission ${msg.id} for VP renewal - update returned no record`
          );
         
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "RENEW_PERMISSION_VP",
            height,
            applicantPerm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for VP renewal:",
            historyErr
          );
         
        }

        try {
          await this.updateWeight(trx, String(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for permission ${msg.id} after VP renewal:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, String(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }
      });

      this.logger.info(`Permission ${msg.id} successfully renewed`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleRenewPermissionVP:", err);
      console.error("FATAL PERM RENEW VP ERROR:", err);
      return { success: false, reason: "Internal error renewing permission VP" };
    }
  }

  private async handleCancelPermissionVPLastRequest(
    msg: MsgCancelPermissionVPLastRequest & { height?: number }
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      if (perm.vp_state !== "PENDING") {
        this.logger.warn(`Permission ${msg.id} is not PENDING`);
        return { success: false, reason: "Permission not pending" };
      }

      if (msg.creator && perm.grantee && msg.creator !== perm.grantee) {
        this.logger.warn(`Creator ${msg.creator} is not permission grantee`);
        return { success: false, reason: "Creator is not grantee" };
      }

      const newVpState = perm.vp_exp ? "VALIDATED" : "TERMINATED";

      const vpValidatorDeposit =
        newVpState === "TERMINATED" ? "0" : perm.vp_validator_deposit;

      // Calculate expire_soon for the updated permission
      const updatedPermData = {
        ...perm,
        vp_state: newVpState,
      };
      const expireSoon = await this.calculateExpireSoon(updatedPermData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      const updateData: any = {
        vp_state: newVpState,
        vp_last_state_change: now,
        vp_current_fees: "0",
        vp_current_deposit: "0",
        vp_validator_deposit: vpValidatorDeposit,
        modified: now,
      };

      if (hasExpireSoonColumn) {
        updateData.expire_soon = expireSoon;
      }

      const [updated] = await knex("permissions")
        .where({ id: msg.id })
        .update(updateData)
        .returning("*");

      if (!updated) {
        this.logger.error(
          `CRITICAL: Failed to update permission ${msg.id} - update returned no record`
        );
       
      }

      // Record history for the permission update
      try {
        await recordPermissionHistory(
          knex,
          updated,
          "CANCEL_PERMISSION_VP",
          height,
          perm
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for VP cancellation:",
          historyErr
        );
       
      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, String(msg.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
      }

      this.logger.info(
        `Permission ${msg.id} validation cancelled. New state: ${newVpState}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCancelPermissionVPLastRequest:", err);
      console.error("FATAL PERM CANCEL VP ERROR:", err);
      return { success: false, reason: "Internal error cancelling permission VP request" };
    }
  }

  private async handleSlashPermissionTrustDeposit(
    msg: MsgSlashPermissionTrustDeposit & { height?: number }
  ) {
    try {
      if (!msg.id || msg.amount == null) {
        this.logger.warn("Missing mandatory parameter: id or amount");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be uint64`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const amountNum = Number(msg.amount);
      if (Number.isNaN(amountNum) || amountNum <= 0) {
        this.logger.warn(`Invalid amount: ${msg.amount}`);
        return { success: false, reason: "Invalid amount" };
      }

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const deposit = Number(perm.deposit || 0);
      if (amountNum > deposit) {
        this.logger.warn(
          `Slash amount ${amountNum} exceeds deposit ${deposit}`
        );
        return { success: false, reason: "Amount exceeds deposit" };
      }

      let isAuthorized = false;
      const caller = msg.creator;

      let validatorPerm = perm;
      while (validatorPerm && validatorPerm.validator_perm_id) {
        validatorPerm = await knex("permissions")
          .where({ id: validatorPerm.validator_perm_id })
          .first();
        if (validatorPerm && validatorPerm.grantee === caller) {
          isAuthorized = true;
          break;
        }
      }

      if (!isAuthorized && perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();
        if (schema && schema.tr_id) {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();
          if (tr && tr.controller === caller) {
            isAuthorized = true;
          }
        }
      }

      if (!isAuthorized) {
        this.logger.warn("Unauthorized caller for slash operation");
        return { success: false, reason: "Unauthorized caller" };
      }

      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;
      const prevSlashed = Number(perm.slashed_deposit || 0);

      // Determine if this is ecosystem or network slash
      const isEcosystemPermission = perm.type === "ECOSYSTEM";
      let isEcosystemSlash = false;
      let isNetworkSlash = false;
      let trController: string | null = null;
      let classificationReason = '';
      
      if (isEcosystemPermission) {
        isNetworkSlash = true; // ECOSYSTEM permission slashed = network slash
        classificationReason = 'ECOSYSTEM permission type';
        this.logger.info(`[Slash] Permission ${msg.id} is ECOSYSTEM type - marking as network slash`);
      } else if (perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();
        
        if (!schema) {
          this.logger.warn(`[Slash] Permission ${msg.id} has schema_id ${perm.schema_id} but schema not found in database`);
          classificationReason = `Schema ${perm.schema_id} not found`;
        } else if (!schema.tr_id) {
          this.logger.warn(`[Slash] Permission ${msg.id} schema ${perm.schema_id} has no tr_id`);
          classificationReason = `Schema ${perm.schema_id} has no tr_id`;
        } else {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();
          
          if (!tr) {
            this.logger.warn(`[Slash] Permission ${msg.id} schema ${perm.schema_id} references TR ${schema.tr_id} but TR not found in database`);
            classificationReason = `TR ${schema.tr_id} not found`;
            trController = null;
          } else {
            trController = tr.controller || null;
            if (!trController) {
              this.logger.warn(`[Slash] Permission ${msg.id} TR ${schema.tr_id} exists but has no controller field`);
              classificationReason = `TR ${schema.tr_id} has no controller`;
            } else if (tr.controller === caller) {
              isEcosystemSlash = true;
              classificationReason = `Slashed by TR controller ${caller}`;
              this.logger.info(`[Slash] Permission ${msg.id} slashed by TR controller ${caller} - marking as ecosystem slash`);
            } else {
              this.logger.warn(`[Slash] Permission ${msg.id} slashed by ${caller} but TR controller is ${tr.controller} - no slash type determined`);
              classificationReason = `Caller ${caller} != TR controller ${tr.controller}`;
            }
          }
        }
      } else {
        this.logger.warn(`[Slash] Permission ${msg.id} has no schema_id - no slash type determined`);
        classificationReason = 'No schema_id';
      }

      if (!isEcosystemSlash && !isNetworkSlash) {
        this.logger.error(`[Slash] CRITICAL: Could not classify slash for permission ${msg.id}. Classification reason: ${classificationReason}. Schema ID: ${perm.schema_id || 'N/A'}, Type: ${perm.type}, Caller: ${caller}, TR Controller: ${trController || 'N/A'}`);
      }

      await knex.transaction(async (trx) => {
        const currentDeposit = BigInt(perm.deposit || "0");
        const newDeposit = currentDeposit > BigInt(amountNum) 
          ? currentDeposit - BigInt(amountNum) 
          : BigInt(0);
        
        // Calculate expire_soon for slashed permission (slashed without repaid = inactive)
        const slashedPermData = {
          ...perm,
          slashed: now,
          repaid: null, // Not repaid yet
        };
        const expireSoon = await this.calculateExpireSoon(slashedPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          slashed: now,
          slashed_by: caller,
          slashed_deposit: String(prevSlashed + amountNum),
          deposit: String(newDeposit),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to slash permission ${msg.id} - update returned no record`
          );
         
        }

        // Update slash statistics for this permission and ancestors
        try {
          if (isEcosystemSlash || isNetworkSlash) {
            await this.updateSlashStatistics(
              trx,
              String(msg.id),
              isEcosystemSlash,
              isNetworkSlash,
              String(amountNum),
              null
            );
            this.logger.info(`[Slash] Updated slash statistics for permission ${msg.id} - ecosystem: ${isEcosystemSlash}, network: ${isNetworkSlash}, amount: ${amountNum}`);
          } else {
            this.logger.warn(`[Slash] Skipping slash statistics update for permission ${msg.id} - neither ecosystem nor network slash detected`);
          }
        } catch (statsErr: any) {
          this.logger.warn(`Failed to update slash statistics: ${statsErr?.message || statsErr}`);
        }

        try {
          await this.updateWeight(trx, String(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for permission ${String(msg.id)}:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, String(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "SLASH_PERMISSION_TRUST_DEPOSIT",
            height,
            perm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for slash:",
            historyErr
          );
         
        }
      });

      try {
        const schema = await knex("permissions").where({ id: msg.id }).first();
        const account = schema?.grantee;
        if (account) {
          await (this as any).broker.call(
            `${SERVICE.V1.TrustDepositDatabaseService.path}.slash_perm_trust_deposit`,
            {
              account,
              amount: String(amountNum),
              ts: now,
            }
          );
        }
      } catch (err) {
        this.logger.warn("TD processor slash call failed, continuing: ", err);
      }


      try {
        const slashedPerm = await knex("permissions").where({ id: msg.id }).first();
        if (slashedPerm?.schema_id) {
          const schemaId = Number(slashedPerm.schema_id);
          const schema = await knex("credential_schemas")
            .where("id", schemaId)
            .first();

          if (schema) {
            const csStats = await calculateCredentialSchemaStats(schemaId);
            await knex("credential_schemas")
              .where("id", schemaId)
              .update({
                participants: csStats.participants,
                weight: csStats.weight,
                issued: csStats.issued,
                verified: csStats.verified,
                ecosystem_slash_events: csStats.ecosystem_slash_events,
                ecosystem_slashed_amount: csStats.ecosystem_slashed_amount,
                ecosystem_slashed_amount_repaid: csStats.ecosystem_slashed_amount_repaid,
                network_slash_events: csStats.network_slash_events,
                network_slashed_amount: csStats.network_slashed_amount,
                network_slashed_amount_repaid: csStats.network_slashed_amount_repaid,
              });

            if (schema.tr_id) {
              const trStats = await calculateTrustRegistryStats(Number(schema.tr_id));
              await knex("trust_registry")
                .where("id", schema.tr_id)
                .update({
                  participants: trStats.participants,
                  active_schemas: trStats.active_schemas,
                  archived_schemas: trStats.archived_schemas,
                  weight: trStats.weight,
                  issued: trStats.issued,
                  verified: trStats.verified,
                  ecosystem_slash_events: trStats.ecosystem_slash_events,
                  ecosystem_slashed_amount: trStats.ecosystem_slashed_amount,
                  ecosystem_slashed_amount_repaid: trStats.ecosystem_slashed_amount_repaid,
                  network_slash_events: trStats.network_slash_events,
                  network_slashed_amount: trStats.network_slashed_amount,
                  network_slashed_amount_repaid: trStats.network_slashed_amount_repaid,
                });
            }
          }
        }
      } catch (statsErr: any) {
        this.logger.warn(` Failed to update CS/TR statistics after slash: ${statsErr?.message || String(statsErr)}`);
      }

      this.logger.info(
        `✅ Permission ${msg.id} slashed by ${caller} amount ${amountNum}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSlashPermissionTrustDeposit:", err);
      console.error("FATAL PERM SLASH ERROR:", err);
      return { success: false, reason: "Internal error slashing permission trust deposit" };
    }
  }

  private async handleRepayPermissionSlashedTrustDeposit(
    msg: MsgRepayPermissionSlashedTrustDeposit & { height?: number }
  ) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be a valid uint64.`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const slashedDeposit = Number(perm.slashed_deposit || 0);
      if (slashedDeposit <= 0) {
        this.logger.warn(
          `Permission ${msg.id} has no slashed deposit to repay`
        );
        return { success: false, reason: "No slashed deposit to repay" };
      }

      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      // Determine if this was ecosystem or network slash based on previous slash
      const isEcosystemPermission = perm.type === "ECOSYSTEM";
      let isEcosystemSlash = false;
      let isNetworkSlash = false;
      let trController: string | null = null;
      let classificationReason = '';
      
      if (isEcosystemPermission) {
        isNetworkSlash = true;
        classificationReason = 'ECOSYSTEM permission type';
      } else if (perm.slashed_by && perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();
        
        if (!schema) {
          this.logger.warn(`[Repay] Permission ${msg.id} has schema_id ${perm.schema_id} but schema not found`);
          classificationReason = `Schema ${perm.schema_id} not found`;
        } else if (!schema.tr_id) {
          this.logger.warn(`[Repay] Permission ${msg.id} schema ${perm.schema_id} has no tr_id`);
          classificationReason = `Schema ${perm.schema_id} has no tr_id`;
        } else {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();
          
          if (!tr) {
            this.logger.warn(`[Repay] Permission ${msg.id} TR ${schema.tr_id} not found`);
            classificationReason = `TR ${schema.tr_id} not found`;
            trController = null;
          } else {
            trController = tr.controller || null;
            if (!trController) {
              this.logger.warn(`[Repay] Permission ${msg.id} TR ${schema.tr_id} has no controller`);
              classificationReason = `TR ${schema.tr_id} has no controller`;
            } else if (tr.controller === perm.slashed_by) {
              isEcosystemSlash = true;
              classificationReason = `Slashed by TR controller ${perm.slashed_by}`;
            } else {
              classificationReason = `Slashed by ${perm.slashed_by} != TR controller ${tr.controller}`;
            }
          }
        }
      } else {
        classificationReason = perm.slashed_by ? 'No schema_id' : 'No slashed_by';
      }

      if (!isEcosystemSlash && !isNetworkSlash) {
        this.logger.error(`[Repay] CRITICAL: Could not classify repay for permission ${msg.id}. Classification reason: ${classificationReason}. Schema ID: ${perm.schema_id || 'N/A'}, Type: ${perm.type}, Slashed by: ${perm.slashed_by || 'N/A'}, TR Controller: ${trController || 'N/A'}`);
      }

      await knex.transaction(async (trx) => {
        const creator = requireController(msg, `PERM REPAY_SLASHED ${msg.id}`);
        const currentDeposit = BigInt(perm.deposit || "0");
        const repaidAmount = BigInt(slashedDeposit);
        const newDeposit = currentDeposit + repaidAmount;
        
        // Calculate expire_soon for repaid permission (may become active again)
        const repaidPermData = {
          ...perm,
          repaid: now,
          slashed: perm.slashed, // Keep original slashed timestamp
        };
        const expireSoon = await this.calculateExpireSoon(repaidPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          repaid: now,
          repaid_by: creator,
          repaid_deposit: String(slashedDeposit),
          deposit: String(newDeposit),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to repay permission ${msg.id} - update returned no record`
          );
         
        }

        // Update repaid amount in slash statistics
        try {
          await this.updateSlashStatistics(
            trx,
            String(msg.id),
            isEcosystemSlash,
            isNetworkSlash,
            "0", // No new slash, just repayment
            String(slashedDeposit)
          );
        } catch (statsErr: any) {
          this.logger.warn(`Failed to update slash statistics for repay: ${statsErr?.message || statsErr}`);
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "REPAY_PERMISSION_SLASHED_TRUST_DEPOSIT",
            height,
            perm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for repay:",
            historyErr
          );
         
        }

        try {
          await this.updateWeight(trx, String(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for permission ${String(msg.id)} after repay:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, String(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }
      });


      try {
        const repaidPerm = await knex("permissions").where({ id: msg.id }).first();
        if (repaidPerm?.schema_id) {
          const schemaId = Number(repaidPerm.schema_id);
          const schema = await knex("credential_schemas")
            .where("id", schemaId)
            .first();

          if (schema) {
            const csStats = await calculateCredentialSchemaStats(schemaId);
            await knex("credential_schemas")
              .where("id", schemaId)
              .update({
                participants: csStats.participants,
                weight: csStats.weight,
                issued: csStats.issued,
                verified: csStats.verified,
                ecosystem_slash_events: csStats.ecosystem_slash_events,
                ecosystem_slashed_amount: csStats.ecosystem_slashed_amount,
                ecosystem_slashed_amount_repaid: csStats.ecosystem_slashed_amount_repaid,
                network_slash_events: csStats.network_slash_events,
                network_slashed_amount: csStats.network_slashed_amount,
                network_slashed_amount_repaid: csStats.network_slashed_amount_repaid,
              });

            if (schema.tr_id) {
              const trStats = await calculateTrustRegistryStats(Number(schema.tr_id));
              await knex("trust_registry")
                .where("id", schema.tr_id)
                .update({
                  participants: trStats.participants,
                  active_schemas: trStats.active_schemas,
                  archived_schemas: trStats.archived_schemas,
                  weight: trStats.weight,
                  issued: trStats.issued,
                  verified: trStats.verified,
                  ecosystem_slash_events: trStats.ecosystem_slash_events,
                  ecosystem_slashed_amount: trStats.ecosystem_slashed_amount,
                  ecosystem_slashed_amount_repaid: trStats.ecosystem_slashed_amount_repaid,
                  network_slash_events: trStats.network_slash_events,
                  network_slashed_amount: trStats.network_slashed_amount,
                  network_slashed_amount_repaid: trStats.network_slashed_amount_repaid,
                });
            }
          }
        }
      } catch (statsErr: any) {
        this.logger.warn(` Failed to update CS/TR statistics after repay: ${statsErr?.message || String(statsErr)}`);
      }

      this.logger.info(
        `✅ Permission ${msg.id} slashed deposit (${slashedDeposit}) repaid by ${msg.creator}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error(
        "CRITICAL: Error in handleRepayPermissionSlashedTrustDeposit:",
        err
      );
      console.error("FATAL PERM REPAY ERROR:", err);
      return { success: false, reason: "Internal error repaying permission slashed trust deposit" };
    }
  }

  private permissionsColumnExistsCache: Record<string, boolean> | null = null;

  private async checkPermissionsColumnExists(columnName: string): Promise<boolean> {
    if (this.permissionsColumnExistsCache === null) {
      this.permissionsColumnExistsCache = {};
    }
    
    if (this.permissionsColumnExistsCache[columnName] !== undefined) {
      return this.permissionsColumnExistsCache[columnName];
    }
    
    try {
      const exists = await knex.schema.hasColumn("permissions", columnName);
      this.permissionsColumnExistsCache[columnName] = exists;
      if (columnName === "expire_soon") {
        this.logger.info(`[checkPermissionsColumnExists] expire_soon column exists: ${exists}`);
      }
      return exists;
    } catch (error: any) {
      this.logger.warn(`[checkPermissionsColumnExists] Error checking column ${columnName}:`, error?.message || error);
      this.permissionsColumnExistsCache[columnName] = false;
      return false;
    }
  }

  private async incrementPermissionStatistics(
    trx: any,
    permId: string,
    incrementIssued: boolean,
    incrementVerified: boolean
  ): Promise<void> {
    const hasIssuedColumn = await this.checkPermissionsColumnExists("issued");
    const hasVerifiedColumn = await this.checkPermissionsColumnExists("verified");
    
    if (!hasIssuedColumn && !hasVerifiedColumn) {
      this.logger.warn(`[incrementPermissionStatistics] Neither issued nor verified column exists for permission ${permId}`);
      return;
    }
    
    if (incrementIssued && !hasIssuedColumn) {
      this.logger.warn(`[incrementPermissionStatistics] Attempted to increment issued for permission ${permId} but issued column does not exist`);
    }
    
    if (incrementVerified && !hasVerifiedColumn) {
      this.logger.warn(`[incrementPermissionStatistics] Attempted to increment verified for permission ${permId} but verified column does not exist`);
    }
    
    const initialPerm: { schema_id: string; validator_perm_id: string | null } | undefined = await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id').first();
    if (!initialPerm) return;
    
    const schemaId = initialPerm.schema_id;
    let currentPermId: string | null = permId;
    
    while (currentPermId) {
      const perm: { schema_id: string; validator_perm_id: string | null } | undefined = await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }

      const updates: any = {};
      if (incrementIssued && hasIssuedColumn) {
        updates.issued = knex.raw("COALESCE(issued, 0) + 1");
      }
      if (incrementVerified && hasVerifiedColumn) {
        updates.verified = knex.raw("COALESCE(verified, 0) + 1");
      }

      if (Object.keys(updates).length > 0) {
        try {
          await trx("permissions")
            .where({ id: currentPermId })
            .update(updates);
        } catch (error: any) {
          if (error?.nativeError?.code === '42703') {
            this.permissionsColumnExistsCache = null;
            return;
          }
          throw error;
        }
      }

      currentPermId = perm.validator_perm_id;
    }
  }

  /**
   * Update slash statistics for a permission and all its ancestors
   * when a slash or repay event occurs
   */
  private async updateSlashStatistics(
    trx: any,
    permId: string,
    isEcosystemSlash: boolean,
    isNetworkSlash: boolean,
    slashAmount: string,
    repayAmount: string | null
  ): Promise<void> {
    const hasEcosystemSlashEventsColumn = await this.checkPermissionsColumnExists("ecosystem_slash_events");
    if (!hasEcosystemSlashEventsColumn) {
      this.logger.warn(`[updateSlashStatistics] Column ecosystem_slash_events does not exist, skipping update for permission ${permId}`);
      return;
    }
    
    if (!isEcosystemSlash && !isNetworkSlash) {
      this.logger.warn(`[updateSlashStatistics] Neither ecosystem nor network slash flag is set for permission ${permId}, skipping update`);
      return;
    }

    const initialPerm: { schema_id: string; validator_perm_id: string | null; type: string } | undefined = 
      await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id', 'type').first();
    if (!initialPerm) return;
    
    const schemaId = initialPerm.schema_id;
    let currentPermId: string | null = permId;
    
    while (currentPermId) {
      const perm: { schema_id: string; validator_perm_id: string | null } | undefined = 
        await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }

      const updates: any = {};
      
      if (isEcosystemSlash) {
        if (slashAmount !== "0") {
          updates.ecosystem_slash_events = knex.raw("COALESCE(ecosystem_slash_events, 0) + 1");
          updates.ecosystem_slashed_amount = knex.raw(`(COALESCE(ecosystem_slashed_amount, '0')::numeric + ${slashAmount}::numeric)::text`);
        }
        if (repayAmount) {
          updates.ecosystem_slashed_amount_repaid = knex.raw(`(COALESCE(ecosystem_slashed_amount_repaid, '0')::numeric + ${repayAmount}::numeric)::text`);
        }
      }
      
      if (isNetworkSlash) {
        if (slashAmount !== "0") {
          updates.network_slash_events = knex.raw("COALESCE(network_slash_events, 0) + 1");
          updates.network_slashed_amount = knex.raw(`(COALESCE(network_slashed_amount, '0')::numeric + ${slashAmount}::numeric)::text`);
        }
        if (repayAmount) {
          updates.network_slashed_amount_repaid = knex.raw(`(COALESCE(network_slashed_amount_repaid, '0')::numeric + ${repayAmount}::numeric)::text`);
        }
      }

      if (Object.keys(updates).length > 0) {
        try {
          const result = await trx("permissions")
            .where({ id: currentPermId })
            .update(updates);
          this.logger.debug(`[updateSlashStatistics] Updated permission ${currentPermId} with ${Object.keys(updates).length} fields, rows affected: ${result}`);
        } catch (error: any) {
          if (error?.nativeError?.code === '42703') {
            this.logger.warn(`[updateSlashStatistics] Column does not exist, clearing cache for permission ${currentPermId}`);
            this.permissionsColumnExistsCache = null;
            return;
          }
          throw error;
        }
      } else {
        this.logger.warn(`[updateSlashStatistics] No updates to apply for permission ${currentPermId} - isEcosystemSlash: ${isEcosystemSlash}, isNetworkSlash: ${isNetworkSlash}, slashAmount: ${slashAmount}, repayAmount: ${repayAmount}`);
      }

      currentPermId = perm.validator_perm_id;
    }
  }


  private async updateWeight(trx: any, permId: string): Promise<void> {
    const hasWeightColumn = await this.checkPermissionsColumnExists("weight");
    if (!hasWeightColumn) {
      return;
    }

    const initialPerm: { schema_id: string; validator_perm_id: string | null; deposit: string } | undefined = 
      await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id', 'deposit').first();
    if (!initialPerm) return;
    
    const schemaId = initialPerm.schema_id;
    let currentPermId: string | null = permId;
    
    const permStack: string[] = [];
    while (currentPermId) {
      permStack.push(currentPermId);
      const perm: { schema_id: string; validator_perm_id: string | null } | undefined = 
        await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }
      currentPermId = perm.validator_perm_id;
    }

    for (let i = permStack.length - 1; i >= 0; i--) {
      const pid = permStack[i];
      const perm = await trx("permissions").where({ id: pid }).select('deposit', 'schema_id').first();
      if (!perm) continue;

      const children = await trx("permissions")
        .where("validator_perm_id", pid)
        .where("schema_id", perm.schema_id)
        .select("weight");

      let childWeightSum = BigInt(0);
      for (const child of children) {
        const childWeight = child.weight ? BigInt(child.weight) : BigInt(0);
        childWeightSum += childWeight;
      }

      const ownDeposit = BigInt(perm.deposit || "0");
      const totalWeight = ownDeposit + childWeightSum;

      try {
        await trx("permissions")
          .where({ id: pid })
          .update({ weight: String(totalWeight) });
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.permissionsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    }
  }

  private async updateParticipants(trx: any, permId: string): Promise<void> {
    const hasParticipantsColumn = await this.checkPermissionsColumnExists("participants");
    if (!hasParticipantsColumn) {
      return;
    }

    const initialPerm: { schema_id: string; validator_perm_id: string | null } | undefined = 
      await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id').first();
    if (!initialPerm) return;
    
    const schemaId = initialPerm.schema_id;
    let currentPermId: string | null = permId;
    const permStack: string[] = [];
    
    while (currentPermId) {
      permStack.push(currentPermId);
      const perm: { schema_id: string; validator_perm_id: string | null } | undefined = 
        await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }
      currentPermId = perm.validator_perm_id;
    }

    const now = new Date();
    
    for (let i = permStack.length - 1; i >= 0; i--) {
      const pid = permStack[i];
      const perm = await trx("permissions").where({ id: pid }).select('repaid', 'slashed', 'revoked', 'effective_from', 'effective_until', 'type', 'vp_state', 'vp_exp', 'validator_perm_id', 'schema_id').first();
      if (!perm) continue;

      const permState = calculatePermState(
        {
          repaid: perm.repaid,
          slashed: perm.slashed,
          revoked: perm.revoked,
          effective_from: perm.effective_from,
          effective_until: perm.effective_until,
          type: perm.type,
          vp_state: perm.vp_state,
          vp_exp: perm.vp_exp,
          validator_perm_id: perm.validator_perm_id,
        },
        now
      );

      let count = permState === "ACTIVE" ? 1 : 0;

      const children = await trx("permissions")
        .where("validator_perm_id", pid)
        .where("schema_id", perm.schema_id)
        .select("repaid", "slashed", "revoked", "effective_from", "effective_until", "type", "vp_state", "vp_exp", "validator_perm_id", "participants");

      for (const child of children) {
        const childState = calculatePermState(
          {
            repaid: child.repaid,
            slashed: child.slashed,
            revoked: child.revoked,
            effective_from: child.effective_from,
            effective_until: child.effective_until,
            type: child.type,
            vp_state: child.vp_state,
            vp_exp: child.vp_exp,
            validator_perm_id: child.validator_perm_id,
          },
          now
        );

        if (childState === "ACTIVE") {
          count++;
        }

        const childParticipants = child.participants ? Number(child.participants) : 0;
        count += childParticipants;
      }

      try {
        await trx("permissions")
          .where({ id: pid })
          .update({ participants: count });
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.permissionsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    }
  }

  private async handleCreateOrUpdatePermissionSession(
    msg: MsgCreateOrUpdatePermissionSession & { height?: number }
  ) {
    const trx = await knex.transaction();
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      const agentPermId = (msg as any).agentPermId ?? (msg as any).agent_perm_id;
      const walletAgentPermId = (msg as any).walletAgentPermId ?? (msg as any).wallet_agent_perm_id;
      const issuerPermId = (msg as any).issuerPermId ?? (msg as any).issuer_perm_id;
      const verifierPermId = (msg as any).verifierPermId ?? (msg as any).verifier_perm_id;

      if (!msg.id || !agentPermId || !walletAgentPermId) {
        throw new Error("Missing mandatory parameters");
      }
      if (!issuerPermId && !verifierPermId) {
        throw new Error(
          "At least one of issuer_perm_id or verifier_perm_id must be provided"
        );
      }

      const [agentPerm, walletAgentPerm, issuerPerm, verifierPerm] =
        await Promise.all([
          trx("permissions").where({ id: agentPermId }).first(),
          trx("permissions").where({ id: walletAgentPermId }).first(),
          issuerPermId
            ? trx("permissions").where({ id: issuerPermId }).first()
            : null,
          verifierPermId
            ? trx("permissions").where({ id: verifierPermId }).first()
            : null,
        ]);

      if (!agentPerm) {
        this.logger.warn(
          `Agent permission not found for session ${msg.id}. agentPermId=${agentPermId}. Session will be saved but statistics may be incomplete.`
        );
      }
      if (!walletAgentPerm) {
        this.logger.warn(
          `Wallet Agent permission not found for session ${msg.id}. walletAgentPermId=${walletAgentPermId}. Session will be saved but statistics may be incomplete.`
        );
      }
      if (issuerPermId && issuerPerm && issuerPerm.type !== "ISSUER") {
        this.logger.warn(
          `Invalid issuer permission type for session ${msg.id}. Expected ISSUER, got ${issuerPerm.type}. issuerPermId=${issuerPermId}. Session will be saved.`
        );
      }
      if (verifierPermId && verifierPerm && verifierPerm.type !== "VERIFIER") {
        this.logger.warn(
          `Invalid verifier permission type for session ${msg.id}. Expected VERIFIER, got ${verifierPerm.type}. verifierPermId=${verifierPermId}. Session will be saved.`
        );
      }

      const existing = await trx("permission_sessions")
        .where({ id: msg.id })
        .first();
      const previousSession = existing
        ? {
            ...existing,
            authz: parseJson(existing.authz),
          }
        : undefined;
      const authzEntry = {
        issuer_perm_id: issuerPermId || null,
        verifier_perm_id: verifierPermId || null,
        wallet_agent_perm_id: walletAgentPermId,
      };

      if (!existing) {
        const creator = requireController(msg, `PERM SESSION ${msg.id}`);
        const [session] = await trx("permission_sessions")
          .insert({
            id: msg.id,
            controller: creator,
            agent_perm_id: agentPermId,
            wallet_agent_perm_id: walletAgentPermId,
            authz: JSON.stringify([authzEntry]),
            created: now,
            modified: now,
          })
          .returning("*");

        const normalizedSession =
          typeof session.authz === "string"
            ? { ...session, authz: parseJson(session.authz) }
            : session;

        await recordPermissionSessionHistory(
          trx,
          normalizedSession,
          "CREATE_PERMISSION_SESSION",
          height
        );

        if (issuerPermId) {
          try {
            await this.incrementPermissionStatistics(trx, String(issuerPermId), true, false);
          } catch (issuedErr: any) {
            this.logger.error(`[Session] Failed to increment issued for permission ${issuerPermId}:`, issuedErr?.message || issuedErr);
          }
        }
        if (verifierPermId) {
          try {
            await this.incrementPermissionStatistics(trx, String(verifierPermId), false, true);
          } catch (verifiedErr: any) {
            this.logger.error(`[Session] Failed to increment verified for permission ${verifierPermId}:`, verifiedErr?.message || verifiedErr);
          }
        }
      } else {
        let existingAuthz: any[] = [];
        try {
          existingAuthz = JSON.parse(existing.authz || "[]");
        } catch {
          existingAuthz = [];
        }

        existingAuthz.push(authzEntry);

        const [session] = await trx("permission_sessions")
          .where({ id: msg.id })
          .update({
            authz: JSON.stringify(existingAuthz),
            modified: now,
          })
          .returning("*");

        const normalizedSession =
          typeof session.authz === "string"
            ? { ...session, authz: parseJson(session.authz) }
            : session;

        await recordPermissionSessionHistory(
          trx,
          normalizedSession,
          "UPDATE_PERMISSION_SESSION",
          height,
          previousSession
        );

        const previousAuthz = previousSession?.authz || [];
        const previousIssuerPermIds = new Set(
          previousAuthz.map((entry: { issuer_perm_id?: string | number }) => entry.issuer_perm_id).filter(Boolean)
        );
        const previousVerifierPermIds = new Set(
          previousAuthz.map((entry: { verifier_perm_id?: string | number }) => entry.verifier_perm_id).filter(Boolean)
        );

        const newIssuerPermIds = new Set(
          existingAuthz.map((entry: { issuer_perm_id?: string | number }) => entry.issuer_perm_id).filter(Boolean)
        );
        const newVerifierPermIds = new Set(
          existingAuthz.map((entry: { verifier_perm_id?: string | number }) => entry.verifier_perm_id).filter(Boolean)
        );

        for (const issuerId of newIssuerPermIds) {
          if (!previousIssuerPermIds.has(issuerId)) {
            try {
              await this.incrementPermissionStatistics(trx, String(issuerId), true, false);
            } catch (issuedErr: any) {
              this.logger.error(`[Session] Failed to increment issued for permission ${issuerId}:`, issuedErr?.message || issuedErr);
            }
          }
        }
        for (const verifierId of newVerifierPermIds) {
          if (!previousVerifierPermIds.has(verifierId)) {
            try {
              await this.incrementPermissionStatistics(trx, String(verifierId), false, true);
            } catch (verifiedErr: any) {
              this.logger.error(`[Session] Failed to increment verified for permission ${verifierId}:`, verifiedErr?.message || verifiedErr);
            }
          }
        }
      }

      await trx.commit();
      return { success: true };
    } catch (err) {
      await trx.rollback();
      this.logger.error("Error in handleCreateOrUpdatePermissionSession:", err);
      return { success: false, reason: String(err) };
        
    }
  }

  private async queuePermissionForRetry(message: unknown, reason: string): Promise<void> {
    const msg = message as { id: string | number; type?: string };
    const key = `${msg.id}_${msg.type || 'permission'}`;
    const queuedPermission: QueuedPermission = {
      message: msg,
      reason,
      retryCount: 0,
      queuedAt: new Date(),
      nextRetryAt: new Date(Date.now() + this.RETRY_INTERVAL_MS)
    };

    this.retryQueue.set(key, queuedPermission);
    this.logger.info(`Queued permission ${msg.id} for retry: ${reason}`);
  }

  private startRetryProcessor(): void {
    this.retryInterval = setInterval(async () => {
      await this.processRetryQueue();
    }, this.RETRY_INTERVAL_MS);

    this.logger.info("Started permission retry processor");
  }

  private async processRetryQueue(): Promise<void> {
    const now = new Date();
    const keysToRetry: string[] = [];

    for (const [key, queuedPermission] of this.retryQueue.entries()) {
      if (queuedPermission.nextRetryAt <= now && queuedPermission.retryCount < this.MAX_RETRY_ATTEMPTS) {
        keysToRetry.push(key);
      }
    }

    for (const key of keysToRetry) {
      const queuedPermission = this.retryQueue.get(key)!;
      try {
        this.logger.info(`Retrying permission ${queuedPermission.message.id} (attempt ${queuedPermission.retryCount + 1}/${this.MAX_RETRY_ATTEMPTS})`);

        const result = await this.handleSetPermissionVPToValidated(queuedPermission.message);

        if (result && result.success !== false) {
          this.retryQueue.delete(key);
          this.logger.info(`Successfully processed queued permission ${queuedPermission.message.id}`);
        } else {
          queuedPermission.retryCount++;
          const delay = this.RETRY_INTERVAL_MS * (this.RETRY_BACKOFF_MULTIPLIER ** (queuedPermission.retryCount - 1));
          queuedPermission.nextRetryAt = new Date(Date.now() + delay);
          this.logger.warn(`Permission ${queuedPermission.message.id} still failed, scheduled next retry in ${delay}ms`);
        }
      } catch (error) {
        queuedPermission.retryCount++;
        const delay = this.RETRY_INTERVAL_MS * (this.RETRY_BACKOFF_MULTIPLIER ** (queuedPermission.retryCount - 1));
        queuedPermission.nextRetryAt = new Date(Date.now() + delay);
        this.logger.error(`Error retrying permission ${queuedPermission.message.id}:`, error);
      }
    }

    for (const [key, queuedPermission] of this.retryQueue.entries()) {
      if (queuedPermission.retryCount >= this.MAX_RETRY_ATTEMPTS) {
        this.retryQueue.delete(key);
        this.logger.error(`Permission ${queuedPermission.message.id} exceeded max retries (${this.MAX_RETRY_ATTEMPTS}), giving up`);
      }
    }

    if (this.retryQueue.size > 0) {
      this.logger.info(`Retry queue status: ${this.retryQueue.size} permissions waiting`);
    }
  }

  public async started(): Promise<void> {
    this.startRetryProcessor();
  }

  public async stopped(): Promise<void> {
    if (this.retryInterval) {
      clearInterval(this.retryInterval);
      this.retryInterval = null;
    }
    this.logger.info("Stopped permission retry processor");
  }
}
