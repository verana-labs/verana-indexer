import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE, ModulesParamsNamesTypes } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { getBlockHeight, hasBlockHeight } from "../../common/utils/blockHeight";
import { applyOrdering, validateSortParameter, sortByStandardAttributes } from "../../common/utils/query_ordering";
import { getModuleParams } from "../../common/utils/params_service";
import {
  calculatePermState,
  calculateGranteeAvailableActions,
  calculateValidatorAvailableActions,
  type SchemaData,
  type PermState,
} from "./perm_state_utils";
import { calculateCredentialSchemaStats } from "../crawl-cs/cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";

@Service({
  name: SERVICE.V1.PermAPIService.key,
  version: 1,
})
export default class PermAPIService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  private async batchEnrichPermissions(
    permissions: any[],
    blockHeight: number | undefined,
    now: Date,
    batchSize: number = 50
  ): Promise<any[]> {
    const results: any[] = [];
    for (let i = 0; i < permissions.length; i += batchSize) {
      const batch = permissions.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map(perm => this.enrichPermissionWithStateAndActions(perm, blockHeight, now))
      );
      results.push(...batchResults);
    }
    return results;
  }

  private async calculatePermissionWeight(
    permId: string,
    schemaId: string,
    blockHeight?: number,
    visited: Set<string> = new Set()
  ): Promise<string> {
    if (visited.has(permId)) {
      return "0";
    }
    visited.add(permId);

    const ownDeposit = permId ? await this.getPermissionDeposit(permId, schemaId, blockHeight) : "0";
    let ownDepositValue = BigInt(ownDeposit || "0");

    if (typeof blockHeight === "number") {
      const latestHistorySubquery = knex("permission_history")
        .select("permission_id")
        .select(
          knex.raw(
            `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC) as rn`
          )
        )
        .where("schema_id", String(schemaId))
        .where("height", "<=", blockHeight)
        .as("ranked");

      const children = await knex
        .from(latestHistorySubquery)
        .join("permission_history as ph", (join) => {
          join.on("ranked.permission_id", "=", "ph.permission_id")
            .andOn("ranked.rn", "=", knex.raw("1"));
        })
        .where("ph.validator_perm_id", permId)
        .select("ph.permission_id", "ph.deposit");

      const childWeights = await Promise.all(
        children.map(child =>
          this.calculatePermissionWeight(
            child.permission_id,
            schemaId,
            blockHeight,
            visited
          )
        )
      );

      for (const childWeight of childWeights) {
        ownDepositValue += BigInt(childWeight || "0");
      }
    } else {
      const children = await knex("permissions")
        .where("schema_id", String(schemaId))
        .where("validator_perm_id", permId)
        .select("id", "deposit");

      const childWeights = await Promise.all(
        children.map(child =>
          this.calculatePermissionWeight(
            String(child.id),
            schemaId,
            blockHeight,
            visited
          )
        )
      );

      for (const childWeight of childWeights) {
        ownDepositValue += BigInt(childWeight || "0");
      }
    }

    return ownDepositValue.toString();
  }

  private async getPermissionDeposit(
    permId: string,
    schemaId: string,
    blockHeight?: number
  ): Promise<string> {
    if (typeof blockHeight === "number") {
      const historyRecord = await knex("permission_history")
        .where("permission_id", permId)
        .where("schema_id", String(schemaId))
        .where("height", "<=", blockHeight)
        .orderBy("height", "desc")
        .orderBy("created_at", "desc")
        .first()
        .select("deposit");

      return historyRecord?.deposit || "0";
    }
    const permission = await knex("permissions")
      .where("id", permId)
      .where("schema_id", String(schemaId))
      .first()
      .select("deposit");

    return permission?.deposit || "0";
  }

  private async calculateExpireSoon(
    perm: any,
    now: Date,
    blockHeight?: number
  ): Promise<boolean | null> {
    const isActive = this.isPermissionActive(perm, now);
    if (!isActive) {
      return null;
    }
    if (!perm.effective_until) {
      return false;
    }
    let nDaysBefore = 0;
    try {
      const moduleParams = await getModuleParams(ModulesParamsNamesTypes.PERM, blockHeight);
      if (moduleParams?.params) {
        nDaysBefore = moduleParams.params.PERMISSION_SET_EXPIRE_SOON_N_DAYS_BEFORE || 0;
      }
    } catch (error) {
      this.logger.warn(`Failed to get PERMISSION module params:`, error);
      nDaysBefore = 0;
    }
    const expirationCheckDate = new Date(now);
    expirationCheckDate.setDate(expirationCheckDate.getDate() + nDaysBefore);
    const effectiveUntil = new Date(perm.effective_until);
    return expirationCheckDate > effectiveUntil;
  }

  private isPermissionActive(perm: any, now: Date = new Date()): boolean {
    const effectiveFrom = perm.effective_from ? new Date(perm.effective_from) : null;
    const effectiveUntil = perm.effective_until ? new Date(perm.effective_until) : null;
    if (effectiveFrom && now < effectiveFrom) return false;
    if (effectiveUntil && now > effectiveUntil) return false;
    if (perm.revoked) return false;
    if (perm.slashed && !perm.repaid) return false;

    return perm.vp_state === 'VALIDATED' || perm.type === 'ECOSYSTEM';
  }

  private async enrichPermissionWithStateAndActions(
    perm: any,
    blockHeight?: number,
    now: Date = new Date()
  ): Promise<any> {
    let schema: SchemaData;
    const schemaId = Number(perm.schema_id);

    if (typeof blockHeight === "number") {
      try {
        const schemaHistory = await knex("credential_schema_history")
          .where({ credential_schema_id: schemaId })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (schemaHistory) {
          schema = {
            issuer_perm_management_mode: schemaHistory.issuer_perm_management_mode || null,
            verifier_perm_management_mode: schemaHistory.verifier_perm_management_mode || null,
          };
        } else {
          const schemaMain = await knex("credential_schemas")
            .where({ id: schemaId })
            .first();
          schema = {
            issuer_perm_management_mode: schemaMain?.issuer_perm_management_mode || null,
            verifier_perm_management_mode: schemaMain?.verifier_perm_management_mode || null,
          };
        }
      } catch (error: any) {
        this.logger.warn(`credential_schema_history table doesn't have height column, using main table. Error: ${error?.message || error}`);
        const schemaMain = await knex("credential_schemas")
          .where({ id: schemaId })
          .first();
        schema = {
          issuer_perm_management_mode: schemaMain?.issuer_perm_management_mode || null,
          verifier_perm_management_mode: schemaMain?.verifier_perm_management_mode || null,
        };
      }
    } else {
      const schemaMain = await knex("credential_schemas")
        .where({ id: schemaId })
        .first();
      schema = {
        issuer_perm_management_mode: schemaMain?.issuer_perm_management_mode || null,
        verifier_perm_management_mode: schemaMain?.verifier_perm_management_mode || null,
      };
    }

    let validatorPermState: PermState | null = null;
    if (perm.validator_perm_id) {
      if (blockHeight !== undefined) {
        const validatorPermHistory = await knex("permission_history")
          .where({ permission_id: String(perm.validator_perm_id) })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (validatorPermHistory) {
          validatorPermState = calculatePermState(
            {
              repaid: validatorPermHistory.repaid,
              slashed: validatorPermHistory.slashed,
              revoked: validatorPermHistory.revoked,
              effective_from: validatorPermHistory.effective_from,
              effective_until: validatorPermHistory.effective_until,
              type: validatorPermHistory.type,
              vp_state: validatorPermHistory.vp_state,
              vp_exp: validatorPermHistory.vp_exp,
              validator_perm_id: validatorPermHistory.validator_perm_id,
            },
            now
          );
        }
      } else {
        const validatorPerm = await knex("permissions")
          .where({ id: String(perm.validator_perm_id) })
          .first();

        if (validatorPerm) {
          validatorPermState = calculatePermState(
            {
              repaid: validatorPerm.repaid,
              slashed: validatorPerm.slashed,
              revoked: validatorPerm.revoked,
              effective_from: validatorPerm.effective_from,
              effective_until: validatorPerm.effective_until,
              type: validatorPerm.type,
              vp_state: validatorPerm.vp_state,
              vp_exp: validatorPerm.vp_exp,
              validator_perm_id: validatorPerm.validator_perm_id,
            },
            now
          );
        }
      }
    }

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

    const granteeActions = calculateGranteeAvailableActions(
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
      schema,
      validatorPermState || undefined,
      now
    );

    const validatorActions = calculateValidatorAvailableActions(
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
      schema,
      now
    );

    if (blockHeight === undefined) {
      const [weight, statistics, participants, slashStats] = await Promise.all([
        perm.weight !== undefined && perm.weight !== null
          ? Promise.resolve(String(perm.weight || "0"))
          : this.calculatePermissionWeight(String(perm.id), String(perm.schema_id), blockHeight).catch((err: any) => {
              this.logger.warn(`Failed to calculate weight for permission ${perm.id}:`, err?.message || err);
              return "0";
            }),
        perm.issued !== undefined && perm.verified !== undefined && perm.issued !== null && perm.verified !== null
          ? Promise.resolve({ issued: Number(perm.issued || 0), verified: Number(perm.verified || 0) })
          : this.calculatePermissionStatistics(String(perm.id), String(perm.schema_id), blockHeight).catch((err: any) => {
              this.logger.warn(`Failed to calculate statistics for permission ${perm.id}:`, err?.message || err);
              return { issued: 0, verified: 0 };
            }),
        perm.participants !== undefined && perm.participants !== null
          ? Promise.resolve(Number(perm.participants || 0))
          : this.calculateParticipants(String(perm.id), String(perm.schema_id), permState, blockHeight, now).catch((err: any) => {
              this.logger.warn(`Failed to calculate participants for permission ${perm.id}:`, err?.message || err);
              return 0;
            }),
        perm.ecosystem_slash_events !== undefined && perm.ecosystem_slash_events !== null
          ? Promise.resolve({
              ecosystem_slash_events: Number(perm.ecosystem_slash_events || 0),
              ecosystem_slashed_amount: String(perm.ecosystem_slashed_amount || "0"),
              ecosystem_slashed_amount_repaid: String(perm.ecosystem_slashed_amount_repaid || "0"),
              network_slash_events: Number(perm.network_slash_events || 0),
              network_slashed_amount: String(perm.network_slashed_amount || "0"),
              network_slashed_amount_repaid: String(perm.network_slashed_amount_repaid || "0"),
            })
          : this.calculateSlashStatistics(String(perm.id), String(perm.schema_id), blockHeight).catch((err: any) => {
              this.logger.warn(`Failed to calculate slash statistics for permission ${perm.id}:`, err?.message || err);
              return {
                ecosystem_slash_events: 0,
                ecosystem_slashed_amount: "0",
                ecosystem_slashed_amount_repaid: "0",
                network_slash_events: 0,
                network_slashed_amount: "0",
                network_slashed_amount_repaid: "0",
              };
            }),
      ]);

      const expireSoon = await this.calculateExpireSoon(perm, now, blockHeight).catch((err: any) => {
        this.logger.warn(`Failed to calculate expire_soon for permission ${perm.id}:`, err?.message || err);
        return null;
      });

      return {
        ...perm,
        perm_state: permState,
        grantee_available_actions: granteeActions,
        validator_available_actions: validatorActions,
        weight: weight,
        issued: statistics.issued,
        verified: statistics.verified,
        participants: participants,
        ecosystem_slash_events: slashStats.ecosystem_slash_events,
        ecosystem_slashed_amount: slashStats.ecosystem_slashed_amount,
        ecosystem_slashed_amount_repaid: slashStats.ecosystem_slashed_amount_repaid,
        network_slash_events: slashStats.network_slash_events,
        network_slashed_amount: slashStats.network_slashed_amount,
        network_slashed_amount_repaid: slashStats.network_slashed_amount_repaid,
        expire_soon: expireSoon,
      };
    }

    const [weight, statistics, participants, slashStats] = await Promise.all([
      this.calculatePermissionWeight(String(perm.id), String(perm.schema_id), blockHeight).catch((err: any) => {
        this.logger.warn(`Failed to calculate weight for permission ${perm.id}:`, err?.message || err);
        return "0";
      }),
      this.calculatePermissionStatistics(String(perm.id), String(perm.schema_id), blockHeight).catch((err: any) => {
        this.logger.warn(`Failed to calculate statistics for permission ${perm.id}:`, err?.message || err);
        return { issued: "0", verified: "0" };
      }),
      this.calculateParticipants(String(perm.id), String(perm.schema_id), permState, blockHeight, now).catch((err: any) => {
        this.logger.warn(`Failed to calculate participants for permission ${perm.id}:`, err?.message || err);
        return 0;
      }),
      this.calculateSlashStatistics(String(perm.id), String(perm.schema_id), blockHeight).catch((err: any) => {
        this.logger.warn(`Failed to calculate slash statistics for permission ${perm.id}:`, err?.message || err);
        return {
          ecosystem_slash_events: 0,
          ecosystem_slashed_amount: "0",
          ecosystem_slashed_amount_repaid: "0",
          network_slash_events: 0,
          network_slashed_amount: "0",
          network_slashed_amount_repaid: "0",
        };
      }),
    ]);

    const expireSoon = await this.calculateExpireSoon(perm, now, blockHeight).catch((err: any) => {
      this.logger.warn(`Failed to calculate expire_soon for permission ${perm.id}:`, err?.message || err);
      return null;
    });

    return {
      ...perm,
      perm_state: permState,
      grantee_available_actions: granteeActions,
      validator_available_actions: validatorActions,
      weight: weight,
      issued: statistics.issued,
      verified: statistics.verified,
      participants: participants,
      ecosystem_slash_events: slashStats.ecosystem_slash_events,
      ecosystem_slashed_amount: slashStats.ecosystem_slashed_amount,
      ecosystem_slashed_amount_repaid: slashStats.ecosystem_slashed_amount_repaid,
      network_slash_events: slashStats.network_slash_events,
      network_slashed_amount: slashStats.network_slashed_amount,
      network_slashed_amount_repaid: slashStats.network_slashed_amount_repaid,
      expire_soon: expireSoon,
    };
  }

  private async calculatePermissionStatistics(
    permId: string,
    schemaId: string,
    blockHeight?: number
  ): Promise<{ issued: number; verified: number }> {
    try {
      const permissionIds = new Set<string>();
      let currentPermId: string | null = permId;

      if (blockHeight !== undefined) {
        while (currentPermId) {
          permissionIds.add(currentPermId);
          const permHistory: { validator_perm_id: string | null } | undefined = await knex("permission_history")
            .where("permission_id", currentPermId)
            .where("schema_id", String(schemaId))
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first()
            .select("validator_perm_id");

          currentPermId = permHistory?.validator_perm_id || null;
        }
      } else {
        while (currentPermId) {
          permissionIds.add(currentPermId);
          const perm: { validator_perm_id: string | null } | undefined = await knex("permissions")
            .where("id", currentPermId)
            .where("schema_id", String(schemaId))
            .first()
            .select("validator_perm_id");

          currentPermId = perm?.validator_perm_id || null;
        }
      }

      if (permissionIds.size === 0) {
        return { issued: 0, verified: 0 };
      }

      let issuedCount = BigInt(0);
      let verifiedCount = BigInt(0);

      if (blockHeight !== undefined) {
        const latestHistorySubquery = knex("permission_session_history")
          .select("session_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY height DESC, created_at DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const sessions = await knex
          .from(latestHistorySubquery)
          .join("permission_session_history as psh", (join) => {
            join.on("ranked.session_id", "=", "psh.session_id")
              .andOn("ranked.rn", "=", knex.raw("1"));
          })
          .select("psh.authz");

        for (const session of sessions) {
          const authz = typeof session.authz === "string" ? JSON.parse(session.authz) : session.authz;
          if (Array.isArray(authz)) {
            for (const entry of authz) {
              if (entry.issuer_perm_id && permissionIds.has(String(entry.issuer_perm_id))) {
                issuedCount += BigInt(1);
              }
              if (entry.verifier_perm_id && permissionIds.has(String(entry.verifier_perm_id))) {
                verifiedCount += BigInt(1);
              }
            }
          }
        }
      } else {
        const sessions = await knex("permission_sessions")
          .select("authz");

        for (const session of sessions) {
          const authz = typeof session.authz === "string" ? JSON.parse(session.authz) : session.authz;
          if (Array.isArray(authz)) {
            for (const entry of authz) {
              if (entry.issuer_perm_id && permissionIds.has(String(entry.issuer_perm_id))) {
                issuedCount += BigInt(1);
              }
              if (entry.verifier_perm_id && permissionIds.has(String(entry.verifier_perm_id))) {
                verifiedCount += BigInt(1);
              }
            }
          }
        }
      }

      // Convert BigInt to number for issued and verified (counts, not amounts)
      const issuedNumber = Number(issuedCount);
      const verifiedNumber = Number(verifiedCount);
      
      if (issuedNumber > Number.MAX_SAFE_INTEGER || verifiedNumber > Number.MAX_SAFE_INTEGER) {
        console.warn(`Warning: issued (${issuedCount}) or verified (${verifiedCount}) exceeds safe integer range for permission ${permId}`);
      }

      return {
        issued: issuedNumber,
        verified: verifiedNumber,
      };
    } catch (err: any) {
      const errorMsg = err?.message || String(err);
      if (errorMsg.includes("column") && (errorMsg.includes("does not exist") || errorMsg.includes("doesn't exist"))) {
        this.logger.warn(`Columns 'issued' or 'verified' do not exist yet. Migration may not have been run. Returning 0 for statistics.`);
        return { issued: 0, verified: 0 };
      }
      throw err;
    }
  }

 
  private async calculateParticipants(
    permId: string,
    schemaId: string,
    permState: PermState,
    blockHeight?: number,
    now: Date = new Date()
  ): Promise<number> {
    try {
      let count = 0;
      
      if (permState === "ACTIVE") {
        count = 1;
      }
      
      if (blockHeight !== undefined) {
        const latestHistorySubquery = knex("permission_history")
          .select("permission_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .where("schema_id", String(schemaId))
          .where("height", "<=", blockHeight)
          .as("ranked");

        const children = await knex
          .from(latestHistorySubquery)
          .join("permission_history as ph", (join) => {
            join.on("ranked.permission_id", "=", "ph.permission_id")
              .andOn("ranked.rn", "=", knex.raw("1"));
          })
          .where("ph.validator_perm_id", permId)
          .select("ph.permission_id", "ph.repaid", "ph.slashed", "ph.revoked", "ph.effective_from", "ph.effective_until", "ph.type", "ph.vp_state", "ph.vp_exp", "ph.validator_perm_id");

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
          
          const childCount = await this.calculateParticipants(
            String(child.permission_id),
            schemaId,
            childState,
            blockHeight,
            now
          );
          count += childCount;
        }
      } else {
        const children = await knex("permissions")
          .where("validator_perm_id", permId)
          .where("schema_id", String(schemaId))
          .select("id", "repaid", "slashed", "revoked", "effective_from", "effective_until", "type", "vp_state", "vp_exp", "validator_perm_id");

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
          
          const childCount = await this.calculateParticipants(
            String(child.id),
            schemaId,
            childState,
            blockHeight,
            now
          );
          count += childCount;
        }
      }

      return count;
    } catch (err: any) {
      this.logger.warn(`Failed to calculate participants for permission ${permId}:`, err?.message || err);
      return 0;
    }
  }


  private async calculateSlashStatistics(
    permId: string,
    schemaId: string,
    blockHeight?: number
  ): Promise<{
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: string;
    ecosystem_slashed_amount_repaid: string;
    network_slash_events: number;
    network_slashed_amount: string;
    network_slashed_amount_repaid: string;
  }> {
    try {
      const schema = await knex("credential_schemas")
        .where("id", String(schemaId))
        .first();
      
      let trController: string | null = null;
      if (schema?.tr_id) {
        const tr = await knex("trust_registry")
          .where("id", schema.tr_id)
          .first();
        trController = tr?.controller || null;
      }

      const permissionIds = new Set<string>();
      let currentPermId: string | null = permId;

      if (blockHeight !== undefined) {
        while (currentPermId) {
          permissionIds.add(currentPermId);
          const permHistory: { validator_perm_id: string | null; type: string } | undefined = await knex("permission_history")
            .where("permission_id", currentPermId)
            .where("schema_id", String(schemaId))
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first()
            .select("validator_perm_id", "type");

          currentPermId = permHistory?.validator_perm_id || null;
        }
      } else {
        while (currentPermId) {
          permissionIds.add(currentPermId);
          const perm: { validator_perm_id: string | null; type: string } | undefined = await knex("permissions")
            .where("id", currentPermId)
            .where("schema_id", String(schemaId))
            .first()
            .select("validator_perm_id", "type");

          currentPermId = perm?.validator_perm_id || null;
        }
      }

       let slashEvents: any[] = [];
      
      if (blockHeight !== undefined) {
        slashEvents = await knex("permission_history")
          .whereIn("permission_id", Array.from(permissionIds))
          .where("schema_id", String(schemaId))
          .where("height", "<=", blockHeight)
          .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
          .select("permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at")
          .orderBy("permission_id", "asc")
          .orderBy("height", "asc")
          .orderBy("created_at", "asc");
      } else {
        slashEvents = await knex("permission_history")
          .whereIn("permission_id", Array.from(permissionIds))
          .where("schema_id", String(schemaId))
          .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
          .select("permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at")
          .orderBy("permission_id", "asc")
          .orderBy("height", "asc")
          .orderBy("created_at", "asc");
      }

      let ecosystemSlashEvents = 0;
      let ecosystemSlashedAmount = BigInt(0);
      let ecosystemSlashedAmountRepaid = BigInt(0);
      let networkSlashEvents = 0;
      let networkSlashedAmount = BigInt(0);
      let networkSlashedAmountRepaid = BigInt(0);

      const prevSlashedDeposits = new Map<string, string>();
      const prevRepaidDeposits = new Map<string, string>();

      for (const event of slashEvents) {
        const permIdStr = String(event.permission_id);
        const prevSlashed = prevSlashedDeposits.get(permIdStr) || "0";
        const currentSlashed = event.slashed_deposit || "0";
        const incrementalSlashed = BigInt(currentSlashed) - BigInt(prevSlashed);
        
        if (incrementalSlashed <= 0) {
          prevSlashedDeposits.set(permIdStr, currentSlashed);
          const currentRepaid = event.repaid_deposit || "0";
          prevRepaidDeposits.set(permIdStr, currentRepaid);
          continue;
        }

        prevSlashedDeposits.set(permIdStr, currentSlashed);

        const isEcosystemPermission = event.type === "ECOSYSTEM";
        const isSlashedByEcosystemGov = trController && event.slashed_by === trController;

        if (isEcosystemPermission) {
          networkSlashEvents++;
          networkSlashedAmount += incrementalSlashed;
          
          const repaid = event.repaid_deposit || "0";
          const prevRepaid = prevRepaidDeposits.get(permIdStr) || "0";
          const incrementalRepaid = BigInt(repaid) - BigInt(prevRepaid);
          if (incrementalRepaid > 0) {
            networkSlashedAmountRepaid += incrementalRepaid;
          }
          prevRepaidDeposits.set(permIdStr, repaid);
        } else if (isSlashedByEcosystemGov) {
          ecosystemSlashEvents++;
          ecosystemSlashedAmount += incrementalSlashed;
          
          const repaid = event.repaid_deposit || "0";
          const prevRepaid = prevRepaidDeposits.get(permIdStr) || "0";
          const incrementalRepaid = BigInt(repaid) - BigInt(prevRepaid);
          if (incrementalRepaid > 0) {
            ecosystemSlashedAmountRepaid += incrementalRepaid;
          }
          prevRepaidDeposits.set(permIdStr, repaid);
        } else {
          const repaid = event.repaid_deposit || "0";
          prevRepaidDeposits.set(permIdStr, repaid);
        }
      }

      return {
        ecosystem_slash_events: ecosystemSlashEvents,
        ecosystem_slashed_amount: ecosystemSlashedAmount.toString(),
        ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid.toString(),
        network_slash_events: networkSlashEvents,
        network_slashed_amount: networkSlashedAmount.toString(),
        network_slashed_amount_repaid: networkSlashedAmountRepaid.toString(),
      };
    } catch (err: any) {
      this.logger.warn(`Failed to calculate slash statistics for permission ${permId}:`, err?.message || err);
      return {
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: "0",
        ecosystem_slashed_amount_repaid: "0",
        network_slash_events: 0,
        network_slashed_amount: "0",
        network_slashed_amount_repaid: "0",
      };
    }
  }

  /**
   * List Permissions [MOD-PERM-QRY-1]
   */
  @Action({
    rest: "GET list",
    params: {
      schema_id: { type: "number", integer: true, optional: true },
      grantee: { type: "string", optional: true },
      did: { type: "string", optional: true },
      perm_id: { type: "number", integer: true, optional: true },
      validator_perm_id: { type: "number", integer: true, optional: true },
      perm_state: { type: "string", optional: true },
      type: { type: "string", optional: true },

      only_valid: { type: "any", optional: true },
      only_slashed: { type: "any", optional: true },
      only_repaid: { type: "any", optional: true },

      modified_after: { type: "string", optional: true },
      country: { type: "string", optional: true },
      vp_state: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      when: { type: "string", optional: true },
      sort: { type: "string", optional: true },

      min_participants: { type: "number", integer: true, optional: true },
      max_participants: { type: "number", integer: true, optional: true },
      min_weight: { type: "string", optional: true },
      max_weight: { type: "string", optional: true },
      min_issued: { type: "string", optional: true },
      max_issued: { type: "string", optional: true },
      min_verified: { type: "string", optional: true },
      max_verified: { type: "string", optional: true },
      min_ecosystem_slash_events: { type: "number", integer: true, optional: true },
      max_ecosystem_slash_events: { type: "number", integer: true, optional: true },
      min_network_slash_events: { type: "number", integer: true, optional: true },
      max_network_slash_events: { type: "number", integer: true, optional: true },
    },
  })
  async listPermissions(ctx: Context<any>) {
    try {
      const p = ctx.params;
      const blockHeight = getBlockHeight(ctx);
      const now = new Date().toISOString();
      const limit = Math.min(Math.max(p.response_max_size || 64, 1), 1024);

      try {
        validateSortParameter(p.sort);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }

      const onlyValid = p.only_valid === "true" || p.only_valid === true;
      const onlySlashed = p.only_slashed === "true" || p.only_slashed === true;
      const onlyRepaid = p.only_repaid === "true" || p.only_repaid === true;

      if (hasBlockHeight(ctx) && blockHeight !== undefined) {
      const latestHistorySubquery = knex("permission_history")
          .select("permission_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight);

        if (p.schema_id !== undefined) {
          latestHistorySubquery.where("schema_id", String(p.schema_id));
        }

        latestHistorySubquery.as("ranked");

     const permIdsAtHeight = await knex
          .from(latestHistorySubquery)
          .select("permission_id")
          .where("rn", 1)
          .distinct()
          .then((rows: any[]) => rows.map((r: any) => r.permission_id));

        const totalPermsInTable = await knex("permissions").count("* as count").first();
        const totalHistoryEntries = await knex("permission_history")
          .where("height", "<=", blockHeight)
          .countDistinct("permission_id as count")
          .first();

        this.logger.info(`[listPermissions] Debug at height ${blockHeight}: Total permissions in table: ${totalPermsInTable?.count}, Unique permissions in history: ${totalHistoryEntries?.count}, Found permission IDs: ${permIdsAtHeight.length}`);

        if (permIdsAtHeight.length === 0) {
          this.logger.warn(`[listPermissions] No permission IDs found at height ${blockHeight}`);
          return ApiResponder.success(ctx, { permissions: [] }, 200);
        }

        this.logger.info(`[listPermissions] Found ${permIdsAtHeight.length} permission IDs at height ${blockHeight}: ${permIdsAtHeight.join(', ')}`);
        const permissions = await Promise.all(
          permIdsAtHeight.map(async (permId: string) => {
            const historyRecord = await knex("permission_history")
              .where({ permission_id: String(permId) })
              .where("height", "<=", blockHeight)
              .orderBy("height", "desc")
              .orderBy("created_at", "desc")
              .orderBy("id", "desc")
              .first();

            if (!historyRecord) {
              this.logger.warn(`[listPermissions] No history record found for permission ${permId} at height ${blockHeight}`);
              return null;
            }

            this.logger.debug(`[listPermissions] Permission ${permId} at height ${blockHeight}: repaid=${historyRecord.repaid}, slashed=${historyRecord.slashed}, height=${historyRecord.height}`);

            return {
              id: String(historyRecord.permission_id),
              schema_id: String(historyRecord.schema_id),
              grantee: historyRecord.grantee,
              did: historyRecord.did,
              created_by: historyRecord.created_by,
              validator_perm_id: historyRecord.validator_perm_id,
              type: historyRecord.type,
              country: historyRecord.country,
              vp_state: historyRecord.vp_state,
              revoked: historyRecord.revoked,
              revoked_by: historyRecord.revoked_by,
              slashed: historyRecord.slashed,
              slashed_by: historyRecord.slashed_by,
              repaid: historyRecord.repaid,
              repaid_by: historyRecord.repaid_by,
              extended: historyRecord.extended,
              extended_by: historyRecord.extended_by,
              effective_from: historyRecord.effective_from,
              effective_until: historyRecord.effective_until,
              validation_fees: historyRecord.validation_fees,
              issuance_fees: historyRecord.issuance_fees,
              verification_fees: historyRecord.verification_fees,
              deposit: historyRecord.deposit,
              slashed_deposit: historyRecord.slashed_deposit,
              repaid_deposit: historyRecord.repaid_deposit,
              vp_last_state_change: historyRecord.vp_last_state_change,
              vp_current_fees: historyRecord.vp_current_fees,
              vp_current_deposit: historyRecord.vp_current_deposit,
              vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
              vp_exp: historyRecord.vp_exp,
              vp_validator_deposit: historyRecord.vp_validator_deposit,
              vp_term_requested: historyRecord.vp_term_requested,
              created: historyRecord.created,
              modified: historyRecord.modified,
            };
          })
        );

        const validPermissions = permissions.filter((perm): perm is NonNullable<typeof permissions[0]> => perm !== null);
        this.logger.info(`[listPermissions] After filtering nulls: ${validPermissions.length} permissions`);

        let filteredPermissions = await this.batchEnrichPermissions(
          validPermissions,
          blockHeight,
          new Date(now),
          10
        );

        this.logger.info(`[listPermissions] After enrichment: ${filteredPermissions.length} permissions`);

        if (p.schema_id !== undefined) filteredPermissions = filteredPermissions.filter(perm => String(perm.schema_id) === String(p.schema_id));
        if (p.grantee) filteredPermissions = filteredPermissions.filter(perm => perm.grantee === p.grantee);
        if (p.did) filteredPermissions = filteredPermissions.filter(perm => perm.did === p.did);
        if (p.perm_id !== undefined) filteredPermissions = filteredPermissions.filter(perm => String(perm.validator_perm_id) === String(p.perm_id));
        if (p.validator_perm_id !== undefined) filteredPermissions = filteredPermissions.filter(perm => perm.validator_perm_id ? String(perm.validator_perm_id) === String(p.validator_perm_id) : false);
        if (p.type) filteredPermissions = filteredPermissions.filter(perm => perm.type === p.type);
        if (p.country) filteredPermissions = filteredPermissions.filter(perm => perm.country === p.country);
        if (p.vp_state) filteredPermissions = filteredPermissions.filter(perm => perm.vp_state === p.vp_state);
        if (p.perm_state) {
          const requestedState = String(p.perm_state).toUpperCase();
          filteredPermissions = filteredPermissions.filter(perm => perm.perm_state === requestedState);
        }

        if (p.modified_after) {
          const ts = new Date(p.modified_after);
          if (!Number.isNaN(ts.getTime())) {
            filteredPermissions = filteredPermissions.filter(perm => new Date(perm.modified) > ts);
          }
        }
        if (p.when) {
          const whenTs = new Date(p.when);
          if (!Number.isNaN(whenTs.getTime())) {
            filteredPermissions = filteredPermissions.filter(perm => new Date(perm.modified) <= whenTs);
          }
        }

        // Only apply filters when explicitly set to true
        if (onlyValid) {
          filteredPermissions = filteredPermissions.filter(perm => {
            const isNotRevoked = !perm.revoked;
            const isNotSlashedOrRepaid = !perm.slashed || perm.repaid;
            const isEffective = (!perm.effective_until || new Date(perm.effective_until) > new Date(now)) &&
              (!perm.effective_from || new Date(perm.effective_from) <= new Date(now));
            return isNotRevoked && isNotSlashedOrRepaid && isEffective;
          });
        }

        if (p.only_slashed !== undefined) {
        if (onlySlashed) {
          filteredPermissions = filteredPermissions.filter(perm => perm.slashed !== null);
          } else {
            filteredPermissions = filteredPermissions.filter(perm => perm.slashed === null);
          }
        }

        if (p.only_repaid !== undefined) {
        if (onlyRepaid) {
          filteredPermissions = filteredPermissions.filter(perm => perm.repaid !== null);
          } else {
            filteredPermissions = filteredPermissions.filter(perm => perm.repaid === null);
          }
        }

        if (p.perm_state) {
          const requestedState = String(p.perm_state).toUpperCase();
          filteredPermissions = filteredPermissions.filter(perm => perm.perm_state === requestedState);
        }

        if (p.min_participants !== undefined) {
          filteredPermissions = filteredPermissions.filter(perm => (perm.participants || 0) >= p.min_participants);
        }
        if (p.max_participants !== undefined) {
          filteredPermissions = filteredPermissions.filter(perm => (perm.participants || 0) < p.max_participants);
        }
        if (p.min_weight !== undefined) {
          const minWeight = BigInt(p.min_weight);
          filteredPermissions = filteredPermissions.filter(perm => BigInt(perm.weight || "0") >= minWeight);
        }
        if (p.max_weight !== undefined) {
          const maxWeight = BigInt(p.max_weight);
          filteredPermissions = filteredPermissions.filter(perm => BigInt(perm.weight || "0") < maxWeight);
        }
        if (p.min_issued !== undefined) {
          const minIssued = Number(p.min_issued);
          filteredPermissions = filteredPermissions.filter(perm => (perm.issued || 0) >= minIssued);
        }
        if (p.max_issued !== undefined) {
          const maxIssued = Number(p.max_issued);
          filteredPermissions = filteredPermissions.filter(perm => (perm.issued || 0) < maxIssued);
        }
        if (p.min_verified !== undefined) {
          const minVerified = Number(p.min_verified);
          filteredPermissions = filteredPermissions.filter(perm => (perm.verified || 0) >= minVerified);
        }
        if (p.max_verified !== undefined) {
          const maxVerified = Number(p.max_verified);
          filteredPermissions = filteredPermissions.filter(perm => (perm.verified || 0) < maxVerified);
        }
        if (p.min_ecosystem_slash_events !== undefined) {
          filteredPermissions = filteredPermissions.filter(perm => (perm.ecosystem_slash_events || 0) >= p.min_ecosystem_slash_events);
        }
        if (p.max_ecosystem_slash_events !== undefined) {
          filteredPermissions = filteredPermissions.filter(perm => (perm.ecosystem_slash_events || 0) < p.max_ecosystem_slash_events);
        }
        if (p.min_network_slash_events !== undefined) {
          filteredPermissions = filteredPermissions.filter(perm => (perm.network_slash_events || 0) >= p.min_network_slash_events);
        }
        if (p.max_network_slash_events !== undefined) {
          filteredPermissions = filteredPermissions.filter(perm => (perm.network_slash_events || 0) < p.max_network_slash_events);
        }

        filteredPermissions = sortByStandardAttributes(filteredPermissions, p.sort, {
          getId: (item) => Number(item.id),
          getCreated: (item) => item.created,
          getModified: (item) => item.modified,
          getParticipants: (item) => item.participants,
          getWeight: (item) => item.weight,
          getIssued: (item) => item.issued,
          getVerified: (item) => item.verified,
          getEcosystemSlashEvents: (item) => item.ecosystem_slash_events,
          getEcosystemSlashedAmount: (item) => item.ecosystem_slashed_amount,
          getNetworkSlashEvents: (item) => item.network_slash_events,
          getNetworkSlashedAmount: (item) => item.network_slashed_amount,
          defaultAttribute: "modified",
          defaultDirection: "desc",
        }).slice(0, limit);

        return ApiResponder.success(ctx, { permissions: filteredPermissions }, 200);
      }

      const baseColumns = [
        "id",
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
        "vp_current_fees",
        "vp_current_deposit",
        "vp_summary_digest_sri",
        "vp_exp",
        "vp_validator_deposit",
        "vp_term_requested",
      ];

      const hasIssuedColumn = await knex.schema.hasColumn("permissions", "issued");
      const hasVerifiedColumn = await knex.schema.hasColumn("permissions", "verified");
      const hasParticipantsColumn = await knex.schema.hasColumn("permissions", "participants");
      const hasWeightColumn = await knex.schema.hasColumn("permissions", "weight");
      const hasEcosystemSlashEventsColumn = await knex.schema.hasColumn("permissions", "ecosystem_slash_events");

      const selectColumns: any[] = [...baseColumns];
      
      if (hasIssuedColumn) {
        selectColumns.push(knex.raw("COALESCE(issued, 0) as issued"));
      }
      if (hasVerifiedColumn) {
        selectColumns.push(knex.raw("COALESCE(verified, 0) as verified"));
      }
      if (hasParticipantsColumn) {
        selectColumns.push(knex.raw("COALESCE(participants, 0) as participants"));
      }
      if (hasWeightColumn) {
        selectColumns.push(knex.raw("COALESCE(weight, '0') as weight"));
      }
      if (hasEcosystemSlashEventsColumn) {
        selectColumns.push(
          knex.raw("COALESCE(ecosystem_slash_events, 0) as ecosystem_slash_events"),
          knex.raw("COALESCE(ecosystem_slashed_amount, '0') as ecosystem_slashed_amount"),
          knex.raw("COALESCE(ecosystem_slashed_amount_repaid, '0') as ecosystem_slashed_amount_repaid"),
          knex.raw("COALESCE(network_slash_events, 0) as network_slash_events"),
          knex.raw("COALESCE(network_slashed_amount, '0') as network_slashed_amount"),
          knex.raw("COALESCE(network_slashed_amount_repaid, '0') as network_slashed_amount_repaid")
        );
      }

      const query = knex("permissions").select(selectColumns);

      if (p.schema_id !== undefined) query.where("schema_id", String(p.schema_id));
      if (p.grantee) query.where("grantee", p.grantee);
      if (p.did) query.where("did", p.did);
      if (p.perm_id !== undefined) query.where("validator_perm_id", String(p.perm_id));
      if (p.validator_perm_id !== undefined) {
        if (p.validator_perm_id === null || p.validator_perm_id === "null") {
          query.whereNull("validator_perm_id");
        } else {
          query.where("validator_perm_id", String(p.validator_perm_id));
        }
      }
      if (p.type) query.where("type", p.type);
      if (p.country) query.where("country", p.country);
      if (p.vp_state) query.where("vp_state", p.vp_state);

      if (p.modified_after) {
        const ts = new Date(p.modified_after);
        if (!Number.isNaN(ts.getTime()))
          query.where("modified", ">", ts.toISOString());
      }
      if (p.when) {
        const whenTs = new Date(p.when);
        if (!Number.isNaN(whenTs.getTime()))
          query.where("modified", "<=", whenTs.toISOString());
      }

      if (onlyValid) {
        query.where((qb) => {
          qb.whereNull("revoked")
            .andWhere((q) => q.whereNull("slashed").orWhereNotNull("repaid"))
            .andWhere((q) =>
              q
                .whereNull("effective_until")
                .orWhere("effective_until", ">", now)
            )
            .andWhere((q) =>
              q.whereNull("effective_from").orWhere("effective_from", "<=", now)
            );
        });
      }

      if (p.only_slashed !== undefined) {
        if (onlySlashed) query.whereNotNull("slashed");
        else query.whereNull("slashed");
      }

      if (p.only_repaid !== undefined) {
        if (onlyRepaid) query.whereNotNull("repaid");
        else query.whereNull("repaid");
      }

      const orderedQuery = applyOrdering(query, p.sort);
      const results = await orderedQuery.limit(limit);
      // Normalize IDs to strings and enrich with state and actions
      const normalizedResults = results.map(perm => ({
        ...perm,
        id: String(perm.id),
        schema_id: String(perm.schema_id),
        validator_perm_id: perm.validator_perm_id ? String(perm.validator_perm_id) : null,
      }));

      const enrichedResults = await this.batchEnrichPermissions(
        normalizedResults,
        blockHeight,
        new Date(now),
        10
      );

      let finalResults = enrichedResults;
      if (p.perm_state) {
        const requestedState = String(p.perm_state).toUpperCase();
        finalResults = enrichedResults.filter(perm => perm.perm_state === requestedState);
      }

      if (p.min_participants !== undefined && p.max_participants !== undefined && p.min_participants === p.max_participants) {
        // empty range when min === max for [min, max)
        finalResults = [];
      } else {
        if (p.min_participants !== undefined) {
          finalResults = finalResults.filter(perm => (perm.participants || 0) >= p.min_participants);
        }
        if (p.max_participants !== undefined) {
          finalResults = finalResults.filter(perm => (perm.participants || 0) < p.max_participants);
        }
      }
      if (p.min_weight !== undefined && p.max_weight !== undefined && p.min_weight === p.max_weight) {
        // empty range for weight
        finalResults = [];
      } else {
        if (p.min_weight !== undefined) {
          const minWeight = BigInt(p.min_weight);
          finalResults = finalResults.filter(perm => BigInt(perm.weight || "0") >= minWeight);
        }
        if (p.max_weight !== undefined) {
          const maxWeight = BigInt(p.max_weight);
          finalResults = finalResults.filter(perm => BigInt(perm.weight || "0") < maxWeight);
        }
      }
      if (p.min_issued !== undefined && p.max_issued !== undefined && p.min_issued === p.max_issued) {
        // empty range for issued
        finalResults = [];
      } else {
        if (p.min_issued !== undefined) {
          const minIssued = Number(p.min_issued);
          finalResults = finalResults.filter(perm => (perm.issued || 0) >= minIssued);
        }
        if (p.max_issued !== undefined) {
          const maxIssued = Number(p.max_issued);
          finalResults = finalResults.filter(perm => (perm.issued || 0) < maxIssued);
        }
      }
      if (p.min_verified !== undefined && p.max_verified !== undefined && p.min_verified === p.max_verified) {
        // empty range for verified
        finalResults = [];
      } else {
        if (p.min_verified !== undefined) {
          const minVerified = Number(p.min_verified);
          finalResults = finalResults.filter(perm => (perm.verified || 0) >= minVerified);
        }
        if (p.max_verified !== undefined) {
          const maxVerified = Number(p.max_verified);
          finalResults = finalResults.filter(perm => (perm.verified || 0) < maxVerified);
        }
      }
      if (p.min_ecosystem_slash_events !== undefined && p.max_ecosystem_slash_events !== undefined && p.min_ecosystem_slash_events === p.max_ecosystem_slash_events) {
        // empty range for ecosystem slash events
        finalResults = [];
      } else {
        if (p.min_ecosystem_slash_events !== undefined) {
          finalResults = finalResults.filter(perm => (perm.ecosystem_slash_events || 0) >= p.min_ecosystem_slash_events);
        }
        if (p.max_ecosystem_slash_events !== undefined) {
          finalResults = finalResults.filter(perm => (perm.ecosystem_slash_events || 0) < p.max_ecosystem_slash_events);
        }
      }
      if (p.min_network_slash_events !== undefined && p.max_network_slash_events !== undefined && p.min_network_slash_events === p.max_network_slash_events) {
        // empty range for network slash events
        finalResults = [];
      } else {
        if (p.min_network_slash_events !== undefined) {
          finalResults = finalResults.filter(perm => (perm.network_slash_events || 0) >= p.min_network_slash_events);
        }
        if (p.max_network_slash_events !== undefined) {
          finalResults = finalResults.filter(perm => (perm.network_slash_events || 0) < p.max_network_slash_events);
        }
      }

      finalResults = sortByStandardAttributes(finalResults, p.sort, {
        getId: (item) => Number(item.id),
        getCreated: (item) => item.created,
        getModified: (item) => item.modified,
        getParticipants: (item) => item.participants,
        getWeight: (item) => item.weight,
        getIssued: (item) => item.issued,
        getVerified: (item) => item.verified,
        getEcosystemSlashEvents: (item) => item.ecosystem_slash_events,
        getEcosystemSlashedAmount: (item) => item.ecosystem_slashed_amount,
        getNetworkSlashEvents: (item) => item.network_slash_events,
        getNetworkSlashedAmount: (item) => item.network_slashed_amount,
        defaultAttribute: "modified",
        defaultDirection: "asc",
      }).slice(0, limit);

      return ApiResponder.success(ctx, { permissions: finalResults }, 200);
    } catch (err: any) {
      this.logger.error("Error in listPermissions:", err);
      this.logger.error("Error details:", {
        message: err?.message,
        stack: err?.stack,
        code: err?.code,
      });
      return ApiResponder.error(ctx, `Failed to list permissions: ${err?.message || String(err)}`, 500);
    }
  }

  @Action({
    rest: "GET get/:id",
    params: {
      id: { type: "string", pattern: /^[0-9]+$/ },
    },
  })
  async getPermission(ctx: Context<{ id: string }>) {
    try {
      const id = ctx.params.id;
      const blockHeight = getBlockHeight(ctx);

      // If AtBlockHeight is provided, query historical state
      if (hasBlockHeight(ctx) && blockHeight !== undefined) {
        const historyRecord = await knex("permission_history")
          .where({ permission_id: String(id) })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, "Permission not found", 404);
        }

        // Map history record to permission format
        const historicalPermission = {
          id: String(historyRecord.permission_id),
          schema_id: String(historyRecord.schema_id),
          grantee: historyRecord.grantee,
          did: historyRecord.did,
          created_by: historyRecord.created_by,
          validator_perm_id: historyRecord.validator_perm_id ? String(historyRecord.validator_perm_id) : null,
          type: historyRecord.type,
          country: historyRecord.country,
          vp_state: historyRecord.vp_state,
          revoked: historyRecord.revoked,
          revoked_by: historyRecord.revoked_by,
          slashed: historyRecord.slashed,
          slashed_by: historyRecord.slashed_by,
          repaid: historyRecord.repaid,
          repaid_by: historyRecord.repaid_by,
          extended: historyRecord.extended,
          extended_by: historyRecord.extended_by,
          effective_from: historyRecord.effective_from,
          effective_until: historyRecord.effective_until,
          validation_fees: historyRecord.validation_fees,
          issuance_fees: historyRecord.issuance_fees,
          verification_fees: historyRecord.verification_fees,
          deposit: historyRecord.deposit,
          slashed_deposit: historyRecord.slashed_deposit,
          repaid_deposit: historyRecord.repaid_deposit,
          vp_last_state_change: historyRecord.vp_last_state_change,
          vp_current_fees: historyRecord.vp_current_fees,
          vp_current_deposit: historyRecord.vp_current_deposit,
          vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
          vp_exp: historyRecord.vp_exp,
          vp_validator_deposit: historyRecord.vp_validator_deposit,
          vp_term_requested: historyRecord.vp_term_requested,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        const enrichedPermission = await this.enrichPermissionWithStateAndActions(
          historicalPermission,
          blockHeight,
          new Date()
        );

        return ApiResponder.success(ctx, { permission: enrichedPermission }, 200);
      }

      // Otherwise, return latest state
      const permission = await knex("permissions").where("id", String(id)).first();
      if (!permission) {
        return ApiResponder.error(ctx, "Permission not found", 404);
      }
      // Normalize IDs to strings for consistency
      const normalizedPermission = {
        ...permission,
        id: String(permission.id),
        schema_id: String(permission.schema_id),
        validator_perm_id: permission.validator_perm_id ? String(permission.validator_perm_id) : null,
      };

      const enrichedPermission = await this.enrichPermissionWithStateAndActions(
        normalizedPermission,
        blockHeight,
        new Date()
      );

      return ApiResponder.success(ctx, { permission: enrichedPermission }, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermission:", err);
      return ApiResponder.error(ctx, "Failed to get permission", 500);
    }
  }

  @Action({
    rest: "GET history/:id",
    params: {
      id: { type: "string", pattern: /^[0-9]+$/ },
      response_max_size: { type: "number", optional: true, default: 64 },
      transaction_timestamp_older_than: { type: "string", optional: true },
    },
  })
  async getPermissionHistory(ctx: Context<{ id: string; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { id, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;
      const atBlockHeight = (ctx.meta as any)?.$headers?.["at-block-height"] || (ctx.meta as any)?.$headers?.["At-Block-Height"];

      const permissionExists = await knex("permissions").where({ id }).first();
      if (!permissionExists) {
        return ApiResponder.error(ctx, `Permission with id=${id} not found`, 404);
      }

      const { buildActivityTimeline } = await import("../../common/utils/activity_timeline_helper");
      const activity = await buildActivityTimeline(
        {
          entityType: "Permission",
          historyTable: "permission_history",
          idField: "permission_id",
          entityId: id,
          msgTypePrefixes: ["/verana.perm.v1"],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      );

      const result = {
        entity_type: "Permission",
        entity_id: String(id),
        activity: activity || [],
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionHistory:", err);
      this.logger.error("Error stack:", err?.stack);
      this.logger.error("Error details:", {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      });
      return ApiResponder.error(ctx, `Failed to get permission history: ${err?.message || "Unknown error"}`, 500);
    }
  }

  @Action({
    rest: "GET beneficiaries",
    params: {
      issuer_perm_id: { type: "number", integer: true, optional: true },
      verifier_perm_id: { type: "number", integer: true, optional: true },
    },
  })
  async findBeneficiaries(
    ctx: Context<{ issuer_perm_id?: number; verifier_perm_id?: number }>
  ) {
    const { issuer_perm_id: issuerPermId, verifier_perm_id: verifierPermId } =
      ctx.params;
    const blockHeight = getBlockHeight(ctx);

    if (!issuerPermId && !verifierPermId) {
      return ApiResponder.error(
        ctx,
        "issuer_perm_id or verifier_perm_id must be set",
        400
      );
    }

    const foundPermSet = new Set<any>();

    const loadPerm = async (permId: number | string) => {
      const permIdStr = String(permId);
      if (hasBlockHeight(ctx) && blockHeight !== undefined) {
        const historyRecord = await knex("permission_history")
          .where({ permission_id: permIdStr })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();
        if (!historyRecord) throw new Error(`Permission ${permIdStr} not found`);
        return {
          id: String(historyRecord.permission_id),
          schema_id: String(historyRecord.schema_id),
          grantee: historyRecord.grantee,
          did: historyRecord.did,
          created_by: historyRecord.created_by,
          validator_perm_id: historyRecord.validator_perm_id ? String(historyRecord.validator_perm_id) : null,
          type: historyRecord.type,
          country: historyRecord.country,
          vp_state: historyRecord.vp_state,
          revoked: historyRecord.revoked,
          revoked_by: historyRecord.revoked_by,
          slashed: historyRecord.slashed,
          slashed_by: historyRecord.slashed_by,
          repaid: historyRecord.repaid,
          repaid_by: historyRecord.repaid_by,
          extended: historyRecord.extended,
          extended_by: historyRecord.extended_by,
          effective_from: historyRecord.effective_from,
          effective_until: historyRecord.effective_until,
          validation_fees: historyRecord.validation_fees,
          issuance_fees: historyRecord.issuance_fees,
          verification_fees: historyRecord.verification_fees,
          deposit: historyRecord.deposit,
          slashed_deposit: historyRecord.slashed_deposit,
          repaid_deposit: historyRecord.repaid_deposit,
          vp_last_state_change: historyRecord.vp_last_state_change,
          vp_current_fees: historyRecord.vp_current_fees,
          vp_current_deposit: historyRecord.vp_current_deposit,
          vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
          vp_exp: historyRecord.vp_exp,
          vp_validator_deposit: historyRecord.vp_validator_deposit,
          vp_term_requested: historyRecord.vp_term_requested,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };
      }
      const perm = await knex("permissions").where("id", permIdStr).first();
      if (!perm) throw new Error(`Permission ${permIdStr} not found`);
      return {
        ...perm,
        id: String(perm.id),
        schema_id: String(perm.schema_id),
        validator_perm_id: perm.validator_perm_id ? String(perm.validator_perm_id) : null,
      };
    };

    const addAncestors = async (perm: any) => {
      let currentPerm = perm;
      while (currentPerm.validator_perm_id) {
        const parent = await loadPerm(String(currentPerm.validator_perm_id));
        if (!parent.revoked && !parent.slashed) {
          foundPermSet.add(parent);
        }
        currentPerm = parent;
      }
    };

    try {
      if (issuerPermId) {
        const issuerPerm = await loadPerm(issuerPermId);
        if (!verifierPermId) {
          await addAncestors(issuerPerm);
        }
      }

      if (verifierPermId) {
        const verifierPerm = await loadPerm(verifierPermId);
        if (issuerPermId) {
          const issuerPerm = await loadPerm(issuerPermId);
          foundPermSet.add(issuerPerm);
        }
        await addAncestors(verifierPerm);
      }

      // Enrich all permissions with state and actions
      const enrichedPermissions = await Promise.all(
        Array.from(foundPermSet).map(perm =>
          this.enrichPermissionWithStateAndActions(perm, blockHeight, new Date())
        )
      );

      return ApiResponder.success(ctx, { permissions: enrichedPermissions }, 200);
    } catch (err: any) {
      this.logger.error("Error in findBeneficiaries:", err);
      return ApiResponder.error(ctx, "Failed to find beneficiaries", 500);
    }
  }

  @Action({
    rest: "GET permission-session/:id",
    params: {
      id: { type: "string", pattern: /^[0-9a-fA-F-]+$/ },
    },
  })
  async getPermissionSession(ctx: Context<{ id: string }>) {
    try {
      const { id } = ctx.params;
      const blockHeight = getBlockHeight(ctx);

      // If AtBlockHeight is provided, query historical state
      if (hasBlockHeight(ctx) && blockHeight !== undefined) {
        const historyRecord = await knex("permission_session_history")
          .where({ session_id: id })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, "PermissionSession not found", 404);
        }

        // Map history record to session format
        const historicalSession = {
          id: historyRecord.session_id,
          controller: historyRecord.controller,
          agent_perm_id: historyRecord.agent_perm_id,
          wallet_agent_perm_id: historyRecord.wallet_agent_perm_id,
          authz: historyRecord.authz,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        return ApiResponder.success(ctx, { session: historicalSession }, 200);
      }

      // Otherwise, return latest state
      const session = await knex("permission_sessions").where("id", id).first();
      if (!session)
        return ApiResponder.error(ctx, "PermissionSession not found", 404);
      return ApiResponder.success(ctx, { session: session }, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionSession:", err);
      return ApiResponder.error(ctx, "Failed to get PermissionSession", 500);
    }
  }

  @Action({
    rest: "GET permission-session-history/:id",
    params: {
      id: { type: "string", pattern: /^[0-9a-fA-F-]+$/ },
      response_max_size: { type: "number", optional: true, default: 64 },
      transaction_timestamp_older_than: { type: "string", optional: true },
    },
  })
  async getPermissionSessionHistory(ctx: Context<{ id: string; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { id, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;
      const atBlockHeight = (ctx.meta as any)?.$headers?.["at-block-height"] || (ctx.meta as any)?.$headers?.["At-Block-Height"];

      const [currentSession, historySession] = await Promise.all([
        knex("permission_sessions").where({ id }).first(),
        knex("permission_session_history").where({ session_id: id }).first(),
      ]);
      if (!currentSession && !historySession) {
        return ApiResponder.error(ctx, `PermissionSession ${id} not found`, 404);
      }

      const { buildActivityTimeline } = await import("../../common/utils/activity_timeline_helper");
      const activity = await buildActivityTimeline(
        {
          entityType: "PERMISSION_SESSION",
          historyTable: "permission_session_history",
          idField: "session_id",
          entityId: id,
          msgTypePrefixes: ["/verana.perm.v1"],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      );

      const result = {
        entity_type: "PERMISSION_SESSION",
        entity_id: id,
        activity: activity || [],
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionSessionHistory:", err);
      this.logger.error("Error stack:", err?.stack);
      this.logger.error("Error details:", {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      });
      return ApiResponder.error(
        ctx,
        `Failed to get PermissionSession history: ${err?.message || "Unknown error"}`,
        500
      );
    }
  }

  @Action({
    rest: "GET permission-sessions",
    params: {
      modified_after: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      sort: { type: "string", optional: true },
    },
  })
  async listPermissionSessions(ctx: Context<any>) {
    try {
      const {
        modified_after: modifiedAfter,
        response_max_size: responseMaxSize,
        sort,
      } = ctx.params;

      try {
        validateSortParameter(sort);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }
      const blockHeight = getBlockHeight(ctx);
      const limit = Math.min(Math.max(responseMaxSize || 64, 1), 1024);

      // If AtBlockHeight is provided, query historical state
      if (hasBlockHeight(ctx) && blockHeight !== undefined) {
        // Get all unique session IDs that existed at or before the block height
        const latestHistorySubquery = knex("permission_session_history")
          .select("session_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY height DESC, created_at DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const sessionIdsAtHeight = await knex
          .from(latestHistorySubquery)
          .select("session_id")
          .where("rn", 1)
          .then((rows: any[]) => rows.map((r: any) => r.session_id));

        if (sessionIdsAtHeight.length === 0) {
          return ApiResponder.success(ctx, { sessions: [] }, 200);
        }

        // For each session, get the latest history record at or before block height
        const sessions = await Promise.all(
          sessionIdsAtHeight.map(async (sessionId: string) => {
            const historyRecord = await knex("permission_session_history")
              .where({ session_id: sessionId })
              .where("height", "<=", blockHeight)
              .orderBy("height", "desc")
              .orderBy("created_at", "desc")
              .first();

            if (!historyRecord) return null;

            return {
              id: historyRecord.session_id,
              controller: historyRecord.controller,
              agent_perm_id: historyRecord.agent_perm_id,
              wallet_agent_perm_id: historyRecord.wallet_agent_perm_id,
              authz: historyRecord.authz,
              created: historyRecord.created,
              modified: historyRecord.modified,
            };
          })
        );

        // Filter out nulls and apply filters
        let filteredSessions = sessions.filter((sess): sess is NonNullable<typeof sessions[0]> => sess !== null);

        if (modifiedAfter) {
          const ts = new Date(modifiedAfter);
          if (!Number.isNaN(ts.getTime())) {
            filteredSessions = filteredSessions.filter(sess => new Date(sess.modified) > ts);
          }
        }

        filteredSessions = sortByStandardAttributes(filteredSessions, sort, {
          getId: (item) => item.id,
          getCreated: (item) => item.created,
          getModified: (item) => item.modified,
          defaultAttribute: "modified",
          defaultDirection: "asc",
        }).slice(0, limit);

        return ApiResponder.success(ctx, { sessions: filteredSessions }, 200);
      }

      // Otherwise, return latest state
      const query = knex("permission_sessions").select("*");
      if (modifiedAfter) {
        const ts = new Date(modifiedAfter);
        if (!Number.isNaN(ts.getTime()))
          query.where("modified", ">", ts.toISOString());
      }

      const orderedQuery = applyOrdering(query, sort);
      const results = await orderedQuery.limit(limit);
      return ApiResponder.success(ctx, { sessions: results }, 200);
    } catch (err: any) {
      this.logger.error("Error in listPermissionSessions:", err);
      return ApiResponder.error(ctx, "Failed to list PermissionSessions", 500);
    }
  }

  @Action({
    rest: "GET pending/flat",
    params: {
      account: { type: "string" },
      response_max_size: { type: "number", optional: true, default: 64 },
      sort: { type: "string", optional: true },
    },
  })
  async pendingFlat(ctx: Context<{ account: string; response_max_size?: number; sort?: string }>) {
    try {
      const p = ctx.params as any;
      const account = p.account;
      if (!account) return ApiResponder.error(ctx, "Missing required parameter: account", 400);

      try {
        validateSortParameter(p.sort);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }

      const limit = Math.min(Math.max(p.response_max_size || 64, 1), 1024);
      const now = new Date();

      const blockHeight = getBlockHeight(ctx);
      const useHistory = hasBlockHeight(ctx) && blockHeight !== undefined;

      let parentIds: string[] = [];
      const parentIdSet = new Set<string>();

      if (useHistory) {
        const latestParentSub = knex("permission_history")
          .select("permission_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .andWhere("grantee", account)
          .as("ranked");

        parentIds = await knex
          .from(latestParentSub)
          .select("permission_id")
          .where("rn", 1)
          .then((rows: any[]) => rows.map((r: any) => String(r.permission_id)));
        for (const id of parentIds) parentIdSet.add(id);
      } else {
        const parentRows = await knex("permissions").select("id").where("grantee", account).limit(Math.max(limit * 10, 500));
        parentIds = Array.isArray(parentRows) ? parentRows.map((r: any) => String(r.id)) : [];
        for (const id of parentIds) parentIdSet.add(id);
      }

      const baseColumns = [
        "id",
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
        "vp_current_fees",
        "vp_current_deposit",
        "vp_summary_digest_sri",
        "vp_exp",
        "vp_validator_deposit",
        "vp_term_requested",
      ];

      let permissionsAtHeight: any[] = [];
      if (useHistory) {
        const latestSub = knex("permission_history")
          .select("permission_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const permIdsAtHeight = await knex
          .from(latestSub)
        .join("permission_history as ph", (join) => {
          join.on("ranked.permission_id", "=", "ph.permission_id")
            .andOn("ranked.rn", "=", knex.raw("1"));
        })
          .modify((qb) => {
            qb.where("ph.grantee", account);
            if (parentIds.length > 0) qb.orWhereIn("ph.validator_perm_id", parentIds);
          })
          .select("ph.permission_id")
          .then((rows: any[]) => rows.map((r: any) => String(r.permission_id)));

        if (permIdsAtHeight.length === 0) {
          return ApiResponder.success(ctx, { trust_registries: [] }, 200);
        }

        const joined = await knex
          .from(latestSub)
          .join("permission_history as ph", (join) => {
            join
              .on("ranked.permission_id", "=", "ph.permission_id")
              .andOn("ranked.rn", "=", knex.raw("1"));
          })
          .modify((qb) => {
            qb.where("ph.grantee", account);
            if (parentIds.length > 0) qb.orWhereIn("ph.validator_perm_id", parentIds);
          })
          .select(
            "ph.permission_id",
            "ph.schema_id",
            "ph.grantee",
            "ph.did",
            "ph.created_by",
            "ph.validator_perm_id",
            "ph.type",
            "ph.country",
            "ph.vp_state",
            "ph.revoked",
            "ph.revoked_by",
            "ph.slashed",
            "ph.slashed_by",
            "ph.repaid",
            "ph.repaid_by",
            "ph.extended",
            "ph.extended_by",
            "ph.effective_from",
            "ph.effective_until",
            "ph.validation_fees",
            "ph.issuance_fees",
            "ph.verification_fees",
            "ph.deposit",
            "ph.slashed_deposit",
            "ph.repaid_deposit",
            "ph.vp_last_state_change",
            "ph.vp_current_fees",
            "ph.vp_current_deposit",
            "ph.vp_summary_digest_sri",
            "ph.vp_exp",
            "ph.vp_validator_deposit",
            "ph.vp_term_requested",
            "ph.created",
            "ph.modified"
          )
          .orderBy("ph.permission_id", "asc");

        permissionsAtHeight = Array.isArray(joined)
          ? joined.map((historyRecord: any) => ({
              ...historyRecord,
              id: String(historyRecord.permission_id),
              schema_id: String(historyRecord.schema_id),
              validator_perm_id: historyRecord.validator_perm_id ? String(historyRecord.validator_perm_id) : null,
            }))
          : [];
      } else {
        const rows = await knex("permissions")
          .select(baseColumns)
          .where((qb) => {
            qb.where("grantee", account);
            if (parentIds.length > 0) qb.orWhereIn("validator_perm_id", parentIds);
          })
          .limit(Math.max(limit * 10, 500));
        permissionsAtHeight = Array.isArray(rows)
          ? rows.map((perm: any) => ({
              ...perm,
              id: String(perm.id),
              schema_id: String(perm.schema_id),
              validator_perm_id: perm.validator_perm_id ? String(perm.validator_perm_id) : null,
            }))
          : [];
      }

      const enriched = await this.batchEnrichPermissions(permissionsAtHeight, useHistory ? blockHeight : undefined, now, 50);
      const filtered = enriched.filter((perm: any) => {
        if (perm.grantee === account) {
          if (perm.vp_state === "PENDING") return true;
          if (perm.perm_state === "SLASHED") return true;
          if (perm.expire_soon === true) return true;
        }
        if (perm.validator_perm_id && parentIdSet.has(String(perm.validator_perm_id))) {
          if (perm.vp_state === "PENDING") return true;
          if (perm.perm_state === "SLASHED") return true;
        }
        return false;
      });
      filtered.sort((a: any, b: any) => {
        const ta = new Date(a.modified).getTime();
        const tb = new Date(b.modified).getTime();
        return ta - tb;
      });
      const schemaIds = Array.from(new Set(filtered.map((r: any) => String(r.schema_id))));
      const schemas = schemaIds.length > 0
        ? await knex("credential_schemas").whereIn("id", schemaIds).select("id", "tr_id", "json_schema", "title", "description", "participants")
        : [];
      const schemaMap = new Map<string, any>();
      for (const s of schemas) {
        let title: string | undefined;
        let description: string | undefined;
        const js = s.json_schema;
        let schemaObj: any = null;
        if (js) {
          if (typeof js === "string") {
            try { schemaObj = JSON.parse(js); } catch { schemaObj = null; }
          } else {
            schemaObj = js;
          }
        }
        if (schemaObj && typeof schemaObj === "object") {
          if (schemaObj.title && typeof schemaObj.title === "string") title = schemaObj.title;
          if (schemaObj.description && typeof schemaObj.description === "string") description = schemaObj.description;
        }
        schemaMap.set(String(s.id), { id: String(s.id), tr_id: s.tr_id !== null ? String(s.tr_id) : null, title, description, participants: s.participants ?? 0 });
      }

      if (useHistory && schemaMap.size > 0) {
        for (const [schemaId, cs] of schemaMap.entries()) {
          try {
            const stats = await calculateCredentialSchemaStats(Number(schemaId), blockHeight);
            cs.participants = stats.participants || 0;
            cs.weight = stats.weight;
          } catch (err: any) {
            this.logger.warn(`Failed to calculate stats for CS ${schemaId} at height ${blockHeight}: ${err?.message || err}`);
          }
        }
      }

      const trIds = Array.from(new Set(Array.from(schemaMap.values()).map((s: any) => s.tr_id).filter((x: any) => x !== null)));
      const trs = trIds.length > 0 ? await knex("trust_registry").whereIn("id", trIds).select("id", "did", "aka", "participants") : [];
      const trMap = new Map<string, any>();
      for (const tr of trs) {
        trMap.set(String(tr.id), { id: String(tr.id), did: tr.did, aka: tr.aka, credential_schemas: [], pending_tasks: 0, participants: tr.participants ?? 0 });
      }
      const csMap = new Map<string, any>();
      for (const perm of filtered) {
        const schemaId = String(perm.schema_id);
        const csInfo = schemaMap.get(schemaId) || { tr_id: null, title: undefined, description: undefined };
        if (!csMap.has(schemaId)) {
          csMap.set(schemaId, {
            id: schemaId,
            title: csInfo.title,
            description: csInfo.description,
            pending_tasks: 0,
            permissions: [],
          });
        }
        const entry = csMap.get(schemaId);
        entry.permissions.push({
          id: String(perm.id),
          type: perm.type,
          vp_state: perm.vp_state,
          perm_state: perm.perm_state,
          grantee: perm.grantee,
          did: perm.did,
          modified: perm.modified,
        });
        entry.pending_tasks++;
      }
      for (const [schemaId, csEntry] of csMap.entries()) {
        const csInfo = schemaMap.get(schemaId) || { tr_id: null };
        const trId = csInfo.tr_id ? String(csInfo.tr_id) : null;
        if (trId && trMap.has(trId)) {
          const trEntry = trMap.get(trId);
          trEntry.credential_schemas.push(csEntry);
          trEntry.pending_tasks += csEntry.pending_tasks;
        } else {
          const nullTrKey = "null";
          if (!trMap.has(nullTrKey)) {
            trMap.set(nullTrKey, { id: null, did: null, aka: null, credential_schemas: [], pending_tasks: 0 });
          }
          const trEntry = trMap.get(nullTrKey);
          trEntry.credential_schemas.push(csEntry);
          trEntry.pending_tasks += csEntry.pending_tasks;
        }
      }
      if (useHistory && trMap.size > 0) {
        for (const [trId, trEntry] of trMap.entries()) {
          if (trId === "null") continue;
          try {
            const stats = await calculateTrustRegistryStats(Number(trId), blockHeight);
            trEntry.participants = stats.participants || 0;
          } catch (err: any) {
            this.logger.warn(`Failed to calculate stats for TR ${trId} at height ${blockHeight}: ${err?.message || err}`);
            trEntry.participants = trEntry.participants || 0;
          }
        }
      }

      for (const trEntry of trMap.values()) {
        trEntry.credential_schemas.sort((a: any, b: any) => (b.participants || 0) - (a.participants || 0));
      }

      const trustRegistries = Array.from(trMap.values())
        .map((tr: any) => ({
          id: tr.id,
          did: tr.did,
          aka: tr.aka,
          pending_tasks: tr.pending_tasks,
          participants: tr.participants || 0,
          credential_schemas: tr.credential_schemas,
        }))
        .sort((a: any, b: any) => (b.participants || 0) - (a.participants || 0));

      return ApiResponder.success(ctx, { trust_registries: trustRegistries.slice(0, limit) }, 200);
    } catch (err: any) {
      this.logger.error("Error in pendingFlat:", err);
      return ApiResponder.error(ctx, `Failed to get pending tasks: ${err?.message || String(err)}`, 500);
    }
  }
}
