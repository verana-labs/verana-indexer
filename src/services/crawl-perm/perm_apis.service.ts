import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE, ModulesParamsNamesTypes } from "../../common";
import { validateParticipantParam, validateRequiredAccountParam } from "../../common/utils/accountValidation";
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

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || "").includes("pg");

@Service({
  name: SERVICE.V1.PermAPIService.key,
  version: 1,
})
export default class PermAPIService extends BullableService {
  private static readonly LIST_PERMISSIONS_SLOW_MS = 200;
  private static readonly VALID_PERMISSION_TYPES = new Set([
    "ECOSYSTEM",
    "ISSUER_GRANTOR",
    "VERIFIER_GRANTOR",
    "ISSUER",
    "VERIFIER",
    "HOLDER",
  ]);
  private static readonly VALID_VP_STATES = new Set([
    "VALIDATION_STATE_UNSPECIFIED",
    "PENDING",
    "VALIDATED",
    "TERMINATED",
  ]);

  constructor(broker: ServiceBroker) {
    super(broker);
  }

  private async getMetricColumnAvailability(tableName: "permissions" | "permission_history"): Promise<{
    hasIssuedColumn: boolean;
    hasVerifiedColumn: boolean;
    hasParticipantsColumn: boolean;
    hasWeightColumn: boolean;
    hasEcosystemSlashEventsColumn: boolean;
  }> {
    const columnInfo = await knex(tableName).columnInfo();
    return {
      hasIssuedColumn: !!columnInfo.issued,
      hasVerifiedColumn: !!columnInfo.verified,
      hasParticipantsColumn: !!columnInfo.participants,
      hasWeightColumn: !!columnInfo.weight,
      hasEcosystemSlashEventsColumn: !!columnInfo.ecosystem_slash_events,
    };
  }

  private shouldUseHistoryQuery(ctx: Context<any>, blockHeight: number | undefined): boolean {
    if (!hasBlockHeight(ctx) || blockHeight === undefined) return false;
    const latestCheckpointHeight = Number((ctx.meta as any)?.latestCheckpoint?.height);
    if (Number.isFinite(latestCheckpointHeight) && latestCheckpointHeight > 0) {
      return blockHeight < latestCheckpointHeight;
    }
    return true;
  }

  private async getSchemaModes(schemaId: number, blockHeight?: number): Promise<SchemaData> {
    if (typeof blockHeight === "number") {
      try {
        const schemaHistory = await knex("credential_schema_history")
          .where({ credential_schema_id: schemaId })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (schemaHistory) {
          return {
            issuer_perm_management_mode: schemaHistory.issuer_perm_management_mode || null,
            verifier_perm_management_mode: schemaHistory.verifier_perm_management_mode || null,
          };
        }
      } catch (error: any) {
        this.logger.warn(`credential_schema_history table doesn't have height column, using main table. Error: ${error?.message || error}`);
      }
    }

    const schemaMain = await knex("credential_schemas")
      .where({ id: schemaId })
      .first();

    const schema: SchemaData = {
      issuer_perm_management_mode: schemaMain?.issuer_perm_management_mode || null,
      verifier_perm_management_mode: schemaMain?.verifier_perm_management_mode || null,
    };

    return schema;
  }

  private async getPermissionModuleParams(blockHeight?: number): Promise<any> {
    return getModuleParams(ModulesParamsNamesTypes.PERM, blockHeight);
  }

  private isTrustResolutionListQuery(
    params: any,
    _blockHeight: number | undefined
  ): boolean {
    if (!params?.did) return false;
    if (params?.schema_id === undefined || params?.schema_id === null) return false;

    const expensiveMetricFilters = [
      "min_participants",
      "max_participants",
      "min_weight",
      "max_weight",
      "min_issued",
      "max_issued",
      "min_verified",
      "max_verified",
      "min_ecosystem_slash_events",
      "max_ecosystem_slash_events",
      "min_network_slash_events",
      "max_network_slash_events",
    ];
    const hasExpensiveMetricFilter = expensiveMetricFilters.some((k) => params[k] !== undefined);
    if (hasExpensiveMetricFilter) return false;

    if (typeof params?.sort === "string") {
      const sortRaw = params.sort.toLowerCase();
      const expensiveSortKeys = [
        "participants",
        "weight",
        "issued",
        "verified",
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "network_slash_events",
        "network_slashed_amount",
      ];
      if (expensiveSortKeys.some((key) => sortRaw.includes(key))) {
        return false;
      }
    }

    return true;
  }

  private usesDerivedMetricSort(sort: any): boolean {
    if (typeof sort !== "string") return false;
    const sortRaw = sort.toLowerCase();
    const derivedSortKeys = [
      "participants",
      "weight",
      "issued",
      "verified",
      "ecosystem_slash_events",
      "ecosystem_slashed_amount",
      "network_slash_events",
      "network_slashed_amount",
    ];
    return derivedSortKeys.some((key) => sortRaw.includes(key));
  }

  private shouldUseStrictTrustResolutionLightweightMode(params: any, limit: number): boolean {
    if (!params?.did) return false;
    if (params?.schema_id === undefined || params?.schema_id === null) return false;
    if (limit > 32) return false;

    const disallowedKeys = [
      "grantee",
      "perm_id",
      "validator_perm_id",
      "perm_state",
      "type",
      "only_valid",
      "only_slashed",
      "only_repaid",
      "modified_after",
      "country",
      "vp_state",
      "when",
      "min_participants",
      "max_participants",
      "min_weight",
      "max_weight",
      "min_issued",
      "max_issued",
      "min_verified",
      "max_verified",
      "min_ecosystem_slash_events",
      "max_ecosystem_slash_events",
      "min_network_slash_events",
      "max_network_slash_events",
    ];
    return disallowedKeys.every((key) => params[key] === undefined);
  }

  private normalizeAndValidateTypeAndVpState(params: any): {
    ok: boolean;
    message?: string;
    type?: string;
    vp_state?: string;
  } {
    const normalizedType = typeof params?.type === "string" ? params.type.toUpperCase() : undefined;
    const normalizedVpState = typeof params?.vp_state === "string" ? params.vp_state.toUpperCase() : undefined;
    let finalType = normalizedType;
    let finalVpState = normalizedVpState;

    if (normalizedType && !normalizedVpState
      && !PermAPIService.VALID_PERMISSION_TYPES.has(normalizedType)
      && PermAPIService.VALID_VP_STATES.has(normalizedType)) {
      finalVpState = normalizedType;
      finalType = undefined;
    }

    if (finalType !== undefined && !PermAPIService.VALID_PERMISSION_TYPES.has(finalType)) {
      return {
        ok: false,
        message: `Invalid type '${finalType}'. Allowed values: ${Array.from(PermAPIService.VALID_PERMISSION_TYPES).join(", ")}`,
      };
    }

    if (finalVpState !== undefined && !PermAPIService.VALID_VP_STATES.has(finalVpState)) {
      return {
        ok: false,
        message: `Invalid vp_state '${finalVpState}'. Allowed values: ${Array.from(PermAPIService.VALID_VP_STATES).join(", ")}`,
      };
    }

    return {
      ok: true,
      type: finalType,
      vp_state: finalVpState,
    };
  }

  private applyBaseListFiltersToQuery(
    query: any,
    params: any,
    granteeFilter: string | undefined,
    modifiedAfterIso: string | undefined,
    whenIso: string | undefined,
    onlyValid: boolean,
    onlySlashed: boolean,
    onlyRepaid: boolean,
    nowIso: string,
    tablePrefix?: string,
    permissionIdColumn: string = "id"
  ): void {
    const col = (name: string) => (tablePrefix ? `${tablePrefix}.${name}` : name);

    if (params.schema_id !== undefined) query.where(col("schema_id"), Number(params.schema_id));
    if (granteeFilter) query.where(col("grantee"), granteeFilter);
    if (params.did) query.where(col("did"), params.did);
    if (params.perm_id !== undefined) query.where(col(permissionIdColumn), Number(params.perm_id));
    if (params.validator_perm_id !== undefined) {
      if (params.validator_perm_id === null || params.validator_perm_id === "null") {
        query.whereNull(col("validator_perm_id"));
      } else {
        query.where(col("validator_perm_id"), Number(params.validator_perm_id));
      }
    }
    if (params.type) query.where(col("type"), params.type);
    if (params.country) query.where(col("country"), params.country);
    if (params.vp_state) query.where(col("vp_state"), params.vp_state);
    if (modifiedAfterIso) query.where(col("modified"), ">", modifiedAfterIso);
    if (whenIso) query.where(col("modified"), "<=", whenIso);

    if (onlyValid) {
      query.where((qb: any) => {
        qb.whereNull(col("revoked"))
          .andWhere((q: any) => q.whereNull(col("slashed")).orWhereNotNull(col("repaid")))
          .andWhere((q: any) => q.whereNull(col("effective_until")).orWhere(col("effective_until"), ">", nowIso))
          .andWhere((q: any) => q.whereNull(col("effective_from")).orWhere(col("effective_from"), "<=", nowIso));
      });
    }

    if (params.only_slashed !== undefined) {
      if (onlySlashed) query.whereNotNull(col("slashed"));
      else query.whereNull(col("slashed"));
    }

    if (params.only_repaid !== undefined) {
      if (onlyRepaid) query.whereNotNull(col("repaid"));
      else query.whereNull(col("repaid"));
    }
  }

  private applyMetricFiltersToSql(
    query: any,
    params: any,
    options: {
      participants: boolean;
      weight: boolean;
      issued: boolean;
      verified: boolean;
      slashStats: boolean;
      tablePrefix?: string;
    }
  ): { requiresPostFilter: boolean; impossibleRange: boolean } {
    const col = (name: string) => (options.tablePrefix ? `${options.tablePrefix}.${name}` : name);
    const metricSpecs = [
      { min: "min_participants", max: "max_participants", db: "participants", enabled: options.participants },
      { min: "min_weight", max: "max_weight", db: "weight", enabled: options.weight },
      { min: "min_issued", max: "max_issued", db: "issued", enabled: options.issued },
      { min: "min_verified", max: "max_verified", db: "verified", enabled: options.verified },
      { min: "min_ecosystem_slash_events", max: "max_ecosystem_slash_events", db: "ecosystem_slash_events", enabled: options.slashStats },
      { min: "min_network_slash_events", max: "max_network_slash_events", db: "network_slash_events", enabled: options.slashStats },
    ];

    let requiresPostFilter = false;
    let impossibleRange = false;

    for (const spec of metricSpecs) {
      const minRaw = params[spec.min];
      const maxRaw = params[spec.max];
      if (minRaw === undefined && maxRaw === undefined) continue;

      if (!spec.enabled) {
        requiresPostFilter = true;
        continue;
      }

      const minValue = minRaw !== undefined ? Number(minRaw) : undefined;
      const maxValue = maxRaw !== undefined ? Number(maxRaw) : undefined;
      if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
        query.whereRaw("1 = 0");
        impossibleRange = true;
        continue;
      }
      if (minValue !== undefined) query.where(col(spec.db), ">=", minValue);
      if (maxValue !== undefined) query.where(col(spec.db), "<", maxValue);
    }

    return { requiresPostFilter, impossibleRange };
  }

  private applyMetricFiltersInMemory(permissions: any[], params: any): any[] {
    const specs = [
      { min: "min_participants", max: "max_participants", field: "participants" },
      { min: "min_weight", max: "max_weight", field: "weight" },
      { min: "min_issued", max: "max_issued", field: "issued" },
      { min: "min_verified", max: "max_verified", field: "verified" },
      { min: "min_ecosystem_slash_events", max: "max_ecosystem_slash_events", field: "ecosystem_slash_events" },
      { min: "min_network_slash_events", max: "max_network_slash_events", field: "network_slash_events" },
    ];

    let results = permissions;
    for (const spec of specs) {
      const minRaw = params[spec.min];
      const maxRaw = params[spec.max];
      if (minRaw === undefined && maxRaw === undefined) continue;

      const minValue = minRaw !== undefined ? Number(minRaw) : undefined;
      const maxValue = maxRaw !== undefined ? Number(maxRaw) : undefined;
      if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
        return [];
      }
      if (minValue !== undefined) {
        results = results.filter((perm) => Number(perm?.[spec.field] || 0) >= minValue);
      }
      if (maxValue !== undefined) {
        results = results.filter((perm) => Number(perm?.[spec.field] || 0) < maxValue);
      }
    }
    return results;
  }

  private applyPermStateFilterToQuery(
    query: any,
    permStateRaw: any,
    nowIso: string,
    tablePrefix?: string
  ): { pushedDown: boolean } {
    if (!permStateRaw) return { pushedDown: false };

    const permState = String(permStateRaw).toUpperCase();
    const col = (name: string) => (tablePrefix ? `${tablePrefix}.${name}` : name);
    const baseNotRepaidSlashed = (qb: any) => {
      qb.whereNull(col("repaid")).whereNull(col("slashed"));
    };
    const notRevokedAsOfNow = (qb: any) => {
      qb.whereNull(col("revoked")).orWhere(col("revoked"), ">=", nowIso);
    };

    if (permState === "REPAID") {
      query.whereNotNull(col("repaid"));
      return { pushedDown: true };
    }

    if (permState === "SLASHED") {
      query.whereNull(col("repaid")).whereNotNull(col("slashed"));
      return { pushedDown: true };
    }

    if (permState === "REVOKED") {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb);
        qb.whereNotNull(col("revoked")).andWhere(col("revoked"), "<", nowIso);
      });
      return { pushedDown: true };
    }

    if (permState === "EXPIRED") {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb);
        qb.where(notRevokedAsOfNow);
        qb.whereNotNull(col("effective_until")).andWhere(col("effective_until"), "<", nowIso);
      });
      return { pushedDown: true };
    }

    if (permState === "ACTIVE") {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb);
        qb.where(notRevokedAsOfNow);
        qb.where((q: any) => q.whereNull(col("effective_until")).orWhere(col("effective_until"), ">=", nowIso));
        qb.whereNotNull(col("effective_from")).andWhere(col("effective_from"), "<=", nowIso);
      });
      return { pushedDown: true };
    }

    if (permState === "FUTURE") {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb);
        qb.where(notRevokedAsOfNow);
        qb.where((q: any) => q.whereNull(col("effective_until")).orWhere(col("effective_until"), ">=", nowIso));
        qb.whereNotNull(col("effective_from")).andWhere(col("effective_from"), ">", nowIso);
      });
      return { pushedDown: true };
    }

    if (permState === "INACTIVE") {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb);
        qb.where(notRevokedAsOfNow);
        qb.where((q: any) => q.whereNull(col("effective_until")).orWhere(col("effective_until"), ">=", nowIso));
        qb.whereNull(col("effective_from"));
      });
      return { pushedDown: true };
    }

    return { pushedDown: false };
  }

  private async batchEnrichPermissions(
    permissions: any[],
    blockHeight: number | undefined,
    now: Date,
    batchSize: number = 50,
    options?: {
      lightweightDerivedStats?: boolean;
      schemaModesById?: Map<number, SchemaData>;
      validatorPermStateById?: Map<number, PermState | null>;
      moduleParams?: any;
    }
  ): Promise<any[]> {
    if (permissions.length === 0) return [];

    const schemaIds = Array.from(
      new Set(
        permissions
          .map((perm) => Number(perm.schema_id))
          .filter((schemaId) => Number.isFinite(schemaId) && schemaId > 0)
      )
    );
    const validatorPermIds = Array.from(
      new Set(
        permissions
          .map((perm) => Number(perm.validator_perm_id))
          .filter((validatorPermId) => Number.isFinite(validatorPermId) && validatorPermId > 0)
      )
    );

    const [schemaModesById, validatorPermStateById, moduleParams] = await Promise.all([
      options?.schemaModesById ?? this.getSchemaModesBatch(schemaIds, blockHeight),
      options?.validatorPermStateById ?? this.getValidatorPermStateMap(validatorPermIds, blockHeight, now),
      options?.moduleParams !== undefined
        ? Promise.resolve(options.moduleParams)
        : this.getPermissionModuleParams(blockHeight).catch(() => undefined),
    ]);

    const mergedOptions = {
      ...options,
      schemaModesById,
      validatorPermStateById,
      moduleParams,
    };

    const results: any[] = [];
    for (let i = 0; i < permissions.length; i += batchSize) {
      const batch = permissions.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map((perm) =>
          this.enrichPermissionWithStateAndActions(perm, blockHeight, now, mergedOptions)
        )
      );
      results.push(...batchResults);
    }
    return results;
  }

  private async getSchemaModesBatch(
    schemaIds: number[],
    blockHeight?: number
  ): Promise<Map<number, SchemaData>> {
    const modeMap = new Map<number, SchemaData>();
    if (schemaIds.length === 0) return modeMap;

    if (typeof blockHeight === "number") {
      const rankedSchemas = knex("credential_schema_history as csh")
        .select(
          "csh.credential_schema_id",
          "csh.issuer_perm_management_mode",
          "csh.verifier_perm_management_mode",
          knex.raw(
            `ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC) as rn`
          )
        )
        .whereIn("csh.credential_schema_id", schemaIds)
        .where("csh.height", "<=", blockHeight)
        .as("ranked");

      const historicalModes = await knex
        .from(rankedSchemas)
        .select("credential_schema_id", "issuer_perm_management_mode", "verifier_perm_management_mode")
        .where("rn", 1);

      for (const row of historicalModes) {
        const schemaId = Number(row.credential_schema_id);
        modeMap.set(schemaId, {
          issuer_perm_management_mode: row.issuer_perm_management_mode || null,
          verifier_perm_management_mode: row.verifier_perm_management_mode || null,
        });
      }

      const missingSchemaIds = schemaIds.filter((schemaId) => !modeMap.has(schemaId));
      if (missingSchemaIds.length > 0) {
        const fallbackRows = await knex("credential_schemas")
          .whereIn("id", missingSchemaIds)
          .select("id", "issuer_perm_management_mode", "verifier_perm_management_mode");

        for (const row of fallbackRows) {
          const schemaId = Number(row.id);
          modeMap.set(schemaId, {
            issuer_perm_management_mode: row.issuer_perm_management_mode || null,
            verifier_perm_management_mode: row.verifier_perm_management_mode || null,
          });
        }
      }

      return modeMap;
    }

    const schemaRows = await knex("credential_schemas")
      .whereIn("id", schemaIds)
      .select("id", "issuer_perm_management_mode", "verifier_perm_management_mode");

    for (const row of schemaRows) {
      const schemaId = Number(row.id);
      modeMap.set(schemaId, {
        issuer_perm_management_mode: row.issuer_perm_management_mode || null,
        verifier_perm_management_mode: row.verifier_perm_management_mode || null,
      });
    }

    return modeMap;
  }

  private async getValidatorPermStateMap(
    validatorPermIds: number[],
    blockHeight: number | undefined,
    now: Date
  ): Promise<Map<number, PermState | null>> {
    const stateMap = new Map<number, PermState | null>();
    if (validatorPermIds.length === 0) return stateMap;

    if (typeof blockHeight === "number") {
      const rankedValidators = knex("permission_history as ph")
        .select(
          "ph.permission_id",
          "ph.repaid",
          "ph.slashed",
          "ph.revoked",
          "ph.effective_from",
          "ph.effective_until",
          "ph.type",
          "ph.vp_state",
          "ph.vp_exp",
          "ph.validator_perm_id",
          knex.raw(
            `ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn`
          )
        )
        .whereIn("ph.permission_id", validatorPermIds)
        .where("ph.height", "<=", blockHeight)
        .as("ranked");

      const rows = await knex
        .from(rankedValidators)
        .select(
          "permission_id",
          "repaid",
          "slashed",
          "revoked",
          "effective_from",
          "effective_until",
          "type",
          "vp_state",
          "vp_exp",
          "validator_perm_id"
        )
        .where("rn", 1);

      for (const row of rows) {
        const permissionId = Number(row.permission_id);
        stateMap.set(permissionId, calculatePermState(
          {
            repaid: row.repaid,
            slashed: row.slashed,
            revoked: row.revoked,
            effective_from: row.effective_from,
            effective_until: row.effective_until,
            type: row.type,
            vp_state: row.vp_state,
            vp_exp: row.vp_exp,
            validator_perm_id: row.validator_perm_id,
          },
          now
        ));
      }
      return stateMap;
    }

    const rows = await knex("permissions")
      .whereIn("id", validatorPermIds)
      .select(
        "id",
        "repaid",
        "slashed",
        "revoked",
        "effective_from",
        "effective_until",
        "type",
        "vp_state",
        "vp_exp",
        "validator_perm_id"
      );

    for (const row of rows) {
      const permissionId = Number(row.id);
      stateMap.set(permissionId, calculatePermState(
        {
          repaid: row.repaid,
          slashed: row.slashed,
          revoked: row.revoked,
          effective_from: row.effective_from,
          effective_until: row.effective_until,
          type: row.type,
          vp_state: row.vp_state,
          vp_exp: row.vp_exp,
          validator_perm_id: row.validator_perm_id,
        },
        now
      ));
    }

    return stateMap;
  }

  private async calculateExpireSoon(
    perm: any,
    now: Date,
    blockHeight?: number,
    preloadedModuleParams?: any
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
      const moduleParams = preloadedModuleParams ?? await this.getPermissionModuleParams(blockHeight);
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
    now: Date = new Date(),
    options?: {
      lightweightDerivedStats?: boolean;
      schemaModesById?: Map<number, SchemaData>;
      validatorPermStateById?: Map<number, PermState | null>;
      moduleParams?: any;
    }
  ): Promise<any> {
    const schemaId = Number(perm.schema_id);
    const schema = options?.schemaModesById?.get(schemaId) ?? await this.getSchemaModes(schemaId, blockHeight);

    let validatorPermState: PermState | null = null;
    if (perm.validator_perm_id) {
      const validatorPermId = Number(perm.validator_perm_id);
      validatorPermState = options?.validatorPermStateById?.has(validatorPermId)
        ? options.validatorPermStateById.get(validatorPermId) || null
        : null;
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

    const weight = typeof perm.weight === "number" ? perm.weight : Number(perm.weight || 0);
    const statistics = {
      issued: typeof perm.issued === "number" ? perm.issued : Number(perm.issued || 0),
      verified: typeof perm.verified === "number" ? perm.verified : Number(perm.verified || 0),
    };
    const participants = typeof perm.participants === "number" ? perm.participants : Number(perm.participants || 0);
    const slashStats = {
      ecosystem_slash_events: typeof perm.ecosystem_slash_events === "number" ? perm.ecosystem_slash_events : Number(perm.ecosystem_slash_events || 0),
      ecosystem_slashed_amount: typeof perm.ecosystem_slashed_amount === "number" ? perm.ecosystem_slashed_amount : Number(perm.ecosystem_slashed_amount || 0),
      ecosystem_slashed_amount_repaid: typeof perm.ecosystem_slashed_amount_repaid === "number" ? perm.ecosystem_slashed_amount_repaid : Number(perm.ecosystem_slashed_amount_repaid || 0),
      network_slash_events: typeof perm.network_slash_events === "number" ? perm.network_slash_events : Number(perm.network_slash_events || 0),
      network_slashed_amount: typeof perm.network_slashed_amount === "number" ? perm.network_slashed_amount : Number(perm.network_slashed_amount || 0),
      network_slashed_amount_repaid: typeof perm.network_slashed_amount_repaid === "number" ? perm.network_slashed_amount_repaid : Number(perm.network_slashed_amount_repaid || 0),
    };

    const expireSoon = await this.calculateExpireSoon(perm, now, blockHeight, options?.moduleParams).catch((err: any) => {
      this.logger.warn(`Failed to calculate expire_soon for permission ${perm.id}:`, err?.message || err);
      return null;
    });

    return {
      ...perm,
      perm_state: permState,
      grantee_available_actions: granteeActions,
      validator_available_actions: validatorActions,
      id: Number(perm.id),
      schema_id: Number(perm.schema_id),
      validator_perm_id: perm.validator_perm_id ? Number(perm.validator_perm_id) : null,
      validation_fees: perm.validation_fees != null ? Number(perm.validation_fees) : 0,
      issuance_fees: perm.issuance_fees != null ? Number(perm.issuance_fees) : 0,
      verification_fees: perm.verification_fees != null ? Number(perm.verification_fees) : 0,
      deposit: perm.deposit != null ? Number(perm.deposit) : 0,
      slashed_deposit: perm.slashed_deposit != null ? Number(perm.slashed_deposit) : 0,
      repaid_deposit: perm.repaid_deposit != null ? Number(perm.repaid_deposit) : 0,
      vp_current_fees: perm.vp_current_fees != null ? Number(perm.vp_current_fees) : 0,
      vp_current_deposit: perm.vp_current_deposit != null ? Number(perm.vp_current_deposit) : 0,
      vp_validator_deposit: perm.vp_validator_deposit != null ? Number(perm.vp_validator_deposit) : 0,
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
      min_weight: { type: "number", optional: true },
      max_weight: { type: "number", optional: true },
      min_issued: { type: "number", optional: true },
      max_issued: { type: "number", optional: true },
      min_verified: { type: "number", optional: true },
      max_verified: { type: "number", optional: true },
      min_ecosystem_slash_events: { type: "number", integer: true, optional: true },
      max_ecosystem_slash_events: { type: "number", integer: true, optional: true },
      min_network_slash_events: { type: "number", integer: true, optional: true },
      max_network_slash_events: { type: "number", integer: true, optional: true },
    },
  })
  async listPermissions(ctx: Context<any>) {
    const requestStartedMs = Date.now();
    const perfMarks: Record<string, number> = {};
    let perfMeta: Record<string, any> = {};

    try {
      const p = ctx.params;
      const granteeValidation = validateParticipantParam(p.grantee, "grantee");
      if (!granteeValidation.valid) {
        return ApiResponder.error(ctx, granteeValidation.error, 400);
      }
      const granteeFilter = granteeValidation.value;

      const typeVpValidation = this.normalizeAndValidateTypeAndVpState(p);
      if (!typeVpValidation.ok) {
        return ApiResponder.error(ctx, typeVpValidation.message || "Invalid type/vp_state", 400);
      }
      const normalizedParams = {
        ...p,
        type: typeVpValidation.type,
        vp_state: typeVpValidation.vp_state,
      };

      const blockHeight = getBlockHeight(ctx);
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight);
      const now = new Date().toISOString();
      const limit = Math.min(Math.max(normalizedParams.response_max_size || 64, 1), 1024);

      perfMeta = {
        did: normalizedParams.did ? "[set]" : undefined,
        type: normalizedParams.type,
        schema_id: normalizedParams.schema_id,
        limit,
        blockHeight: useHistoryQuery ? blockHeight : undefined,
      };

      try {
        validateSortParameter(normalizedParams.sort);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }

      const onlyValid = normalizedParams.only_valid === "true" || normalizedParams.only_valid === true;
      const onlySlashed = normalizedParams.only_slashed === "true" || normalizedParams.only_slashed === true;
      const onlyRepaid = normalizedParams.only_repaid === "true" || normalizedParams.only_repaid === true;
      let modifiedAfterIso: string | undefined;
      let whenIso: string | undefined;

      if (normalizedParams.modified_after || normalizedParams.when) {
        const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
        if (normalizedParams.modified_after) {
          if (!isValidISO8601UTC(normalizedParams.modified_after)) {
            return ApiResponder.error(
              ctx,
              "Invalid modified_after format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
              400
            );
          }
          const ts = new Date(normalizedParams.modified_after);
          if (!Number.isNaN(ts.getTime())) modifiedAfterIso = ts.toISOString();
        }
        if (normalizedParams.when) {
          if (!isValidISO8601UTC(normalizedParams.when)) {
            return ApiResponder.error(
              ctx,
              "Invalid when format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
              400
            );
          }
          const whenTs = new Date(normalizedParams.when);
          if (!Number.isNaN(whenTs.getTime())) whenIso = whenTs.toISOString();
        }
      }
      const derivedSortRequested = this.usesDerivedMetricSort(normalizedParams.sort);
      const lightweightDerivedStats = this.shouldUseStrictTrustResolutionLightweightMode(normalizedParams, limit)
        || this.isTrustResolutionListQuery(normalizedParams, useHistoryQuery ? blockHeight : undefined);

      if (useHistoryQuery && blockHeight !== undefined) {
        let historyRequiresMetricPostFilter = false;
        let historyPermStatePushedDown = false;

        const {
          hasIssuedColumn,
          hasVerifiedColumn,
          hasParticipantsColumn,
          hasWeightColumn,
          hasEcosystemSlashEventsColumn,
        } = await this.getMetricColumnAvailability("permission_history");
        const historyHasAllDerivedColumns = hasIssuedColumn
          && hasVerifiedColumn
          && hasParticipantsColumn
          && hasWeightColumn
          && hasEcosystemSlashEventsColumn;
        const historyColumns: any[] = [
          "ph.permission_id as id",
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
          "ph.modified",
        ];
        if (hasIssuedColumn) historyColumns.push(knex.raw("COALESCE(ph.issued, 0) as issued"));
        if (hasVerifiedColumn) historyColumns.push(knex.raw("COALESCE(ph.verified, 0) as verified"));
        if (hasParticipantsColumn) historyColumns.push(knex.raw("COALESCE(ph.participants, 0) as participants"));
        if (hasWeightColumn) historyColumns.push(knex.raw("COALESCE(ph.weight, 0) as weight"));
        if (hasEcosystemSlashEventsColumn) {
          historyColumns.push(
            knex.raw("COALESCE(ph.ecosystem_slash_events, 0) as ecosystem_slash_events"),
            knex.raw("COALESCE(ph.ecosystem_slashed_amount, 0) as ecosystem_slashed_amount"),
            knex.raw("COALESCE(ph.ecosystem_slashed_amount_repaid, 0) as ecosystem_slashed_amount_repaid"),
            knex.raw("COALESCE(ph.network_slash_events, 0) as network_slash_events"),
            knex.raw("COALESCE(ph.network_slashed_amount, 0) as network_slashed_amount"),
            knex.raw("COALESCE(ph.network_slashed_amount_repaid, 0) as network_slashed_amount_repaid")
          );
        }

        const needsPostEnrichFiltering = (!historyPermStatePushedDown && !!normalizedParams.perm_state)
          || historyRequiresMetricPostFilter
          || derivedSortRequested;
        const historyFetchLimit = needsPostEnrichFiltering
          ? Math.min(Math.max(limit * 10, 500), 5000)
          : limit;

        perfMarks.dbQueryStart = Date.now();
        const historySortParamForDb =
          derivedSortRequested && historyRequiresMetricPostFilter ? undefined : normalizedParams.sort;
        let historyQuery: any;
        if (IS_PG_CLIENT) {
          const latestHistory = knex("permission_history as ph")
            .distinctOn("ph.permission_id")
            .select(historyColumns)
            .where("ph.height", "<=", blockHeight)
            .modify((qb) => {
              this.applyBaseListFiltersToQuery(
                qb,
                normalizedParams,
                granteeFilter,
                modifiedAfterIso,
                whenIso,
                onlyValid,
                onlySlashed,
                onlyRepaid,
                now,
                "ph",
                "permission_id"
              );
              const metricPushdown = this.applyMetricFiltersToSql(qb, normalizedParams, {
                participants: hasParticipantsColumn,
                weight: hasWeightColumn,
                issued: hasIssuedColumn,
                verified: hasVerifiedColumn,
                slashStats: hasEcosystemSlashEventsColumn,
                tablePrefix: "ph",
              });
              historyRequiresMetricPostFilter = metricPushdown.requiresPostFilter;
              const permStatePushdown = this.applyPermStateFilterToQuery(
                qb,
                normalizedParams.perm_state,
                now,
                "ph"
              );
              historyPermStatePushedDown = permStatePushdown.pushedDown;
            })
            .orderBy("ph.permission_id", "asc")
            .orderBy("ph.height", "desc")
            .orderBy("ph.created_at", "desc")
            .orderBy("ph.id", "desc")
            .as("latest");
          historyQuery = knex.from(latestHistory).select("*");
        } else {
          const rankedHistory = knex("permission_history as ph")
            .select([
              ...historyColumns,
              knex.raw(
                `ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn`
              ),
            ])
            .where("ph.height", "<=", blockHeight)
            .modify((qb) => {
              this.applyBaseListFiltersToQuery(
                qb,
                normalizedParams,
                granteeFilter,
                modifiedAfterIso,
                whenIso,
                onlyValid,
                onlySlashed,
                onlyRepaid,
                now,
                "ph",
                "permission_id"
              );
              const metricPushdown = this.applyMetricFiltersToSql(qb, normalizedParams, {
                participants: hasParticipantsColumn,
                weight: hasWeightColumn,
                issued: hasIssuedColumn,
                verified: hasVerifiedColumn,
                slashStats: hasEcosystemSlashEventsColumn,
                tablePrefix: "ph",
              });
              historyRequiresMetricPostFilter = metricPushdown.requiresPostFilter;
              const permStatePushdown = this.applyPermStateFilterToQuery(
                qb,
                normalizedParams.perm_state,
                now,
                "ph"
              );
              historyPermStatePushedDown = permStatePushdown.pushedDown;
            })
            .as("ranked");
          historyQuery = knex
            .from(rankedHistory)
            .select("*")
            .where("rn", 1);
        }
        const orderedHistoryQuery = applyOrdering(historyQuery, historySortParamForDb);
        const historyRows = await orderedHistoryQuery.limit(historyFetchLimit);
        perfMarks.dbQueryEnd = Date.now();

        if (historyRows.length === 0) {
          return ApiResponder.success(ctx, { permissions: [] }, 200);
        }

        const normalizedHistoryRows = historyRows.map((historyRecord: any) => {
          const permission: any = {
            id: Number(historyRecord.id),
            schema_id: Number(historyRecord.schema_id),
            grantee: historyRecord.grantee,
            did: historyRecord.did,
            created_by: historyRecord.created_by,
            validator_perm_id: historyRecord.validator_perm_id ? Number(historyRecord.validator_perm_id) : null,
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
            validation_fees: historyRecord.validation_fees != null ? Number(historyRecord.validation_fees) : 0,
            issuance_fees: historyRecord.issuance_fees != null ? Number(historyRecord.issuance_fees) : 0,
            verification_fees: historyRecord.verification_fees != null ? Number(historyRecord.verification_fees) : 0,
            deposit: historyRecord.deposit != null ? Number(historyRecord.deposit) : 0,
            slashed_deposit: historyRecord.slashed_deposit != null ? Number(historyRecord.slashed_deposit) : 0,
            repaid_deposit: historyRecord.repaid_deposit != null ? Number(historyRecord.repaid_deposit) : 0,
            vp_last_state_change: historyRecord.vp_last_state_change,
            vp_current_fees: historyRecord.vp_current_fees != null ? Number(historyRecord.vp_current_fees) : 0,
            vp_current_deposit: historyRecord.vp_current_deposit != null ? Number(historyRecord.vp_current_deposit) : 0,
            vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
            vp_exp: historyRecord.vp_exp,
            vp_validator_deposit: historyRecord.vp_validator_deposit != null ? Number(historyRecord.vp_validator_deposit) : 0,
            vp_term_requested: historyRecord.vp_term_requested,
            created: historyRecord.created,
            modified: historyRecord.modified,
          };
          if (hasIssuedColumn && historyRecord.issued !== undefined) {
            permission.issued = Number(historyRecord.issued || 0);
          }
          if (hasVerifiedColumn && historyRecord.verified !== undefined) {
            permission.verified = Number(historyRecord.verified || 0);
          }
          if (hasParticipantsColumn && historyRecord.participants !== undefined) {
            permission.participants = Number(historyRecord.participants || 0);
          }
          if (hasWeightColumn && historyRecord.weight !== undefined) {
            permission.weight = Number(historyRecord.weight || 0);
          }
          if (hasEcosystemSlashEventsColumn) {
            permission.ecosystem_slash_events = Number(historyRecord.ecosystem_slash_events || 0);
            permission.ecosystem_slashed_amount = Number(historyRecord.ecosystem_slashed_amount || 0);
            permission.ecosystem_slashed_amount_repaid = Number(historyRecord.ecosystem_slashed_amount_repaid || 0);
            permission.network_slash_events = Number(historyRecord.network_slash_events || 0);
            permission.network_slashed_amount = Number(historyRecord.network_slashed_amount || 0);
            permission.network_slashed_amount_repaid = Number(historyRecord.network_slashed_amount_repaid || 0);
          }
          return permission;
        });

        perfMarks.enrichStart = Date.now();
        const historyLightweightDerivedStats = lightweightDerivedStats || historyHasAllDerivedColumns;
        let filteredPermissions = await this.batchEnrichPermissions(
          normalizedHistoryRows,
          blockHeight,
          new Date(now),
          300,
          { lightweightDerivedStats: historyLightweightDerivedStats }
        );
        perfMarks.enrichEnd = Date.now();

        if (!historyPermStatePushedDown && normalizedParams.perm_state) {
          const requestedState = String(normalizedParams.perm_state).toUpperCase();
          filteredPermissions = filteredPermissions.filter(perm => perm.perm_state === requestedState);
        }
        if (historyRequiresMetricPostFilter) {
          filteredPermissions = this.applyMetricFiltersInMemory(filteredPermissions, normalizedParams);
        }

        if (derivedSortRequested) {
          filteredPermissions = sortByStandardAttributes(filteredPermissions, normalizedParams.sort, {
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
          });
        }
        filteredPermissions = filteredPermissions.slice(0, limit);

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

      const {
        hasIssuedColumn,
        hasVerifiedColumn,
        hasParticipantsColumn,
        hasWeightColumn,
        hasEcosystemSlashEventsColumn,
      } = await this.getMetricColumnAvailability("permissions");
      const liveHasAllDerivedColumns = hasIssuedColumn
        && hasVerifiedColumn
        && hasParticipantsColumn
        && hasWeightColumn
        && hasEcosystemSlashEventsColumn;

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
        selectColumns.push(knex.raw("COALESCE(weight, 0) as weight"));
      }
      if (hasEcosystemSlashEventsColumn) {
        selectColumns.push(
          knex.raw("COALESCE(ecosystem_slash_events, 0) as ecosystem_slash_events"),
          knex.raw("COALESCE(ecosystem_slashed_amount, 0) as ecosystem_slashed_amount"),
          knex.raw("COALESCE(ecosystem_slashed_amount_repaid, 0) as ecosystem_slashed_amount_repaid"),
          knex.raw("COALESCE(network_slash_events, 0) as network_slash_events"),
          knex.raw("COALESCE(network_slashed_amount, 0) as network_slashed_amount"),
          knex.raw("COALESCE(network_slashed_amount_repaid, 0) as network_slashed_amount_repaid")
        );
      }

      const query = knex("permissions").select(selectColumns);
      this.applyBaseListFiltersToQuery(
        query,
        normalizedParams,
        granteeFilter,
        modifiedAfterIso,
        whenIso,
        onlyValid,
        onlySlashed,
        onlyRepaid,
        now,
        undefined,
        "id"
      );
      const liveMetricPushdown = this.applyMetricFiltersToSql(query, normalizedParams, {
        participants: hasParticipantsColumn,
        weight: hasWeightColumn,
        issued: hasIssuedColumn,
        verified: hasVerifiedColumn,
        slashStats: hasEcosystemSlashEventsColumn,
      });
      const livePermStatePushdown = this.applyPermStateFilterToQuery(
        query,
        normalizedParams.perm_state,
        now
      );

      const liveNeedsPostEnrich = (!livePermStatePushdown.pushedDown && !!normalizedParams.perm_state)
        || liveMetricPushdown.requiresPostFilter
        || derivedSortRequested;
      const liveFetchLimit = liveNeedsPostEnrich
        ? Math.min(Math.max(limit * 10, 500), 5000)
        : limit;
      const liveSortParamForDb =
        derivedSortRequested && liveMetricPushdown.requiresPostFilter ? undefined : normalizedParams.sort;
      const orderedQuery = applyOrdering(query, liveSortParamForDb);
      perfMarks.dbQueryStart = Date.now();
      const results = await orderedQuery.limit(liveFetchLimit);
      perfMarks.dbQueryEnd = Date.now();
      const normalizedResults = results.map(perm => {
        const normalized: any = {
          ...perm,
          id: Number(perm.id),
          schema_id: Number(perm.schema_id),
          validator_perm_id: perm.validator_perm_id ? Number(perm.validator_perm_id) : null,
          validation_fees: perm.validation_fees != null ? Number(perm.validation_fees) : 0,
          issuance_fees: perm.issuance_fees != null ? Number(perm.issuance_fees) : 0,
          verification_fees: perm.verification_fees != null ? Number(perm.verification_fees) : 0,
          deposit: perm.deposit != null ? Number(perm.deposit) : 0,
          slashed_deposit: perm.slashed_deposit != null ? Number(perm.slashed_deposit) : 0,
          repaid_deposit: perm.repaid_deposit != null ? Number(perm.repaid_deposit) : 0,
          vp_current_fees: perm.vp_current_fees != null ? Number(perm.vp_current_fees) : 0,
          vp_current_deposit: perm.vp_current_deposit != null ? Number(perm.vp_current_deposit) : 0,
          vp_validator_deposit: perm.vp_validator_deposit != null ? Number(perm.vp_validator_deposit) : 0,
        };
        
        if (perm.weight !== undefined) {
          normalized.weight = perm.weight != null ? Number(perm.weight) : 0;
        }
        if (perm.issued !== undefined) {
          normalized.issued = perm.issued != null ? Number(perm.issued) : 0;
        }
        if (perm.verified !== undefined) {
          normalized.verified = perm.verified != null ? Number(perm.verified) : 0;
        }
        if (perm.participants !== undefined) {
          normalized.participants = perm.participants != null ? Number(perm.participants) : 0;
        }
        if (perm.ecosystem_slash_events !== undefined) {
          normalized.ecosystem_slash_events = perm.ecosystem_slash_events != null ? Number(perm.ecosystem_slash_events) : 0;
          normalized.ecosystem_slashed_amount = perm.ecosystem_slashed_amount != null ? Number(perm.ecosystem_slashed_amount) : 0;
          normalized.ecosystem_slashed_amount_repaid = perm.ecosystem_slashed_amount_repaid != null ? Number(perm.ecosystem_slashed_amount_repaid) : 0;
          normalized.network_slash_events = perm.network_slash_events != null ? Number(perm.network_slash_events) : 0;
          normalized.network_slashed_amount = perm.network_slashed_amount != null ? Number(perm.network_slashed_amount) : 0;
          normalized.network_slashed_amount_repaid = perm.network_slashed_amount_repaid != null ? Number(perm.network_slashed_amount_repaid) : 0;
        }
        
        return normalized;
      });

      perfMarks.enrichStart = Date.now();
      const liveLightweightDerivedStats = lightweightDerivedStats || liveHasAllDerivedColumns;
      const enrichedResults = await this.batchEnrichPermissions(
        normalizedResults,
        blockHeight,
        new Date(now),
        300,
        { lightweightDerivedStats: liveLightweightDerivedStats }
      );
      perfMarks.enrichEnd = Date.now();

      let finalResults = enrichedResults;
      if (!livePermStatePushdown.pushedDown && normalizedParams.perm_state) {
        const requestedState = String(normalizedParams.perm_state).toUpperCase();
        finalResults = enrichedResults.filter(perm => perm.perm_state === requestedState);
      }
      if (liveMetricPushdown.requiresPostFilter) {
        finalResults = this.applyMetricFiltersInMemory(finalResults, normalizedParams);
      }

      if (derivedSortRequested) {
        finalResults = sortByStandardAttributes(finalResults, normalizedParams.sort, {
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
        });
      }
      finalResults = finalResults.slice(0, limit);
      const responsePayload = { permissions: finalResults };
      return ApiResponder.success(ctx, responsePayload, 200);
    } catch (err: any) {
      const errMessage = err?.message || String(err);
      if (typeof errMessage === "string" && (
        errMessage.includes("invalid input value for enum permission_type")
        || errMessage.includes("invalid input value for enum validation_state")
      )) {
        return ApiResponder.error(ctx, `Invalid enum filter value: ${errMessage}`, 400);
      }
      this.logger.error("Error in listPermissions:", err);
      this.logger.error("Error details:", {
        message: err?.message,
        stack: err?.stack,
        code: err?.code,
      });
      return ApiResponder.error(ctx, `Failed to list permissions: ${err?.message || String(err)}`, 500);
    } finally {
      const totalMs = Date.now() - requestStartedMs;
      const dbMs = perfMarks.dbQueryStart && perfMarks.dbQueryEnd
        ? perfMarks.dbQueryEnd - perfMarks.dbQueryStart
        : undefined;
      const enrichMs = perfMarks.enrichStart && perfMarks.enrichEnd
        ? perfMarks.enrichEnd - perfMarks.enrichStart
        : undefined;

      const msg = `[listPermissions] duration=${totalMs}ms${dbMs !== undefined ? ` db=${dbMs}ms` : ""}${enrichMs !== undefined ? ` enrich=${enrichMs}ms` : ""} limit=${perfMeta.limit ?? "?"} schema_id=${perfMeta.schema_id ?? "-"} type=${perfMeta.type ?? "-"} did=${perfMeta.did ? "yes" : "no"} at_height=${perfMeta.blockHeight ?? "live"}`;

      if (totalMs >= PermAPIService.LIST_PERMISSIONS_SLOW_MS) {
        this.logger.warn(msg);
      } else {
        this.logger.debug(msg);
      }
    }
  }

  @Action({
    rest: "GET get/:id",
    params: {
      id: { type: "number", integer: true },
    },
  })
  async getPermission(ctx: Context<{ id: number }>) {
    try {
      const id = ctx.params.id;
      const blockHeight = getBlockHeight(ctx);
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight);

      // If AtBlockHeight is provided, query historical state
      if (useHistoryQuery && blockHeight !== undefined) {
        const {
          hasIssuedColumn,
          hasVerifiedColumn,
          hasParticipantsColumn,
          hasWeightColumn,
          hasEcosystemSlashEventsColumn,
        } = await this.getMetricColumnAvailability("permission_history");
        const historyHasAllDerivedColumns = hasIssuedColumn
          && hasVerifiedColumn
          && hasParticipantsColumn
          && hasWeightColumn
          && hasEcosystemSlashEventsColumn;

        const selectColumns: any[] = [
          "permission_id", "schema_id", "grantee", "did", "created_by", "validator_perm_id",
          "type", "country", "vp_state", "revoked", "revoked_by", "slashed", "slashed_by",
          "repaid", "repaid_by", "extended", "extended_by", "effective_from", "effective_until",
          "validation_fees", "issuance_fees", "verification_fees", "deposit", "slashed_deposit",
          "repaid_deposit", "vp_last_state_change", "vp_current_fees", "vp_current_deposit", "vp_summary_digest_sri",
          "vp_exp", "vp_validator_deposit", "vp_term_requested", "created", "modified",
        ];
        if (hasIssuedColumn) selectColumns.push(knex.raw("COALESCE(issued, 0) as issued"));
        if (hasVerifiedColumn) selectColumns.push(knex.raw("COALESCE(verified, 0) as verified"));
        if (hasParticipantsColumn) selectColumns.push(knex.raw("COALESCE(participants, 0) as participants"));
        if (hasWeightColumn) selectColumns.push(knex.raw("COALESCE(weight, 0) as weight"));
        if (hasEcosystemSlashEventsColumn) {
          selectColumns.push(
            knex.raw("COALESCE(ecosystem_slash_events, 0) as ecosystem_slash_events"),
            knex.raw("COALESCE(ecosystem_slashed_amount, 0) as ecosystem_slashed_amount"),
            knex.raw("COALESCE(ecosystem_slashed_amount_repaid, 0) as ecosystem_slashed_amount_repaid"),
            knex.raw("COALESCE(network_slash_events, 0) as network_slash_events"),
            knex.raw("COALESCE(network_slashed_amount, 0) as network_slashed_amount"),
            knex.raw("COALESCE(network_slashed_amount_repaid, 0) as network_slashed_amount_repaid")
          );
        }
        
        const historyRecord = await knex("permission_history")
          .select(selectColumns)
          .where({ permission_id: Number(id) })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, "Permission not found", 404);
        }

        const historicalPermission: any = {
          id: Number(historyRecord.permission_id),
          schema_id: Number(historyRecord.schema_id),
          grantee: historyRecord.grantee,
          did: historyRecord.did,
          created_by: historyRecord.created_by,
          validator_perm_id: historyRecord.validator_perm_id ? Number(historyRecord.validator_perm_id) : null,
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
          validation_fees: historyRecord.validation_fees != null ? Number(historyRecord.validation_fees) : 0,
          issuance_fees: historyRecord.issuance_fees != null ? Number(historyRecord.issuance_fees) : 0,
          verification_fees: historyRecord.verification_fees != null ? Number(historyRecord.verification_fees) : 0,
          deposit: historyRecord.deposit != null ? Number(historyRecord.deposit) : 0,
          slashed_deposit: historyRecord.slashed_deposit != null ? Number(historyRecord.slashed_deposit) : 0,
          repaid_deposit: historyRecord.repaid_deposit != null ? Number(historyRecord.repaid_deposit) : 0,
          vp_last_state_change: historyRecord.vp_last_state_change,
          vp_current_fees: historyRecord.vp_current_fees != null ? Number(historyRecord.vp_current_fees) : 0,
          vp_current_deposit: historyRecord.vp_current_deposit != null ? Number(historyRecord.vp_current_deposit) : 0,
          vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
          vp_exp: historyRecord.vp_exp,
          vp_validator_deposit: historyRecord.vp_validator_deposit != null ? Number(historyRecord.vp_validator_deposit) : 0,
          vp_term_requested: historyRecord.vp_term_requested,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };
        
        if (hasIssuedColumn && historyRecord.issued !== undefined) {
          historicalPermission.issued = Number(historyRecord.issued || 0);
        }
        if (hasVerifiedColumn && historyRecord.verified !== undefined) {
          historicalPermission.verified = Number(historyRecord.verified || 0);
        }
        if (hasParticipantsColumn && historyRecord.participants !== undefined) {
          historicalPermission.participants = Number(historyRecord.participants || 0);
        }
        if (hasWeightColumn && historyRecord.weight !== undefined) {
          historicalPermission.weight = Number(historyRecord.weight || 0);
        }
        if (hasEcosystemSlashEventsColumn && historyRecord.ecosystem_slash_events !== undefined) {
          historicalPermission.ecosystem_slash_events = Number(historyRecord.ecosystem_slash_events || 0);
          historicalPermission.ecosystem_slashed_amount = Number(historyRecord.ecosystem_slashed_amount || 0);
          historicalPermission.ecosystem_slashed_amount_repaid = Number(historyRecord.ecosystem_slashed_amount_repaid || 0);
          historicalPermission.network_slash_events = Number(historyRecord.network_slash_events || 0);
          historicalPermission.network_slashed_amount = Number(historyRecord.network_slashed_amount || 0);
          historicalPermission.network_slashed_amount_repaid = Number(historyRecord.network_slashed_amount_repaid || 0);
        }

        const enrichedPermission = await this.enrichPermissionWithStateAndActions(
          historicalPermission,
          blockHeight,
          new Date(),
          { lightweightDerivedStats: historyHasAllDerivedColumns }
        );

        return ApiResponder.success(ctx, { permission: enrichedPermission }, 200);
      }

      const permission = await knex("permissions").where("id", Number(id)).first();
      if (!permission) {
        return ApiResponder.error(ctx, "Permission not found", 404);
      }
      const normalizedPermission = {
        ...permission,
        id: Number(permission.id),
        schema_id: Number(permission.schema_id),
        validator_perm_id: permission.validator_perm_id ? Number(permission.validator_perm_id) : null,
        validation_fees: permission.validation_fees != null ? Number(permission.validation_fees) : 0,
        issuance_fees: permission.issuance_fees != null ? Number(permission.issuance_fees) : 0,
        verification_fees: permission.verification_fees != null ? Number(permission.verification_fees) : 0,
        deposit: permission.deposit != null ? Number(permission.deposit) : 0,
        slashed_deposit: permission.slashed_deposit != null ? Number(permission.slashed_deposit) : 0,
        repaid_deposit: permission.repaid_deposit != null ? Number(permission.repaid_deposit) : 0,
        vp_current_fees: permission.vp_current_fees != null ? Number(permission.vp_current_fees) : 0,
        vp_current_deposit: permission.vp_current_deposit != null ? Number(permission.vp_current_deposit) : 0,
        vp_validator_deposit: permission.vp_validator_deposit != null ? Number(permission.vp_validator_deposit) : 0,
      };
      if (permission.weight !== undefined) {
        (normalizedPermission as any).weight = permission.weight != null ? Number(permission.weight) : 0;
      }
      if (permission.issued !== undefined) {
        (normalizedPermission as any).issued = permission.issued != null ? Number(permission.issued) : 0;
      }
      if (permission.verified !== undefined) {
        (normalizedPermission as any).verified = permission.verified != null ? Number(permission.verified) : 0;
      }
      if (permission.participants !== undefined) {
        (normalizedPermission as any).participants = permission.participants != null ? Number(permission.participants) : 0;
      }
      if (permission.ecosystem_slash_events !== undefined) {
        (normalizedPermission as any).ecosystem_slash_events = permission.ecosystem_slash_events != null ? Number(permission.ecosystem_slash_events) : 0;
        (normalizedPermission as any).ecosystem_slashed_amount = permission.ecosystem_slashed_amount != null ? Number(permission.ecosystem_slashed_amount) : 0;
        (normalizedPermission as any).ecosystem_slashed_amount_repaid = permission.ecosystem_slashed_amount_repaid != null ? Number(permission.ecosystem_slashed_amount_repaid) : 0;
        (normalizedPermission as any).network_slash_events = permission.network_slash_events != null ? Number(permission.network_slash_events) : 0;
        (normalizedPermission as any).network_slashed_amount = permission.network_slashed_amount != null ? Number(permission.network_slashed_amount) : 0;
        (normalizedPermission as any).network_slashed_amount_repaid = permission.network_slashed_amount_repaid != null ? Number(permission.network_slashed_amount_repaid) : 0;
      }
      const liveHasAllDerivedColumns = permission.issued !== undefined
        && permission.verified !== undefined
        && permission.participants !== undefined
        && permission.weight !== undefined
        && permission.ecosystem_slash_events !== undefined;

      const enrichedPermission = await this.enrichPermissionWithStateAndActions(
        normalizedPermission,
        blockHeight,
        new Date(),
        { lightweightDerivedStats: liveHasAllDerivedColumns }
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
      id: { type: "number", integer: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      transaction_timestamp_older_than: { type: "string", optional: true },
    },
  })
  async getPermissionHistory(ctx: Context<{ id: number; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
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
        entity_id: Number(id),
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
    const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight);

    if (!issuerPermId && !verifierPermId) {
      return ApiResponder.error(
        ctx,
        "issuer_perm_id or verifier_perm_id must be set",
        400
      );
    }

    const foundPermSet = new Set<any>();

    const loadPerm = async (permId: number | string) => {
      const permIdStr = typeof permId === 'string' ? Number(permId) : permId;
      if (useHistoryQuery && blockHeight !== undefined) {
        const historyRecord = await knex("permission_history")
          .where({ permission_id: permIdStr })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();
        if (!historyRecord) throw new Error(`Permission ${permIdStr} not found`);
        return {
          id: historyRecord.permission_id,
          schema_id: historyRecord.schema_id,
          grantee: historyRecord.grantee,
          did: historyRecord.did,
          created_by: historyRecord.created_by,
          validator_perm_id: historyRecord.validator_perm_id || null,
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
        id: Number(perm.id),
        schema_id: Number(perm.schema_id),
        validator_perm_id: perm.validator_perm_id ? Number(perm.validator_perm_id) : null,
      };
    };

    const addAncestors = async (perm: any) => {
      let currentPerm = perm;
      while (currentPerm.validator_perm_id) {
        const parent = await loadPerm(currentPerm.validator_perm_id);
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
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight);

      // If AtBlockHeight is provided, query historical state
      if (useHistoryQuery && blockHeight !== undefined) {
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
      
      if (transactionTimestampOlderThan) {
        const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
        if (!isValidISO8601UTC(transactionTimestampOlderThan)) {
          return ApiResponder.error(
            ctx,
            "Invalid transaction_timestamp_older_than format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
            400
          );
        }
        const timestampDate = new Date(transactionTimestampOlderThan);
        if (Number.isNaN(timestampDate.getTime())) {
          return ApiResponder.error(ctx, "Invalid transaction_timestamp_older_than format", 400);
        }
      }
      
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

      if (modifiedAfter) {
        const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
        if (!isValidISO8601UTC(modifiedAfter)) {
          return ApiResponder.error(
            ctx,
            "Invalid modified_after format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
            400
          );
        }
        const timestampDate = new Date(modifiedAfter);
        if (Number.isNaN(timestampDate.getTime())) {
          return ApiResponder.error(ctx, "Invalid modified_after format", 400);
        }
      }

      try {
        validateSortParameter(sort);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }
      const blockHeight = getBlockHeight(ctx);
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight);
      const limit = Math.min(Math.max(responseMaxSize || 64, 1), 1024);

      // If AtBlockHeight is provided, query historical state
      if (useHistoryQuery && blockHeight !== undefined) {
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
      const accountValidation = validateRequiredAccountParam(p.account, "account");
      if (!accountValidation.valid) {
        return ApiResponder.error(ctx, accountValidation.error, 400);
      }
      const account = accountValidation.value;

      const sortParam = p.sort ?? "-modified";
      try {
        validateSortParameter(sortParam);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }

      const limit = Math.min(Math.max(p.response_max_size || 64, 1), 1024);
      const now = new Date();

      const blockHeight = getBlockHeight(ctx);
      const useHistory = this.shouldUseHistoryQuery(ctx, blockHeight);

      let parentIds: number[] = [];
      const parentIdSet = new Set<number>();

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
          .then((rows: any[]) => rows.map((r: any) => Number(r.permission_id)));
        for (const id of parentIds) parentIdSet.add(id);
      } else {
        const parentRows = await knex("permissions").select("id").where("grantee", account).limit(Math.max(limit * 10, 500));
        parentIds = Array.isArray(parentRows) ? parentRows.map((r: any) => r.id) : [];
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
          .then((rows: any[]) => rows.map((r: any) => Number(r.permission_id)));

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
            id: Number(historyRecord.permission_id),
            schema_id: Number(historyRecord.schema_id),
            validator_perm_id: historyRecord.validator_perm_id ? Number(historyRecord.validator_perm_id) : null,
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
            id: perm.id,
            schema_id: perm.schema_id,
            validator_perm_id: perm.validator_perm_id || null,
          }))
          : [];
      }

      const enriched = await this.batchEnrichPermissions(permissionsAtHeight, useHistory ? blockHeight : undefined, now, 50);
      const filtered = enriched.filter((perm: any) => {
        if (perm.grantee === account) {
          if (perm.vp_state === "PENDING") return true;
          if (perm.perm_state === "SLASHED") return true;
          if (perm.perm_state === "ACTIVE" && perm.expire_soon === true) return true;
        }
        if (perm.validator_perm_id && parentIdSet.has(Number(perm.validator_perm_id))) {
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
      const schemaIds = Array.from(new Set(filtered.map((r: any) => Number(r.schema_id))));
      const schemas = schemaIds.length > 0
        ? await knex("credential_schemas").whereIn("id", schemaIds).select("id", "tr_id", "json_schema", "title", "description", "participants")
        : [];
      const schemaMap = new Map<number, any>();
      for (const s of schemas) {
        const js = s.json_schema;
        let schemaObj: any = null;
        if (js) {
          if (typeof js === "string") {
            try { schemaObj = JSON.parse(js); } catch { schemaObj = null; }
          } else {
            schemaObj = js;
          }
        }
        const title = (schemaObj && typeof schemaObj === "object" && typeof schemaObj.title === "string" ? schemaObj.title : null) ?? s.title ?? undefined;
        const description = (schemaObj && typeof schemaObj === "object" && typeof schemaObj.description === "string" ? schemaObj.description : null) ?? s.description ?? undefined;
        schemaMap.set(s.id, { id: s.id, tr_id: s.tr_id || null, title, description, participants: s.participants ?? 0 });
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
      const trMap = new Map<number | string, any>();
      for (const tr of trs) {
        trMap.set(Number(tr.id), { id: Number(tr.id), did: tr.did, aka: tr.aka, credential_schemas: [], pending_tasks: 0, participants: tr.participants ?? 0 });
      }
      const csMap = new Map<number, any>();
      for (const perm of filtered) {
        const schemaId = perm.schema_id;
        const csInfo = schemaMap.get(schemaId) || { tr_id: null, title: undefined, description: undefined };
        if (!csMap.has(schemaId)) {
          csMap.set(schemaId, {
            id: schemaId,
            title: csInfo.title,
            description: csInfo.description,
            pending_tasks: 0,
            participants: csInfo.participants ?? 0,
            permissions: [],
          });
        }
        const entry = csMap.get(schemaId);
        entry.permissions.push({ ...perm });
        entry.pending_tasks++;
      }
      for (const csEntry of csMap.values()) {
        csEntry.permissions.sort((a: any, b: any) => {
          const ta = new Date(a.modified || 0).getTime();
          const tb = new Date(b.modified || 0).getTime();
          return ta - tb;
        });
      }
      for (const [schemaId, csEntry] of csMap.entries()) {
        const csInfo = schemaMap.get(schemaId) || { tr_id: null };
        const trId = csInfo.tr_id != null ? Number(csInfo.tr_id) : null;
        if (trId !== null && trMap.has(trId)) {
          const trEntry = trMap.get(trId);
          trEntry.credential_schemas.push(csEntry);
          trEntry.pending_tasks += csEntry.pending_tasks;
        } else {
          const nullTrKey = "null";
          if (!trMap.has(nullTrKey)) {
            trMap.set(nullTrKey, { id: null, did: null, aka: null, credential_schemas: [], pending_tasks: 0, participants: 0 });
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
      return ApiResponder.error(ctx, `Failed to get pending tasks: ${err?.message || err}`, 500);
    }
  }
}
