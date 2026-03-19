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
import { applyOrdering, validateSortParameter, sortByStandardAttributes, parseSortParameter } from "../../common/utils/query_ordering";
import { getModuleParams } from "../../common/utils/params_service";
import { isValidISO8601UTC } from "../../common/utils/date_utils";
import { buildActivityTimeline } from "../../common/utils/activity_timeline_helper";
import { mapPermissionType } from "../../common/utils/utils";
import {
  calculatePermState,
  calculateGranteeAvailableActions,
  calculateValidatorAvailableActions,
  type SchemaData,
  type PermState,
} from "./perm_state_utils";

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
  private readonly metricColumnAvailabilityCache = new Map<string, Promise<{
    hasIssuedColumn: boolean;
    hasVerifiedColumn: boolean;
    hasParticipantsColumn: boolean;
    hasParticipantRoleColumns: boolean;
    hasWeightColumn: boolean;
    hasEcosystemSlashEventsColumn: boolean;
    hasExpireSoonColumn: boolean;
  }>>();

  constructor(broker: ServiceBroker) {
    super(broker);
  }

  private async getMetricColumnAvailability(tableName: "permissions" | "permission_history"): Promise<{
    hasIssuedColumn: boolean;
    hasVerifiedColumn: boolean;
    hasParticipantsColumn: boolean;
    hasParticipantRoleColumns: boolean;
    hasWeightColumn: boolean;
    hasEcosystemSlashEventsColumn: boolean;
    hasExpireSoonColumn: boolean;
  }> {
    const cacheKey = tableName;
    const cached = this.metricColumnAvailabilityCache.get(cacheKey);
    if (cached) return cached;

    const loadPromise = knex(tableName)
      .columnInfo()
      .then((columnInfo: any) => ({
        hasIssuedColumn: !!columnInfo.issued,
        hasVerifiedColumn: !!columnInfo.verified,
        hasParticipantsColumn: !!columnInfo.participants,
        hasParticipantRoleColumns: !!columnInfo.participants_ecosystem
          && !!columnInfo.participants_issuer_grantor
          && !!columnInfo.participants_issuer
          && !!columnInfo.participants_verifier_grantor
          && !!columnInfo.participants_verifier
          && !!columnInfo.participants_holder,
        hasWeightColumn: !!columnInfo.weight,
        hasEcosystemSlashEventsColumn: !!columnInfo.ecosystem_slash_events,
        hasExpireSoonColumn: !!columnInfo.expire_soon,
      }))
      .catch((error) => {
        this.metricColumnAvailabilityCache.delete(cacheKey);
        throw error;
      });

    this.metricColumnAvailabilityCache.set(cacheKey, loadPromise);
    return loadPromise;
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
          .whereRaw("height <= ?", [Number(blockHeight)])
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

  private normalizePermissionSessionRow(row: any): any {
    if (!row) return row;

    let authz: any[] = [];
    try {
      if (typeof row.authz === "string") {
        authz = JSON.parse(row.authz || "[]");
      } else if (Array.isArray(row.authz)) {
        authz = row.authz;
      } else {
        authz = [];
      }
    } catch {
      authz = [];
    }

    const mappedAuthz = authz.map((entry: any) => ({
      executor_perm_id: Number(entry?.executor_perm_id ?? 0) || 0,
      beneficiary_perm_id: Number(entry?.beneficiary_perm_id ?? 0) || 0,
      wallet_agent_perm_id: Number(entry?.wallet_agent_perm_id ?? 0) || 0,
    }));

    return {
      id: row.id ?? row.session_id,
      controller: row.controller ?? null,
      agent_perm_id: Number(row.agent_perm_id ?? 0) || 0,
      wallet_agent_perm_id: Number(row.wallet_agent_perm_id ?? 0) || 0,
      authz: mappedAuthz,
      created: row.created ?? null,
      modified: row.modified ?? null,
    };
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
      participantRoles: boolean;
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
      { min: "min_participants_ecosystem", max: "max_participants_ecosystem", db: "participants_ecosystem", enabled: options.participantRoles },
      { min: "min_participants_issuer_grantor", max: "max_participants_issuer_grantor", db: "participants_issuer_grantor", enabled: options.participantRoles },
      { min: "min_participants_issuer", max: "max_participants_issuer", db: "participants_issuer", enabled: options.participantRoles },
      { min: "min_participants_verifier_grantor", max: "max_participants_verifier_grantor", db: "participants_verifier_grantor", enabled: options.participantRoles },
      { min: "min_participants_verifier", max: "max_participants_verifier", db: "participants_verifier", enabled: options.participantRoles },
      { min: "min_participants_holder", max: "max_participants_holder", db: "participants_holder", enabled: options.participantRoles },
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

  private canPushDerivedSortToSql(
    sort: any,
    options: {
      hasParticipantsColumn: boolean;
      hasParticipantRoleColumns: boolean;
      hasWeightColumn: boolean;
      hasIssuedColumn: boolean;
      hasVerifiedColumn: boolean;
      hasEcosystemSlashEventsColumn: boolean;
    }
  ): boolean {
    if (typeof sort !== "string" || !sort.trim()) return false;

    try {
      const sortOrders = parseSortParameter(sort);
      for (const { attribute } of sortOrders) {
        if (attribute === "id" || attribute === "modified" || attribute === "created") continue;
        if (attribute === "participants" && options.hasParticipantsColumn) continue;
        if (
          (attribute === "participants_ecosystem"
            || attribute === "participants_issuer_grantor"
            || attribute === "participants_issuer"
            || attribute === "participants_verifier_grantor"
            || attribute === "participants_verifier"
            || attribute === "participants_holder")
          && options.hasParticipantRoleColumns
        ) {
          continue;
        }
        if ((attribute === "weight") && options.hasWeightColumn) continue;
        if ((attribute === "issued") && options.hasIssuedColumn) continue;
        if ((attribute === "verified") && options.hasVerifiedColumn) continue;
        if (
          (attribute === "ecosystem_slash_events"
            || attribute === "ecosystem_slashed_amount"
            || attribute === "network_slash_events"
            || attribute === "network_slashed_amount")
          && options.hasEcosystemSlashEventsColumn
        ) {
          continue;
        }
        return false;
      }
      return true;
    } catch {
      return false;
    }
  }

  private applyRequestedSortToQuery(
    query: any,
    sort: string | undefined
  ): any {
    if (typeof sort !== "string" || !sort.trim()) {
      return query.orderBy("modified", "desc").orderBy("id", "desc");
    }

    const sortOrders = parseSortParameter(sort);
    let hasIdSort = false;
    for (const { attribute, direction } of sortOrders) {
      query.orderBy(attribute, direction);
      if (attribute === "id") hasIdSort = true;
    }

    if (!hasIdSort) {
      query.orderBy("id", "desc");
    }

    return query;
  }

  private applyMetricFiltersInMemory(permissions: any[], params: any): any[] {
    const specs = [
      { min: "min_participants", max: "max_participants", field: "participants" },
      { min: "min_participants_ecosystem", max: "max_participants_ecosystem", field: "participants_ecosystem" },
      { min: "min_participants_issuer_grantor", max: "max_participants_issuer_grantor", field: "participants_issuer_grantor" },
      { min: "min_participants_issuer", max: "max_participants_issuer", field: "participants_issuer" },
      { min: "min_participants_verifier_grantor", max: "max_participants_verifier_grantor", field: "participants_verifier_grantor" },
      { min: "min_participants_verifier", max: "max_participants_verifier", field: "participants_verifier" },
      { min: "min_participants_holder", max: "max_participants_holder", field: "participants_holder" },
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

    const requiresSchemaModes = (permType: string | undefined): boolean => {
      if (!permType) return false;
      return permType === "ISSUER_GRANTOR"
        || permType === "ISSUER"
        || permType === "VERIFIER_GRANTOR"
        || permType === "VERIFIER";
    };

    const schemaIds = Array.from(
      new Set(
        permissions
          .filter((perm) => requiresSchemaModes(String(perm?.type || "").toUpperCase()))
          .map((perm) => Number(perm.schema_id))
          .filter((schemaId) => Number.isFinite(schemaId) && schemaId > 0)
      )
    );

    const locallyKnownPermStateById = new Map<number, PermState>();
    for (const perm of permissions) {
      const permissionId = Number(perm?.id);
      if (!Number.isFinite(permissionId) || permissionId <= 0) continue;
      locallyKnownPermStateById.set(permissionId, calculatePermState(
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
      ));
    }

    const validatorPermIds = Array.from(
      new Set(
        permissions
          .filter((perm) => String(perm?.type || "").toUpperCase() !== "ECOSYSTEM")
          .map((perm) => Number(perm.validator_perm_id))
          .filter((validatorPermId) => Number.isFinite(validatorPermId) && validatorPermId > 0)
      )
    );
    const missingValidatorPermIds = validatorPermIds.filter(
      (validatorPermId) => !locallyKnownPermStateById.has(validatorPermId)
    );

    const shouldLoadModuleParams = permissions.some(
      (perm) => perm?.effective_until !== null && perm?.effective_until !== undefined
    );

    const [schemaModesById, validatorPermStateById, moduleParams] = await Promise.all([
      options?.schemaModesById ?? this.getSchemaModesBatch(schemaIds, blockHeight),
      options?.validatorPermStateById ?? this.getValidatorPermStateMap(missingValidatorPermIds, blockHeight, now),
      options?.moduleParams !== undefined || !shouldLoadModuleParams
        ? Promise.resolve(options?.moduleParams)
        : this.getPermissionModuleParams(blockHeight).catch(() => undefined),
    ]);

    const mergedValidatorPermStateById = new Map<number, PermState | null>();
    for (const [permissionId, state] of locallyKnownPermStateById.entries()) {
      mergedValidatorPermStateById.set(permissionId, state);
    }
    for (const [permissionId, state] of validatorPermStateById.entries()) {
      mergedValidatorPermStateById.set(permissionId, state);
    }

    const mergedOptions = {
      ...options,
      schemaModesById,
      validatorPermStateById: mergedValidatorPermStateById,
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

  private normalizeVpStateForResponse(value: unknown): string | null {
    if (value === null || value === undefined) return null;
    if (typeof value === "string") return value.toUpperCase();
    const n = Number(value);
    if (n === 1) return "PENDING";
    if (n === 2) return "VALIDATED";
    if (n === 3 || n === 4) return "TERMINATED";
    return "VALIDATION_STATE_UNSPECIFIED";
  }

  private normalizePermissionRow(perm: any): any {
    const normalized: any = {
      ...perm,
      id: Number(perm.id),
      schema_id: Number(perm.schema_id),
      type: perm.type !== undefined && perm.type !== null ? mapPermissionType(perm.type) : perm.type,
      vp_state: this.normalizeVpStateForResponse(perm.vp_state),
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
      weight: perm.weight != null ? Number(perm.weight) : 0,
      issued: perm.issued != null ? Number(perm.issued) : 0,
      verified: perm.verified != null ? Number(perm.verified) : 0,
      participants: perm.participants != null ? Number(perm.participants) : 0,
      participants_ecosystem: perm.participants_ecosystem != null ? Number(perm.participants_ecosystem) : 0,
      participants_issuer_grantor: perm.participants_issuer_grantor != null ? Number(perm.participants_issuer_grantor) : 0,
      participants_issuer: perm.participants_issuer != null ? Number(perm.participants_issuer) : 0,
      participants_verifier_grantor: perm.participants_verifier_grantor != null ? Number(perm.participants_verifier_grantor) : 0,
      participants_verifier: perm.participants_verifier != null ? Number(perm.participants_verifier) : 0,
      participants_holder: perm.participants_holder != null ? Number(perm.participants_holder) : 0,
      ecosystem_slash_events: perm.ecosystem_slash_events != null ? Number(perm.ecosystem_slash_events) : 0,
      ecosystem_slashed_amount: perm.ecosystem_slashed_amount != null ? Number(perm.ecosystem_slashed_amount) : 0,
      ecosystem_slashed_amount_repaid: perm.ecosystem_slashed_amount_repaid != null ? Number(perm.ecosystem_slashed_amount_repaid) : 0,
      network_slash_events: perm.network_slash_events != null ? Number(perm.network_slash_events) : 0,
      network_slashed_amount: perm.network_slashed_amount != null ? Number(perm.network_slashed_amount) : 0,
      network_slashed_amount_repaid: perm.network_slashed_amount_repaid != null ? Number(perm.network_slashed_amount_repaid) : 0,
    };

    normalized.extended_by = perm.extended_by === "" ? null : perm.extended_by ?? null;
    normalized.repaid_by = perm.repaid_by === "" ? null : perm.repaid_by ?? null;
    normalized.slashed_by = perm.slashed_by === "" ? null : perm.slashed_by ?? null;
    normalized.revoked_by = perm.revoked_by === "" ? null : perm.revoked_by ?? null;

    return normalized;
  }

  private async getPermissionsByIdsMap(permissionIds: number[], blockHeight?: number): Promise<Map<number, any>> {
    const idMap = new Map<number, any>();
    if (permissionIds.length === 0) return idMap;
    const uniqueIds = Array.from(new Set(permissionIds.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)));
    if (uniqueIds.length === 0) return idMap;

    if (typeof blockHeight === "number") {
      let rows: any[] = [];
      if (IS_PG_CLIENT) {
        rows = await knex("permission_history as ph")
          .distinctOn("ph.permission_id")
          .select([
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
            "ph.weight",
            "ph.issued",
            "ph.verified",
            "ph.participants",
            "ph.participants_ecosystem",
            "ph.participants_issuer_grantor",
            "ph.participants_issuer",
            "ph.participants_verifier_grantor",
            "ph.participants_verifier",
            "ph.participants_holder",
            "ph.ecosystem_slash_events",
            "ph.ecosystem_slashed_amount",
            "ph.ecosystem_slashed_amount_repaid",
            "ph.network_slash_events",
            "ph.network_slashed_amount",
            "ph.network_slashed_amount_repaid",
            "ph.created",
            "ph.modified",
          ])
          .whereIn("ph.permission_id", uniqueIds)
          .where("ph.height", "<=", blockHeight)
          .orderBy("ph.permission_id", "asc")
          .orderBy("ph.height", "desc")
          .orderBy("ph.created_at", "desc")
          .orderBy("ph.id", "desc");
      } else {
        const ranked = knex("permission_history as ph")
          .select([
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
            "ph.weight",
            "ph.issued",
            "ph.verified",
            "ph.participants",
            "ph.participants_ecosystem",
            "ph.participants_issuer_grantor",
            "ph.participants_issuer",
            "ph.participants_verifier_grantor",
            "ph.participants_verifier",
            "ph.participants_holder",
            "ph.ecosystem_slash_events",
            "ph.ecosystem_slashed_amount",
            "ph.ecosystem_slashed_amount_repaid",
            "ph.network_slash_events",
            "ph.network_slashed_amount",
            "ph.network_slashed_amount_repaid",
            "ph.created",
            "ph.modified",
            knex.raw("ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn"),
          ])
          .whereIn("ph.permission_id", uniqueIds)
          .where("ph.height", "<=", blockHeight)
          .as("ranked");

        rows = await knex.from(ranked).select("*").where("rn", 1);
      }

      for (const row of rows) {
        const normalized = this.normalizePermissionRow(row);
        idMap.set(Number(normalized.id), normalized);
      }
      return idMap;
    }

    const rows = await knex("permissions")
      .select([
        "id",
        "schema_id",
        "grantee",
        "did",
        "created_by",
        "validator_perm_id",
        "type",
        "country",
        "vp_state",
        "revoked",
        "revoked_by",
        "slashed",
        "slashed_by",
        "repaid",
        "repaid_by",
        "extended",
        "extended_by",
        "effective_from",
        "effective_until",
        "validation_fees",
        "issuance_fees",
        "verification_fees",
        "deposit",
        "slashed_deposit",
        "repaid_deposit",
        "vp_last_state_change",
        "vp_current_fees",
        "vp_current_deposit",
        "vp_summary_digest_sri",
        "vp_exp",
        "vp_validator_deposit",
        "vp_term_requested",
        "weight",
        "issued",
        "verified",
        "participants",
        "participants_ecosystem",
        "participants_issuer_grantor",
        "participants_issuer",
        "participants_verifier_grantor",
        "participants_verifier",
        "participants_holder",
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "ecosystem_slashed_amount_repaid",
        "network_slash_events",
        "network_slashed_amount",
        "network_slashed_amount_repaid",
        "created",
        "modified",
      ])
      .whereIn("id", uniqueIds);
    for (const row of rows) {
      const normalized = this.normalizePermissionRow(row);
      idMap.set(Number(normalized.id), normalized);
    }
    return idMap;
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
    const schemaFromBatch = options?.schemaModesById?.get(schemaId);
    const schema = schemaFromBatch
      || (options?.schemaModesById !== undefined ? {} : await this.getSchemaModes(schemaId, blockHeight));

    let validatorPermState: PermState | null = null;
    const validatorPermStateById = options?.validatorPermStateById;
    if (perm.validator_perm_id) {
      const validatorPermId = Number(perm.validator_perm_id);
      validatorPermState = validatorPermStateById?.has(validatorPermId)
        ? validatorPermStateById.get(validatorPermId) || null
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
    const participantsByRole = {
      participants_ecosystem: typeof perm.participants_ecosystem === "number" ? perm.participants_ecosystem : Number(perm.participants_ecosystem || 0),
      participants_issuer_grantor: typeof perm.participants_issuer_grantor === "number" ? perm.participants_issuer_grantor : Number(perm.participants_issuer_grantor || 0),
      participants_issuer: typeof perm.participants_issuer === "number" ? perm.participants_issuer : Number(perm.participants_issuer || 0),
      participants_verifier_grantor: typeof perm.participants_verifier_grantor === "number" ? perm.participants_verifier_grantor : Number(perm.participants_verifier_grantor || 0),
      participants_verifier: typeof perm.participants_verifier === "number" ? perm.participants_verifier : Number(perm.participants_verifier || 0),
      participants_holder: typeof perm.participants_holder === "number" ? perm.participants_holder : Number(perm.participants_holder || 0),
    };
    const participantsSum = participantsByRole.participants_ecosystem
      + participantsByRole.participants_issuer_grantor
      + participantsByRole.participants_issuer
      + participantsByRole.participants_verifier_grantor
      + participantsByRole.participants_verifier
      + participantsByRole.participants_holder;
    const participants = (perm.participants != null && perm.participants !== "")
      ? Number(perm.participants)
      : participantsSum;
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
      participants_ecosystem: participantsByRole.participants_ecosystem,
      participants_issuer_grantor: participantsByRole.participants_issuer_grantor,
      participants_issuer: participantsByRole.participants_issuer,
      participants_verifier_grantor: participantsByRole.participants_verifier_grantor,
      participants_verifier: participantsByRole.participants_verifier,
      participants_holder: participantsByRole.participants_holder,
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
      min_participants_ecosystem: { type: "number", integer: true, optional: true },
      max_participants_ecosystem: { type: "number", integer: true, optional: true },
      min_participants_issuer_grantor: { type: "number", integer: true, optional: true },
      max_participants_issuer_grantor: { type: "number", integer: true, optional: true },
      min_participants_issuer: { type: "number", integer: true, optional: true },
      max_participants_issuer: { type: "number", integer: true, optional: true },
      min_participants_verifier_grantor: { type: "number", integer: true, optional: true },
      max_participants_verifier_grantor: { type: "number", integer: true, optional: true },
      min_participants_verifier: { type: "number", integer: true, optional: true },
      max_participants_verifier: { type: "number", integer: true, optional: true },
      min_participants_holder: { type: "number", integer: true, optional: true },
      max_participants_holder: { type: "number", integer: true, optional: true },
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
          hasParticipantRoleColumns,
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
        if (hasParticipantRoleColumns) {
          historyColumns.push(
            knex.raw("COALESCE(ph.participants_ecosystem, 0) as participants_ecosystem"),
            knex.raw("COALESCE(ph.participants_issuer_grantor, 0) as participants_issuer_grantor"),
            knex.raw("COALESCE(ph.participants_issuer, 0) as participants_issuer"),
            knex.raw("COALESCE(ph.participants_verifier_grantor, 0) as participants_verifier_grantor"),
            knex.raw("COALESCE(ph.participants_verifier, 0) as participants_verifier"),
            knex.raw("COALESCE(ph.participants_holder, 0) as participants_holder")
          );
        }
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

        perfMarks.dbQueryStart = Date.now();
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
                participantRoles: hasParticipantRoleColumns,
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
                participantRoles: hasParticipantRoleColumns,
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

        const historySortPushedToSql = this.canPushDerivedSortToSql(normalizedParams.sort, {
          hasParticipantsColumn,
          hasParticipantRoleColumns,
          hasWeightColumn,
          hasIssuedColumn,
          hasVerifiedColumn,
          hasEcosystemSlashEventsColumn,
        });

        const needsPostEnrichFiltering = (!historyPermStatePushedDown && !!normalizedParams.perm_state)
          || historyRequiresMetricPostFilter
          || (derivedSortRequested && !historySortPushedToSql);
        const historyFetchLimit = needsPostEnrichFiltering
          ? Math.min(Math.max(limit * 10, 500), 5000)
          : limit;

        const orderedHistoryQuery = historySortPushedToSql
          ? this.applyRequestedSortToQuery(historyQuery, normalizedParams.sort)
          : applyOrdering(historyQuery, derivedSortRequested ? undefined : normalizedParams.sort);
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
            type: historyRecord.type !== undefined && historyRecord.type !== null ? mapPermissionType(historyRecord.type) : historyRecord.type,
            country: historyRecord.country,
            vp_state: this.normalizeVpStateForResponse(historyRecord.vp_state) ?? historyRecord.vp_state,
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
          if (hasParticipantRoleColumns) {
            permission.participants_ecosystem = Number(historyRecord.participants_ecosystem || 0);
            permission.participants_issuer_grantor = Number(historyRecord.participants_issuer_grantor || 0);
            permission.participants_issuer = Number(historyRecord.participants_issuer || 0);
            permission.participants_verifier_grantor = Number(historyRecord.participants_verifier_grantor || 0);
            permission.participants_verifier = Number(historyRecord.participants_verifier || 0);
            permission.participants_holder = Number(historyRecord.participants_holder || 0);
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

        if (derivedSortRequested && !historySortPushedToSql) {
          filteredPermissions = sortByStandardAttributes(filteredPermissions, normalizedParams.sort, {
            getId: (item) => Number(item.id),
            getCreated: (item) => item.created,
            getModified: (item) => item.modified,
            getParticipants: (item) => item.participants,
            getParticipantsEcosystem: (item) => item.participants_ecosystem,
            getParticipantsIssuerGrantor: (item) => item.participants_issuer_grantor,
            getParticipantsIssuer: (item) => item.participants_issuer,
            getParticipantsVerifierGrantor: (item) => item.participants_verifier_grantor,
            getParticipantsVerifier: (item) => item.participants_verifier,
            getParticipantsHolder: (item) => item.participants_holder,
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
        hasParticipantRoleColumns,
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
      if (hasParticipantRoleColumns) {
        selectColumns.push(
          knex.raw("COALESCE(participants_ecosystem, 0) as participants_ecosystem"),
          knex.raw("COALESCE(participants_issuer_grantor, 0) as participants_issuer_grantor"),
          knex.raw("COALESCE(participants_issuer, 0) as participants_issuer"),
          knex.raw("COALESCE(participants_verifier_grantor, 0) as participants_verifier_grantor"),
          knex.raw("COALESCE(participants_verifier, 0) as participants_verifier"),
          knex.raw("COALESCE(participants_holder, 0) as participants_holder")
        );
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
        participantRoles: hasParticipantRoleColumns,
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
      const liveSortPushedToSql = this.canPushDerivedSortToSql(normalizedParams.sort, {
        hasParticipantsColumn,
        hasParticipantRoleColumns,
        hasWeightColumn,
        hasIssuedColumn,
        hasVerifiedColumn,
        hasEcosystemSlashEventsColumn,
      });

      const liveNeedsPostEnrich = (!livePermStatePushdown.pushedDown && !!normalizedParams.perm_state)
        || liveMetricPushdown.requiresPostFilter
        || (derivedSortRequested && !liveSortPushedToSql);
      const liveFetchLimit = liveNeedsPostEnrich
        ? Math.min(Math.max(limit * 10, 500), 5000)
        : limit;
      const liveSortParamForDb =
        liveSortPushedToSql ? normalizedParams.sort : (derivedSortRequested && liveMetricPushdown.requiresPostFilter ? undefined : normalizedParams.sort);
      const orderedQuery = liveSortPushedToSql
        ? this.applyRequestedSortToQuery(query, normalizedParams.sort)
        : applyOrdering(query, liveSortParamForDb);
      perfMarks.dbQueryStart = Date.now();
      const results = await orderedQuery.limit(liveFetchLimit);
      perfMarks.dbQueryEnd = Date.now();
      const normalizedResults = results.map((perm: any) => this.normalizePermissionRow(perm));

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

      if (derivedSortRequested && !liveSortPushedToSql) {
        finalResults = sortByStandardAttributes(finalResults, normalizedParams.sort, {
          getId: (item) => Number(item.id),
          getCreated: (item) => item.created,
          getModified: (item) => item.modified,
          getParticipants: (item) => item.participants,
          getParticipantsEcosystem: (item) => item.participants_ecosystem,
          getParticipantsIssuerGrantor: (item) => item.participants_issuer_grantor,
          getParticipantsIssuer: (item) => item.participants_issuer,
          getParticipantsVerifierGrantor: (item) => item.participants_verifier_grantor,
          getParticipantsVerifier: (item) => item.participants_verifier,
          getParticipantsHolder: (item) => item.participants_holder,
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
          hasExpireSoonColumn,
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
        if (hasExpireSoonColumn) selectColumns.push("expire_soon");
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
          .whereRaw("height <= ?", [Number(blockHeight)])
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
          type: historyRecord.type !== undefined && historyRecord.type !== null ? mapPermissionType(historyRecord.type) : historyRecord.type,
          country: historyRecord.country,
          vp_state: this.normalizeVpStateForResponse(historyRecord.vp_state) ?? historyRecord.vp_state,
          revoked: historyRecord.revoked,
          revoked_by: historyRecord.revoked_by === "" ? null : historyRecord.revoked_by ?? null,
          slashed: historyRecord.slashed,
          slashed_by: historyRecord.slashed_by === "" ? null : historyRecord.slashed_by ?? null,
          repaid: historyRecord.repaid,
          repaid_by: historyRecord.repaid_by === "" ? null : historyRecord.repaid_by ?? null,
          extended: historyRecord.extended,
          extended_by: historyRecord.extended_by === "" ? null : historyRecord.extended_by ?? null,
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
        
        if (hasIssuedColumn) {
          historicalPermission.issued = Number(historyRecord.issued ?? 0);
        }
        if (hasVerifiedColumn) {
          historicalPermission.verified = Number(historyRecord.verified ?? 0);
        }
        if (hasParticipantsColumn) {
          historicalPermission.participants = Number(historyRecord.participants ?? 0);
        }
        if (hasWeightColumn) {
          historicalPermission.weight = Number(historyRecord.weight ?? 0);
        }
        if (hasEcosystemSlashEventsColumn) {
          historicalPermission.ecosystem_slash_events = Number(historyRecord.ecosystem_slash_events ?? 0);
          historicalPermission.ecosystem_slashed_amount = Number(historyRecord.ecosystem_slashed_amount ?? 0);
          historicalPermission.ecosystem_slashed_amount_repaid = Number(historyRecord.ecosystem_slashed_amount_repaid ?? 0);
          historicalPermission.network_slash_events = Number(historyRecord.network_slash_events ?? 0);
          historicalPermission.network_slashed_amount = Number(historyRecord.network_slashed_amount ?? 0);
          historicalPermission.network_slashed_amount_repaid = Number(historyRecord.network_slashed_amount_repaid ?? 0);
        }
        if (hasExpireSoonColumn) {
          historicalPermission.expire_soon = historyRecord.expire_soon ?? null;
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
      const normalizedPermission = this.normalizePermissionRow(permission);
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

    try {
      const rootIds = [issuerPermId, verifierPermId]
        .filter((id): id is number => id !== undefined && id !== null)
        .map((id) => Number(id));

      const initialMap = await this.getPermissionsByIdsMap(rootIds, useHistoryQuery ? blockHeight : undefined);
      const missingRootIds = rootIds.filter((rootId) => !initialMap.has(rootId));
      if (missingRootIds.length > 0) {
        return ApiResponder.error(
          ctx,
          `Permission not found for id(s): ${missingRootIds.join(", ")}`,
          404
        );
      }

      const foundPermMap = new Map<number, any>();
      const collectAncestors = async (startPermId: number) => {
        const visited = new Set<number>([startPermId]);
        let frontier: number[] = [startPermId];

        while (frontier.length > 0) {
          const currentMap = await this.getPermissionsByIdsMap(frontier, useHistoryQuery ? blockHeight : undefined);
          const parentIds: number[] = [];
          const nextFrontier: number[] = [];
          for (const permId of frontier) {
            const perm = currentMap.get(permId);
            if (!perm) {
              continue;
            }
            const parentId = perm.validator_perm_id ? Number(perm.validator_perm_id) : null;
            if (!parentId || visited.has(parentId)) {
              continue;
            }
            visited.add(parentId);
            parentIds.push(parentId);
            nextFrontier.push(parentId);
          }
          const parentMap = parentIds.length > 0
            ? await this.getPermissionsByIdsMap(parentIds, useHistoryQuery ? blockHeight : undefined)
            : new Map<number, any>();
          for (const parentId of parentIds) {
            const parent = parentMap.get(parentId);
            if (!parent) continue;
            if (!parent.revoked && !parent.slashed) foundPermMap.set(Number(parent.id), parent);
          }
          frontier = nextFrontier;
        }
      };

      if (issuerPermId) {
        if (!verifierPermId) {
          await collectAncestors(Number(issuerPermId));
        }
      }

      if (verifierPermId) {
        if (issuerPermId) {
          const issuerPerm = initialMap.get(Number(issuerPermId));
          if (issuerPerm) foundPermMap.set(Number(issuerPerm.id), issuerPerm);
        }
        await collectAncestors(Number(verifierPermId));
      }

      const enrichedPermissions = await this.batchEnrichPermissions(
        Array.from(foundPermMap.values()),
        useHistoryQuery ? blockHeight : undefined,
        new Date(),
        100
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
          .whereRaw("height <= ?", [Number(blockHeight)])
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, "PermissionSession not found", 404);
        }

        const historicalSession = this.normalizePermissionSessionRow(historyRecord);
        return ApiResponder.success(ctx, { session: historicalSession }, 200);
      }

      // Otherwise, return latest state
      const session = await knex("permission_sessions").where("id", id).first();
      if (!session) {
        return ApiResponder.error(ctx, "PermissionSession not found", 404);
      }
      const normalized = this.normalizePermissionSessionRow(session);
      return ApiResponder.success(ctx, { session: normalized }, 200);
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
        let historyQuery: any;
        if (IS_PG_CLIENT) {
          const latest = knex("permission_session_history as psh")
            .distinctOn("psh.session_id")
            .select(
              "psh.session_id as id",
              "psh.controller",
              "psh.agent_perm_id",
              "psh.wallet_agent_perm_id",
              "psh.authz",
              "psh.created",
              "psh.modified"
            )
            .where("psh.height", "<=", blockHeight)
            .orderBy("psh.session_id", "asc")
            .orderBy("psh.height", "desc")
            .orderBy("psh.created_at", "desc")
            .orderBy("psh.id", "desc")
            .as("latest");
          historyQuery = knex.from(latest).select("*");
        } else {
          const ranked = knex("permission_session_history as psh")
            .select(
              "psh.session_id as id",
              "psh.controller",
              "psh.agent_perm_id",
              "psh.wallet_agent_perm_id",
              "psh.authz",
              "psh.created",
              "psh.modified",
              knex.raw("ROW_NUMBER() OVER (PARTITION BY psh.session_id ORDER BY psh.height DESC, psh.created_at DESC, psh.id DESC) as rn")
            )
            .where("psh.height", "<=", blockHeight)
            .as("ranked");
          historyQuery = knex.from(ranked).select("*").where("rn", 1);
        }

        if (modifiedAfter) {
          const ts = new Date(modifiedAfter);
          if (!Number.isNaN(ts.getTime())) {
            historyQuery.where("modified", ">", ts.toISOString());
          }
        }

        const orderedHistoryQuery = sort
          ? applyOrdering(historyQuery, sort)
          : historyQuery.orderBy("modified", "asc").orderBy("id", "desc");
        const sessionsRaw = await orderedHistoryQuery.limit(limit);
        const sessions = sessionsRaw.map((row: any) => this.normalizePermissionSessionRow(row));
        return ApiResponder.success(ctx, { sessions }, 200);
      }

      // Otherwise, return latest state
      const query = knex("permission_sessions").select("*");
      if (modifiedAfter) {
        const ts = new Date(modifiedAfter);
        if (!Number.isNaN(ts.getTime()))
          query.where("modified", ">", ts.toISOString());
      }

      const orderedQuery = applyOrdering(query, sort);
      const resultsRaw = await orderedQuery.limit(limit);
      const results = resultsRaw.map((row: any) => this.normalizePermissionSessionRow(row));
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
          .whereRaw("height <= ?", [Number(blockHeight)])
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
          .whereRaw("height <= ?", [Number(blockHeight)])
          .where((qb) => {
            qb.where("grantee", account);
            if (parentIds.length > 0) qb.orWhereIn("validator_perm_id", parentIds);
          })
          .as("ranked");

        const joined = await knex
          .from(latestSub)
          .join("permission_history as ph", (join) => {
            join
              .on("ranked.permission_id", "=", "ph.permission_id")
              .andOn("ranked.rn", "=", knex.raw("1"));
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

        if (!Array.isArray(joined) || joined.length === 0) {
          return ApiResponder.success(ctx, { trust_registries: [] }, 200);
        }

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
      const sortedFiltered = sortByStandardAttributes(filtered, sortParam, {
        getId: (item: any) => item.id,
        getCreated: (item: any) => item.created,
        getModified: (item: any) => item.modified,
        getParticipants: (item: any) => item.participants,
        getParticipantsEcosystem: (item: any) => item.participants_ecosystem,
        getParticipantsIssuerGrantor: (item: any) => item.participants_issuer_grantor,
        getParticipantsIssuer: (item: any) => item.participants_issuer,
        getParticipantsVerifierGrantor: (item: any) => item.participants_verifier_grantor,
        getParticipantsVerifier: (item: any) => item.participants_verifier,
        getParticipantsHolder: (item: any) => item.participants_holder,
        getWeight: (item: any) => item.weight,
        getIssued: (item: any) => item.issued,
        getVerified: (item: any) => item.verified,
        getEcosystemSlashEvents: (item: any) => item.ecosystem_slash_events,
        getEcosystemSlashedAmount: (item: any) => item.ecosystem_slashed_amount,
        getNetworkSlashEvents: (item: any) => item.network_slash_events,
        getNetworkSlashedAmount: (item: any) => item.network_slashed_amount,
        defaultAttribute: "modified",
        defaultDirection: "desc",
      });
      const schemaIds = Array.from(new Set(sortedFiltered.map((r: any) => Number(r.schema_id))));
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
        const schemaIdList = Array.from(schemaMap.keys());
        try {
          let latestSchemaRows: any[] = [];
          if (IS_PG_CLIENT) {
            latestSchemaRows = await knex("credential_schema_history as csh")
              .distinctOn("csh.credential_schema_id")
              .select("csh.credential_schema_id", knex.raw("COALESCE(csh.participants, 0) as participants"))
              .whereIn("csh.credential_schema_id", schemaIdList)
              .where("csh.height", "<=", Number(blockHeight))
              .orderBy("csh.credential_schema_id", "asc")
              .orderBy("csh.height", "desc")
              .orderBy("csh.created_at", "desc")
              .orderBy("csh.id", "desc");
          } else {
            const rankedSchemas = knex("credential_schema_history as csh")
              .select(
                "csh.credential_schema_id",
                knex.raw("COALESCE(csh.participants, 0) as participants"),
                knex.raw("ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn")
              )
              .whereIn("csh.credential_schema_id", schemaIdList)
              .where("csh.height", "<=", Number(blockHeight))
              .as("ranked");
            latestSchemaRows = await knex.from(rankedSchemas).select("credential_schema_id", "participants").where("rn", 1);
          }
          for (const row of latestSchemaRows) {
            const schemaId = Number(row.credential_schema_id);
            const cs = schemaMap.get(schemaId);
            if (cs) cs.participants = Number(row.participants || 0);
          }
        } catch {
          // Old deployments may not have stats columns in history tables.
        }
      }

      const trIds = Array.from(new Set(Array.from(schemaMap.values()).map((s: any) => s.tr_id).filter((x: any) => x !== null)));
      const trs = trIds.length > 0 ? await knex("trust_registry").whereIn("id", trIds).select("id", "did", "aka", "participants") : [];
      const trMap = new Map<number | string, any>();
      for (const tr of trs) {
        trMap.set(Number(tr.id), { id: Number(tr.id), did: tr.did, aka: tr.aka, credential_schemas: [], pending_tasks: 0, participants: tr.participants ?? 0 });
      }
      const csMap = new Map<number, any>();
      for (const perm of sortedFiltered) {
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
        const trIdList = Array.from(trMap.keys())
          .filter((trId) => trId !== "null")
          .map((trId) => Number(trId))
          .filter((trId) => Number.isFinite(trId) && trId > 0);
        if (trIdList.length > 0) {
          try {
            let latestTrRows: any[] = [];
            if (IS_PG_CLIENT) {
              latestTrRows = await knex("trust_registry_history as trh")
                .distinctOn("trh.tr_id")
                .select("trh.tr_id", knex.raw("COALESCE(trh.participants, 0) as participants"))
                .whereIn("trh.tr_id", trIdList)
                .where("trh.height", "<=", Number(blockHeight))
                .orderBy("trh.tr_id", "asc")
                .orderBy("trh.height", "desc")
                .orderBy("trh.created_at", "desc")
                .orderBy("trh.id", "desc");
            } else {
              const rankedTrs = knex("trust_registry_history as trh")
                .select(
                  "trh.tr_id",
                  knex.raw("COALESCE(trh.participants, 0) as participants"),
                  knex.raw("ROW_NUMBER() OVER (PARTITION BY trh.tr_id ORDER BY trh.height DESC, trh.created_at DESC, trh.id DESC) as rn")
                )
                .whereIn("trh.tr_id", trIdList)
                .where("trh.height", "<=", Number(blockHeight))
                .as("ranked");
              latestTrRows = await knex.from(rankedTrs).select("tr_id", "participants").where("rn", 1);
            }
            for (const row of latestTrRows) {
              const trId = Number(row.tr_id);
              const trEntry = trMap.get(trId);
              if (trEntry) trEntry.participants = Number(row.participants || 0);
            }
          } catch {
            // Fallback to live participants if historical stats are unavailable.
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
