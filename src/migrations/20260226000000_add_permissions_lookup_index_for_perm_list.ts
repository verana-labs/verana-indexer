import { Knex } from "knex";

export const config = { transaction: false };

const isPostgres = (knex: Knex): boolean =>
  String(knex.client.config?.client || "").includes("pg");

const hasColumns = async (knex: Knex, table: string, columns: string[]): Promise<boolean> => {
  const tableExists = await knex.schema.hasTable(table);
  if (!tableExists) return false;
  const checks = await Promise.all(columns.map((column) => knex.schema.hasColumn(table, column)));
  return checks.every(Boolean);
};

const LIVE_INDEX_NAME = "idx_permissions_did_schema_type_modified_id";
const LIVE_CORPORATION_QUERY_INDEX_NAME = "idx_permissions_corporation_schema_type_vp_validator_modified_id";
const LEGACY_LIVE_INDEX_NAMES = [
  "idx_permissions_did_type_schema_modified_id",
  "idx_permissions_did_type_schema_modified",
  "idx_permissions_did_schema_type_modified",
];
const HISTORY_INDEX_NAME = "idx_permission_history_did_schema_type_height_modified_created_id";
const HISTORY_RANKING_INDEX_NAME = "idx_permission_history_permission_height_created_id_desc";
const LIST_FILTERS_SORT_INDEX = "idx_permissions_schema_type_vp_validator_modified_id";
const LIST_FILTERS_SORT_ACTIVE_INDEX = "idx_permissions_active_schema_type_vp_validator_modified_id";
const HISTORY_LATEST_DID_SCHEMA_TYPE_IDX = "idx_permission_history_did_schema_type_permission_height_desc";
const HISTORY_LATEST_ACTIVE_DID_SCHEMA_TYPE_IDX = "idx_permission_history_did_schema_type_active_permission_height_desc";
const LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX = "idx_permissions_did_schema_type_active_modified_id";
const HISTORY_CORPORATION_HEIGHT_PERMISSION_IDX = "idx_permission_history_corporation_height_permission_created_id_desc";
const HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX = "idx_permission_history_validator_height_permission_created_id_desc";
const PERM_SESSION_HISTORY_RANKING_IDX = "idx_permission_session_history_session_height_created_id_desc";
const CS_HISTORY_RANKING_IDX = "idx_credential_schema_history_schema_height_created_id_desc";
const TR_HISTORY_RANKING_IDX = "idx_trust_registry_history_tr_height_created_id_desc";
const TD_HISTORY_CORPORATION_HEIGHT_CREATED_IDX = "idx_td_history_corporation_height_created_id_desc";
const PH_CORPORATION_SCHEMA_HEIGHT_CREATED_IDX = "idx_permission_history_corporation_schema_height_created_id_desc";
const CS_LIVE_TR_ARCHIVED_MODIFIED_IDX = "idx_cs_tr_archived_modified_id";
const CS_LIVE_MODES_MODIFIED_IDX = "idx_cs_modes_modified_id";
const CS_LIVE_PARTICIPANTS_IDX = "idx_cs_participants_id";
const CS_LIVE_WEIGHT_IDX = "idx_cs_weight_id";
const CS_LIVE_ISSUED_IDX = "idx_cs_issued_id";
const CS_LIVE_VERIFIED_IDX = "idx_cs_verified_id";
const CS_LIVE_ECO_SLASH_EVENTS_IDX = "idx_cs_eco_slash_events_id";
const CS_LIVE_NET_SLASH_EVENTS_IDX = "idx_cs_net_slash_events_id";
const TR_LIVE_CORPORATION_ARCHIVED_MODIFIED_IDX = "idx_tr_corporation_archived_modified_id";
const TR_LIVE_PARTICIPANTS_IDX = "idx_tr_participants_id";
const TR_LIVE_ACTIVE_SCHEMAS_IDX = "idx_tr_active_schemas_id";
const TR_LIVE_WEIGHT_IDX = "idx_tr_weight_id";
const TR_LIVE_ISSUED_IDX = "idx_tr_issued_id";
const TR_LIVE_VERIFIED_IDX = "idx_tr_verified_id";
const TR_LIVE_ECO_SLASH_EVENTS_IDX = "idx_tr_eco_slash_events_id";
const TR_LIVE_NET_SLASH_EVENTS_IDX = "idx_tr_net_slash_events_id";
const GM_BLOCK_HEIGHT_COMPUTED_AT_IDX = "idx_global_metrics_block_height_computed_at_desc";
const PERM_SESSIONS_MODIFIED_ID_IDX = "idx_permission_sessions_modified_id_desc";
const PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX = "idx_permission_session_history_session_height_modified_created_id_desc";
const TR_HISTORY_CORPORATION_HEIGHT_MODIFIED_TR_IDX = "idx_tr_history_corporation_height_modified_tr_created_id_desc";
const GFV_HISTORY_TR_HEIGHT_CREATED_ID_IDX = "idx_gfv_history_tr_height_created_id_desc";
const GFV_HISTORY_TR_VERSION_HEIGHT_CREATED_ID_IDX = "idx_gfv_history_tr_version_height_created_id_desc";
const GFD_HISTORY_GFV_TR_HEIGHT_CREATED_ID_IDX = "idx_gfd_history_gfv_tr_height_created_id_desc";
const GFD_HISTORY_TR_HEIGHT_CREATED_ID_IDX = "idx_gfd_history_tr_height_created_id_desc";
const MODULE_PARAMS_HISTORY_MODULE_HEIGHT_CREATED_ID_IDX = "idx_module_params_history_module_height_created_id_desc";
const PERMISSIONS_COUNTRY_MODIFIED_ID_IDX = "idx_permissions_country_modified_id_desc";
const PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX = "idx_permission_history_schema_permission_height_created_id_desc";
const CSH_TR_SCHEMA_HEIGHT_CREATED_ID_IDX = "idx_credential_schema_history_tr_schema_height_created_id_desc";
const PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX = "idx_permission_history_schema_event_height_permission_created_id_desc";
const STATS_ENTITY_GRANULARITY_ID_TIME_IDX = "idx_stats_entity_granularity_id_time";
const STATS_GLOBAL_GRANULARITY_TIME_IDX = "idx_stats_global_granularity_time";
const LEGACY_REDUNDANT_HISTORY_INDEXES = [
  "idx_permission_history_permission_height_desc",
  "idx_permission_history_grantee_height_desc",
];

export async function up(knex: Knex): Promise<void> {
  const pg = isPostgres(knex);
  const liveColumns = ["did", "schema_id", "type", "modified", "id"];
  const liveCorporationQueryColumns = ["corporation", "schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const listFiltersSortColumns = ["schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const activeListFiltersColumns = ["schema_id", "type", "vp_state", "validator_perm_id", "modified", "id", "slashed", "repaid"];
  const historyColumns = ["did", "schema_id", "type", "height", "modified", "created_at", "id"];
  const historyRankingColumns = ["permission_id", "height", "created_at", "id"];
  const historyLatestColumns = ["did", "schema_id", "type", "permission_id", "height", "created_at", "id"];
  const historyLatestActiveColumns = ["did", "schema_id", "type", "permission_id", "height", "created_at", "id", "slashed", "repaid"];
  const liveActiveDidColumns = ["did", "schema_id", "type", "modified", "id", "slashed", "repaid"];
  const historyByCorporationColumns = ["corporation", "height", "permission_id", "created_at", "id"];
  const historyByValidatorColumns = ["validator_perm_id", "height", "permission_id", "created_at", "id"];
  const sessionHistoryRankingColumns = ["session_id", "height", "created_at", "id"];
  const csHistoryRankingColumns = ["credential_schema_id", "height", "created_at", "id"];
  const trHistoryRankingColumns = ["tr_id", "height", "created_at", "id"];
  const tdHistoryRankingColumns = ["corporation", "height", "created_at", "id"];
  const historyCorporationSchemaColumns = ["corporation", "schema_id", "height", "created_at", "id"];
  const csTrArchivedModifiedColumns = ["tr_id", "archived", "modified", "id"];
  const csModesColumns = ["issuer_onboarding_mode", "verifier_onboarding_mode", "modified", "id"];
  const csParticipantsColumns = ["participants", "id"];
  const csWeightColumns = ["weight", "id"];
  const csIssuedColumns = ["issued", "id"];
  const csVerifiedColumns = ["verified", "id"];
  const csEcoSlashColumns = ["ecosystem_slash_events", "id"];
  const csNetSlashColumns = ["network_slash_events", "id"];
  const trCorporationArchivedModifiedColumns = ["corporation", "archived", "modified", "id"];
  const trParticipantsColumns = ["participants", "id"];
  const trActiveSchemasColumns = ["active_schemas", "id"];
  const trWeightColumns = ["weight", "id"];
  const trIssuedColumns = ["issued", "id"];
  const trVerifiedColumns = ["verified", "id"];
  const trEcoSlashColumns = ["ecosystem_slash_events", "id"];
  const trNetSlashColumns = ["network_slash_events", "id"];
  const gmLookupColumns = ["block_height", "computed_at"];
  const permSessionsModifiedColumns = ["modified", "id"];
  const permSessionHistorySessionHeightModifiedColumns = ["session_id", "height", "modified", "created_at", "id"];
  const trHistoryCorporationHeightModifiedColumns = ["corporation", "height", "modified", "tr_id", "created_at", "id"];
  const gfvHistoryTrHeightColumns = ["tr_id", "height", "created_at", "id"];
  const gfvHistoryTrVersionHeightColumns = ["tr_id", "version", "height", "created_at", "id"];
  const gfdHistoryGfvTrHeightColumns = ["gfv_id", "tr_id", "height", "created_at", "id"];
  const gfdHistoryTrHeightColumns = ["tr_id", "height", "created_at", "id"];
  const moduleParamsHistoryModuleHeightColumns = ["module", "height", "created_at", "id"];
  const permissionsCountryColumns = ["country", "modified", "id"];
  const phSchemaPermissionHeightColumns = ["schema_id", "permission_id", "height", "created_at", "id"];
  const cshTrSchemaHeightColumns = ["tr_id", "credential_schema_id", "height", "created_at", "id"];
  const phSchemaEventHeightPermissionColumns = ["schema_id", "event_type", "height", "permission_id", "created_at", "id"];
  const statsEntityGranularityIdTimeColumns = ["entity_type", "granularity", "entity_id", "timestamp"];
  const statsGlobalGranularityTimeColumns = ["entity_type", "granularity", "timestamp"];

  const [hasLiveColumns, hasLiveCorporationQueryColumns, hasListFiltersSortColumns, hasActiveListFiltersColumns, hasHistoryColumns, hasHistoryRankingColumns, hasHistoryLatestColumns, hasHistoryLatestActiveColumns, hasLiveActiveDidColumns, hasHistoryByCorporationColumns, hasHistoryByValidatorColumns, hasSessionHistoryRankingColumns, hasCsHistoryRankingColumns, hasTrHistoryRankingColumns, hasTdHistoryRankingColumns, hasHistoryCorporationSchemaColumns, hasCsTrArchivedModifiedColumns, hasCsModesColumns, hasCsParticipantsColumns, hasCsWeightColumns, hasCsIssuedColumns, hasCsVerifiedColumns, hasCsEcoSlashColumns, hasCsNetSlashColumns, hasTrCorporationArchivedModifiedColumns, hasTrParticipantsColumns, hasTrActiveSchemasColumns, hasTrWeightColumns, hasTrIssuedColumns, hasTrVerifiedColumns, hasTrEcoSlashColumns, hasTrNetSlashColumns, hasGlobalMetricsLookupColumns, hasPermSessionsModifiedColumns, hasPermSessionHistorySessionHeightModifiedColumns, hasTrHistoryCorporationHeightModifiedColumns, hasGfvHistoryTrHeightColumns, hasGfvHistoryTrVersionHeightColumns, hasGfdHistoryGfvTrHeightColumns, hasGfdHistoryTrHeightColumns, hasModuleParamsHistoryModuleHeightColumns, hasPermissionsCountryColumns, hasPhSchemaPermissionHeightColumns, hasCshTrSchemaHeightColumns, hasPhSchemaEventHeightPermissionColumns, hasStatsEntityGranularityIdTimeColumns, hasStatsGlobalGranularityTimeColumns] = await Promise.all([
    hasColumns(knex, "permissions", liveColumns),
    hasColumns(knex, "permissions", liveCorporationQueryColumns),
    hasColumns(knex, "permissions", listFiltersSortColumns),
    hasColumns(knex, "permissions", activeListFiltersColumns),
    hasColumns(knex, "permission_history", historyColumns),
    hasColumns(knex, "permission_history", historyRankingColumns),
    hasColumns(knex, "permission_history", historyLatestColumns),
    hasColumns(knex, "permission_history", historyLatestActiveColumns),
    hasColumns(knex, "permissions", liveActiveDidColumns),
    hasColumns(knex, "permission_history", historyByCorporationColumns),
    hasColumns(knex, "permission_history", historyByValidatorColumns),
    hasColumns(knex, "permission_session_history", sessionHistoryRankingColumns),
    hasColumns(knex, "credential_schema_history", csHistoryRankingColumns),
    hasColumns(knex, "trust_registry_history", trHistoryRankingColumns),
    hasColumns(knex, "trust_deposit_history", tdHistoryRankingColumns),
    hasColumns(knex, "permission_history", historyCorporationSchemaColumns),
    hasColumns(knex, "credential_schemas", csTrArchivedModifiedColumns),
    hasColumns(knex, "credential_schemas", csModesColumns),
    hasColumns(knex, "credential_schemas", csParticipantsColumns),
    hasColumns(knex, "credential_schemas", csWeightColumns),
    hasColumns(knex, "credential_schemas", csIssuedColumns),
    hasColumns(knex, "credential_schemas", csVerifiedColumns),
    hasColumns(knex, "credential_schemas", csEcoSlashColumns),
    hasColumns(knex, "credential_schemas", csNetSlashColumns),
    hasColumns(knex, "trust_registry", trCorporationArchivedModifiedColumns),
    hasColumns(knex, "trust_registry", trParticipantsColumns),
    hasColumns(knex, "trust_registry", trActiveSchemasColumns),
    hasColumns(knex, "trust_registry", trWeightColumns),
    hasColumns(knex, "trust_registry", trIssuedColumns),
    hasColumns(knex, "trust_registry", trVerifiedColumns),
    hasColumns(knex, "trust_registry", trEcoSlashColumns),
    hasColumns(knex, "trust_registry", trNetSlashColumns),
    hasColumns(knex, "global_metrics", gmLookupColumns),
    hasColumns(knex, "permission_sessions", permSessionsModifiedColumns),
    hasColumns(knex, "permission_session_history", permSessionHistorySessionHeightModifiedColumns),
    hasColumns(knex, "trust_registry_history", trHistoryCorporationHeightModifiedColumns),
    hasColumns(knex, "governance_framework_version_history", gfvHistoryTrHeightColumns),
    hasColumns(knex, "governance_framework_version_history", gfvHistoryTrVersionHeightColumns),
    hasColumns(knex, "governance_framework_document_history", gfdHistoryGfvTrHeightColumns),
    hasColumns(knex, "governance_framework_document_history", gfdHistoryTrHeightColumns),
    hasColumns(knex, "module_params_history", moduleParamsHistoryModuleHeightColumns),
    hasColumns(knex, "permissions", permissionsCountryColumns),
    hasColumns(knex, "permission_history", phSchemaPermissionHeightColumns),
    hasColumns(knex, "credential_schema_history", cshTrSchemaHeightColumns),
    hasColumns(knex, "permission_history", phSchemaEventHeightPermissionColumns),
    hasColumns(knex, "stats", statsEntityGranularityIdTimeColumns),
    hasColumns(knex, "stats", statsGlobalGranularityTimeColumns),
  ]);

  if (pg) {
    for (const legacyIndexName of LEGACY_REDUNDANT_HISTORY_INDEXES) {
      await knex.raw(`DROP INDEX CONCURRENTLY IF EXISTS ${legacyIndexName}`);
    }
    if (hasLiveColumns) {
      for (const legacyIndexName of LEGACY_LIVE_INDEX_NAMES) {
        await knex.raw(`DROP INDEX CONCURRENTLY IF EXISTS ${legacyIndexName}`);
      }
      await knex.raw(`DROP INDEX CONCURRENTLY IF EXISTS ${LIVE_INDEX_NAME}`);
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIVE_INDEX_NAME}
        ON permissions (did, schema_id, type, modified DESC, id DESC)
      `);
      if (hasLiveCorporationQueryColumns) {
        await knex.raw(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIVE_CORPORATION_QUERY_INDEX_NAME}
          ON permissions (corporation, schema_id, type, vp_state, validator_perm_id, modified ASC, id DESC)
        `);
      }
      if (hasListFiltersSortColumns) {
        await knex.raw(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIST_FILTERS_SORT_INDEX}
          ON permissions (schema_id, type, vp_state, validator_perm_id, modified ASC, id DESC)
        `);
      }
      if (hasActiveListFiltersColumns) {
        await knex.raw(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIST_FILTERS_SORT_ACTIVE_INDEX}
          ON permissions (schema_id, type, vp_state, validator_perm_id, modified ASC, id DESC)
          WHERE slashed IS NULL AND repaid IS NULL
        `);
      }
    }

    if (hasHistoryColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_INDEX_NAME}
        ON permission_history (did, schema_id, type, height DESC, modified DESC, created_at DESC, id DESC)
      `);
    }
    if (hasHistoryRankingColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_RANKING_INDEX_NAME}
        ON permission_history (permission_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasHistoryLatestColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_LATEST_DID_SCHEMA_TYPE_IDX}
        ON permission_history (did, schema_id, type, permission_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasHistoryLatestActiveColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_LATEST_ACTIVE_DID_SCHEMA_TYPE_IDX}
        ON permission_history (did, schema_id, type, permission_id, height DESC, created_at DESC, id DESC)
        WHERE slashed IS NULL AND repaid IS NULL
      `);
    }
    if (hasLiveActiveDidColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX}
        ON permissions (did, schema_id, type, modified ASC, id DESC)
        WHERE slashed IS NULL AND repaid IS NULL
      `);
    }
    if (hasHistoryByCorporationColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_CORPORATION_HEIGHT_PERMISSION_IDX}
        ON permission_history (corporation, height DESC, permission_id, created_at DESC, id DESC)
      `);
    }
    if (hasHistoryByValidatorColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX}
        ON permission_history (validator_perm_id, height DESC, permission_id, created_at DESC, id DESC)
      `);
    }
    if (hasSessionHistoryRankingColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PERM_SESSION_HISTORY_RANKING_IDX}
        ON permission_session_history (session_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasCsHistoryRankingColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_HISTORY_RANKING_IDX}
        ON credential_schema_history (credential_schema_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasTrHistoryRankingColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_HISTORY_RANKING_IDX}
        ON trust_registry_history (tr_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasTdHistoryRankingColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TD_HISTORY_CORPORATION_HEIGHT_CREATED_IDX}
        ON trust_deposit_history (corporation, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasHistoryCorporationSchemaColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PH_CORPORATION_SCHEMA_HEIGHT_CREATED_IDX}
        ON permission_history (corporation, schema_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasCsTrArchivedModifiedColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_TR_ARCHIVED_MODIFIED_IDX}
        ON credential_schemas (tr_id, archived, modified DESC, id DESC)
      `);
    }
    if (hasCsModesColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_MODES_MODIFIED_IDX}
        ON credential_schemas (issuer_onboarding_mode, verifier_onboarding_mode, modified DESC, id DESC)
      `);
    }
    if (hasCsParticipantsColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_PARTICIPANTS_IDX} ON credential_schemas (participants, id DESC)`);
    if (hasCsWeightColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_WEIGHT_IDX} ON credential_schemas (weight, id DESC)`);
    if (hasCsIssuedColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_ISSUED_IDX} ON credential_schemas (issued, id DESC)`);
    if (hasCsVerifiedColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_VERIFIED_IDX} ON credential_schemas (verified, id DESC)`);
    if (hasCsEcoSlashColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_ECO_SLASH_EVENTS_IDX} ON credential_schemas (ecosystem_slash_events, id DESC)`);
    if (hasCsNetSlashColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CS_LIVE_NET_SLASH_EVENTS_IDX} ON credential_schemas (network_slash_events, id DESC)`);
    if (hasTrCorporationArchivedModifiedColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_CORPORATION_ARCHIVED_MODIFIED_IDX}
        ON trust_registry (corporation, archived, modified DESC, id DESC)
      `);
    }
    if (hasTrParticipantsColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_PARTICIPANTS_IDX} ON trust_registry (participants, id DESC)`);
    if (hasTrActiveSchemasColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_ACTIVE_SCHEMAS_IDX} ON trust_registry (active_schemas, id DESC)`);
    if (hasTrWeightColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_WEIGHT_IDX} ON trust_registry (weight, id DESC)`);
    if (hasTrIssuedColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_ISSUED_IDX} ON trust_registry (issued, id DESC)`);
    if (hasTrVerifiedColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_VERIFIED_IDX} ON trust_registry (verified, id DESC)`);
    if (hasTrEcoSlashColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_ECO_SLASH_EVENTS_IDX} ON trust_registry (ecosystem_slash_events, id DESC)`);
    if (hasTrNetSlashColumns) await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_LIVE_NET_SLASH_EVENTS_IDX} ON trust_registry (network_slash_events, id DESC)`);
    if (hasGlobalMetricsLookupColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${GM_BLOCK_HEIGHT_COMPUTED_AT_IDX}
        ON global_metrics (block_height DESC, computed_at DESC)
      `);
    }
    if (hasPermSessionsModifiedColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PERM_SESSIONS_MODIFIED_ID_IDX}
        ON permission_sessions (modified DESC, id DESC)
      `);
    }
    if (hasPermSessionHistorySessionHeightModifiedColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX}
        ON permission_session_history (session_id, height DESC, modified DESC, created_at DESC, id DESC)
      `);
    }
    if (hasTrHistoryCorporationHeightModifiedColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${TR_HISTORY_CORPORATION_HEIGHT_MODIFIED_TR_IDX}
        ON trust_registry_history (corporation, height DESC, modified DESC, tr_id, created_at DESC, id DESC)
      `);
    }
    if (hasGfvHistoryTrHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${GFV_HISTORY_TR_HEIGHT_CREATED_ID_IDX}
        ON governance_framework_version_history (tr_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasGfvHistoryTrVersionHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${GFV_HISTORY_TR_VERSION_HEIGHT_CREATED_ID_IDX}
        ON governance_framework_version_history (tr_id, version, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasGfdHistoryGfvTrHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${GFD_HISTORY_GFV_TR_HEIGHT_CREATED_ID_IDX}
        ON governance_framework_document_history (gfv_id, tr_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasGfdHistoryTrHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${GFD_HISTORY_TR_HEIGHT_CREATED_ID_IDX}
        ON governance_framework_document_history (tr_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasModuleParamsHistoryModuleHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${MODULE_PARAMS_HISTORY_MODULE_HEIGHT_CREATED_ID_IDX}
        ON module_params_history (module, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasPermissionsCountryColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PERMISSIONS_COUNTRY_MODIFIED_ID_IDX}
        ON permissions (country, modified DESC, id DESC)
      `);
    }
    if (hasPhSchemaPermissionHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX}
        ON permission_history (schema_id, permission_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasCshTrSchemaHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${CSH_TR_SCHEMA_HEIGHT_CREATED_ID_IDX}
        ON credential_schema_history (tr_id, credential_schema_id, height DESC, created_at DESC, id DESC)
      `);
    }
    if (hasPhSchemaEventHeightPermissionColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX}
        ON permission_history (schema_id, event_type, height DESC, permission_id, created_at DESC, id DESC)
      `);
    }
    if (hasStatsEntityGranularityIdTimeColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${STATS_ENTITY_GRANULARITY_ID_TIME_IDX}
        ON stats (entity_type, granularity, entity_id, "timestamp")
      `);
    }
    if (hasStatsGlobalGranularityTimeColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${STATS_GLOBAL_GRANULARITY_TIME_IDX}
        ON stats (entity_type, granularity, "timestamp")
        WHERE entity_id IS NULL
      `);
    }
    return;
  }

  if (hasLiveColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(liveColumns, LIVE_INDEX_NAME);
    });
  }
  if (hasLiveCorporationQueryColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(liveCorporationQueryColumns, LIVE_CORPORATION_QUERY_INDEX_NAME);
    });
  }
  if (hasListFiltersSortColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(listFiltersSortColumns, LIST_FILTERS_SORT_INDEX);
    });
  }

  if (hasHistoryColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyColumns, HISTORY_INDEX_NAME);
    });
  }
  if (hasHistoryRankingColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyRankingColumns, HISTORY_RANKING_INDEX_NAME);
    });
  }
  if (hasHistoryLatestColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyLatestColumns, HISTORY_LATEST_DID_SCHEMA_TYPE_IDX);
    });
  }
  if (hasLiveActiveDidColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(["did", "schema_id", "type", "modified", "id"], LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX);
    });
  }
  if (hasHistoryByCorporationColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyByCorporationColumns, HISTORY_CORPORATION_HEIGHT_PERMISSION_IDX);
    });
  }
  if (hasHistoryByValidatorColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyByValidatorColumns, HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX);
    });
  }
  if (hasSessionHistoryRankingColumns) {
    await knex.schema.table("permission_session_history", (table) => {
      table.index(sessionHistoryRankingColumns, PERM_SESSION_HISTORY_RANKING_IDX);
    });
  }
  if (hasCsHistoryRankingColumns) {
    await knex.schema.table("credential_schema_history", (table) => {
      table.index(csHistoryRankingColumns, CS_HISTORY_RANKING_IDX);
    });
  }
  if (hasTrHistoryRankingColumns) {
    await knex.schema.table("trust_registry_history", (table) => {
      table.index(trHistoryRankingColumns, TR_HISTORY_RANKING_IDX);
    });
  }
  if (hasTdHistoryRankingColumns) {
    await knex.schema.table("trust_deposit_history", (table) => {
      table.index(tdHistoryRankingColumns, TD_HISTORY_CORPORATION_HEIGHT_CREATED_IDX);
    });
  }
  if (hasStatsEntityGranularityIdTimeColumns) {
    await knex.schema.table("stats", (table) => {
      table.index(statsEntityGranularityIdTimeColumns, STATS_ENTITY_GRANULARITY_ID_TIME_IDX);
    });
  }
  if (hasStatsGlobalGranularityTimeColumns) {
    await knex.schema.table("stats", (table) => {
      table.index(statsGlobalGranularityTimeColumns, STATS_GLOBAL_GRANULARITY_TIME_IDX);
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  const pg = isPostgres(knex);
  const liveColumns = ["did", "schema_id", "type", "modified", "id"];
  const liveCorporationQueryColumns = ["corporation", "schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const listFiltersSortColumns = ["schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const historyColumns = ["did", "schema_id", "type", "height", "modified", "created_at", "id"];
  const historyRankingColumns = ["permission_id", "height", "created_at", "id"];
  const historyLatestColumns = ["did", "schema_id", "type", "permission_id", "height", "created_at", "id"];
  const liveActiveDidDropColumns = ["did", "schema_id", "type", "modified", "id"];
  const historyByCorporationColumns = ["corporation", "height", "permission_id", "created_at", "id"];
  const historyByValidatorColumns = ["validator_perm_id", "height", "permission_id", "created_at", "id"];
  const sessionHistoryRankingColumns = ["session_id", "height", "created_at", "id"];
  const csHistoryRankingColumns = ["credential_schema_id", "height", "created_at", "id"];
  const trHistoryRankingColumns = ["tr_id", "height", "created_at", "id"];
  const tdHistoryRankingColumns = ["corporation", "height", "created_at", "id"];
  const statsEntityGranularityIdTimeColumns = ["entity_type", "granularity", "entity_id", "timestamp"];
  const statsGlobalGranularityTimeColumns = ["entity_type", "granularity", "timestamp"];

  if (pg) {
    for (const indexName of [
      LIVE_INDEX_NAME,
      LIVE_CORPORATION_QUERY_INDEX_NAME,
      LIST_FILTERS_SORT_INDEX,
      LIST_FILTERS_SORT_ACTIVE_INDEX,
      HISTORY_LATEST_DID_SCHEMA_TYPE_IDX,
      HISTORY_LATEST_ACTIVE_DID_SCHEMA_TYPE_IDX,
      LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX,
      ...LEGACY_LIVE_INDEX_NAMES,
      ...LEGACY_REDUNDANT_HISTORY_INDEXES,
      HISTORY_INDEX_NAME,
      HISTORY_RANKING_INDEX_NAME,
      HISTORY_CORPORATION_HEIGHT_PERMISSION_IDX,
      HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX,
      PERM_SESSION_HISTORY_RANKING_IDX,
      CS_HISTORY_RANKING_IDX,
      TR_HISTORY_RANKING_IDX,
      TD_HISTORY_CORPORATION_HEIGHT_CREATED_IDX,
      PH_CORPORATION_SCHEMA_HEIGHT_CREATED_IDX,
      CS_LIVE_TR_ARCHIVED_MODIFIED_IDX,
      CS_LIVE_MODES_MODIFIED_IDX,
      CS_LIVE_PARTICIPANTS_IDX,
      CS_LIVE_WEIGHT_IDX,
      CS_LIVE_ISSUED_IDX,
      CS_LIVE_VERIFIED_IDX,
      CS_LIVE_ECO_SLASH_EVENTS_IDX,
      CS_LIVE_NET_SLASH_EVENTS_IDX,
      TR_LIVE_CORPORATION_ARCHIVED_MODIFIED_IDX,
      TR_LIVE_PARTICIPANTS_IDX,
      TR_LIVE_ACTIVE_SCHEMAS_IDX,
      TR_LIVE_WEIGHT_IDX,
      TR_LIVE_ISSUED_IDX,
      TR_LIVE_VERIFIED_IDX,
      TR_LIVE_ECO_SLASH_EVENTS_IDX,
      TR_LIVE_NET_SLASH_EVENTS_IDX,
      GM_BLOCK_HEIGHT_COMPUTED_AT_IDX,
      PERM_SESSIONS_MODIFIED_ID_IDX,
      PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX,
      TR_HISTORY_CORPORATION_HEIGHT_MODIFIED_TR_IDX,
      GFV_HISTORY_TR_HEIGHT_CREATED_ID_IDX,
      GFV_HISTORY_TR_VERSION_HEIGHT_CREATED_ID_IDX,
      GFD_HISTORY_GFV_TR_HEIGHT_CREATED_ID_IDX,
      GFD_HISTORY_TR_HEIGHT_CREATED_ID_IDX,
      MODULE_PARAMS_HISTORY_MODULE_HEIGHT_CREATED_ID_IDX,
      PERMISSIONS_COUNTRY_MODIFIED_ID_IDX,
      PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX,
      CSH_TR_SCHEMA_HEIGHT_CREATED_ID_IDX,
      PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX,
      STATS_ENTITY_GRANULARITY_ID_TIME_IDX,
      STATS_GLOBAL_GRANULARITY_TIME_IDX,
    ]) {
      await knex.raw(`DROP INDEX IF EXISTS ${indexName}`);
    }
    return;
  }

  const [hasLiveColumns, hasLiveCorporationQueryColumns, hasListFiltersSortColumns, hasHistoryColumns, hasHistoryRankingColumns, hasHistoryLatestColumns, hasLiveActiveDidColumns, hasHistoryByCorporationColumns, hasHistoryByValidatorColumns, hasSessionHistoryRankingColumns, hasCsHistoryRankingColumns, hasTrHistoryRankingColumns, hasTdHistoryRankingColumns, hasStatsEntityGranularityIdTimeColumns, hasStatsGlobalGranularityTimeColumns] = await Promise.all([
    hasColumns(knex, "permissions", liveColumns),
    hasColumns(knex, "permissions", liveCorporationQueryColumns),
    hasColumns(knex, "permissions", listFiltersSortColumns),
    hasColumns(knex, "permission_history", historyColumns),
    hasColumns(knex, "permission_history", historyRankingColumns),
    hasColumns(knex, "permission_history", historyLatestColumns),
    hasColumns(knex, "permissions", liveActiveDidDropColumns),
    hasColumns(knex, "permission_history", historyByCorporationColumns),
    hasColumns(knex, "permission_history", historyByValidatorColumns),
    hasColumns(knex, "permission_session_history", sessionHistoryRankingColumns),
    hasColumns(knex, "credential_schema_history", csHistoryRankingColumns),
    hasColumns(knex, "trust_registry_history", trHistoryRankingColumns),
    hasColumns(knex, "trust_deposit_history", tdHistoryRankingColumns),
    hasColumns(knex, "stats", statsEntityGranularityIdTimeColumns),
    hasColumns(knex, "stats", statsGlobalGranularityTimeColumns),
  ]);

  if (hasLiveColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(liveColumns, LIVE_INDEX_NAME);
    });
  }
  if (hasLiveCorporationQueryColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(liveCorporationQueryColumns, LIVE_CORPORATION_QUERY_INDEX_NAME);
    });
  }
  if (hasListFiltersSortColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(listFiltersSortColumns, LIST_FILTERS_SORT_INDEX);
    });
  }

  if (hasHistoryColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyColumns, HISTORY_INDEX_NAME);
    });
  }

  if (hasHistoryRankingColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyRankingColumns, HISTORY_RANKING_INDEX_NAME);
    });
  }
  if (hasHistoryLatestColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyLatestColumns, HISTORY_LATEST_DID_SCHEMA_TYPE_IDX);
    });
  }
  if (hasLiveActiveDidColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(liveActiveDidDropColumns, LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX);
    });
  }
  if (hasHistoryByCorporationColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyByCorporationColumns, HISTORY_CORPORATION_HEIGHT_PERMISSION_IDX);
    });
  }
  if (hasHistoryByValidatorColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyByValidatorColumns, HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX);
    });
  }
  if (hasSessionHistoryRankingColumns) {
    await knex.schema.table("permission_session_history", (table) => {
      table.dropIndex(sessionHistoryRankingColumns, PERM_SESSION_HISTORY_RANKING_IDX);
    });
  }
  if (hasCsHistoryRankingColumns) {
    await knex.schema.table("credential_schema_history", (table) => {
      table.dropIndex(csHistoryRankingColumns, CS_HISTORY_RANKING_IDX);
    });
  }
  if (hasTrHistoryRankingColumns) {
    await knex.schema.table("trust_registry_history", (table) => {
      table.dropIndex(trHistoryRankingColumns, TR_HISTORY_RANKING_IDX);
    });
  }
  if (hasTdHistoryRankingColumns) {
    await knex.schema.table("trust_deposit_history", (table) => {
      table.dropIndex(tdHistoryRankingColumns, TD_HISTORY_CORPORATION_HEIGHT_CREATED_IDX);
    });
  }
  if (hasStatsEntityGranularityIdTimeColumns) {
    await knex.schema.table("stats", (table) => {
      table.dropIndex(statsEntityGranularityIdTimeColumns, STATS_ENTITY_GRANULARITY_ID_TIME_IDX);
    });
  }
  if (hasStatsGlobalGranularityTimeColumns) {
    await knex.schema.table("stats", (table) => {
      table.dropIndex(statsGlobalGranularityTimeColumns, STATS_GLOBAL_GRANULARITY_TIME_IDX);
    });
  }
}
