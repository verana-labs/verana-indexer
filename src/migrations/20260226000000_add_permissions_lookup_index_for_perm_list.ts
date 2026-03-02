import { Knex } from "knex";

export const config = { transaction: false };

const isPostgres = (knex: Knex): boolean =>
  String(knex.client.config?.client || "").includes("pg");

const hasColumns = async (knex: Knex, table: string, columns: string[]): Promise<boolean> => {
  const checks = await Promise.all(columns.map((column) => knex.schema.hasColumn(table, column)));
  return checks.every(Boolean);
};

const LIVE_INDEX_NAME = "idx_permissions_did_schema_type_modified_id";
const LIVE_GRANTEE_QUERY_INDEX_NAME = "idx_permissions_grantee_schema_type_vp_validator_modified_id";
const LIST_FILTERS_SORT_INDEX = "idx_permissions_schema_type_vp_validator_modified_id";
const LIST_FILTERS_SORT_ACTIVE_INDEX = "idx_permissions_active_schema_type_vp_validator_modified_id";
const LEGACY_LIVE_INDEX_NAMES = [
  "idx_permissions_did_type_schema_modified_id",
  "idx_permissions_did_type_schema_modified",
  "idx_permissions_did_schema_type_modified",
];

const HISTORY_INDEX_NAME = "idx_permission_history_did_schema_type_height_modified_created_id";
const HISTORY_RANKING_INDEX_NAME = "idx_permission_history_permission_height_created_id_desc";
const HISTORY_LATEST_DID_SCHEMA_TYPE_IDX = "idx_permission_history_did_schema_type_permission_height_desc";
const HISTORY_LATEST_ACTIVE_DID_SCHEMA_TYPE_IDX = "idx_permission_history_did_schema_type_active_permission_height_desc";
const LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX = "idx_permissions_did_schema_type_active_modified_id";
const HISTORY_GRANTEE_HEIGHT_PERMISSION_IDX = "idx_permission_history_grantee_height_permission_created_id_desc";
const HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX = "idx_permission_history_validator_height_permission_created_id_desc";
const PH_GRANTEE_SCHEMA_HEIGHT_CREATED_IDX = "idx_permission_history_grantee_schema_height_created_id_desc";
const PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX = "idx_permission_history_schema_permission_height_created_id_desc";
const PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX = "idx_permission_history_schema_event_height_permission_created_id_desc";
const LEGACY_REDUNDANT_HISTORY_INDEXES = [
  "idx_permission_history_permission_height_desc",
  "idx_permission_history_grantee_height_desc",
];

const PERM_SESSION_HISTORY_RANKING_IDX = "idx_permission_session_history_session_height_created_id_desc";
const PERM_SESSIONS_MODIFIED_ID_IDX = "idx_permission_sessions_modified_id_desc";
const PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX = "idx_permission_session_history_session_height_modified_created_id_desc";
const PERMISSIONS_COUNTRY_MODIFIED_ID_IDX = "idx_permissions_country_modified_id_desc";

export async function up(knex: Knex): Promise<void> {
  const pg = isPostgres(knex);

  const liveColumns = ["did", "schema_id", "type", "modified", "id"];
  const liveGranteeQueryColumns = ["grantee", "schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const listFiltersSortColumns = ["schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const activeListFiltersColumns = ["schema_id", "type", "vp_state", "validator_perm_id", "modified", "id", "slashed", "repaid"];
  const historyColumns = ["did", "schema_id", "type", "height", "modified", "created_at", "id"];
  const historyRankingColumns = ["permission_id", "height", "created_at", "id"];
  const historyLatestColumns = ["did", "schema_id", "type", "permission_id", "height", "created_at", "id"];
  const historyLatestActiveColumns = ["did", "schema_id", "type", "permission_id", "height", "created_at", "id", "slashed", "repaid"];
  const liveActiveDidColumns = ["did", "schema_id", "type", "modified", "id", "slashed", "repaid"];
  const historyByGranteeColumns = ["grantee", "height", "permission_id", "created_at", "id"];
  const historyByValidatorColumns = ["validator_perm_id", "height", "permission_id", "created_at", "id"];
  const historyGranteeSchemaColumns = ["grantee", "schema_id", "height", "created_at", "id"];
  const phSchemaPermissionHeightColumns = ["schema_id", "permission_id", "height", "created_at", "id"];
  const phSchemaEventHeightPermissionColumns = ["schema_id", "event_type", "height", "permission_id", "created_at", "id"];
  const sessionHistoryRankingColumns = ["session_id", "height", "created_at", "id"];
  const permSessionsModifiedColumns = ["modified", "id"];
  const permSessionHistorySessionHeightModifiedColumns = ["session_id", "height", "modified", "created_at", "id"];
  const permissionsCountryColumns = ["country", "modified", "id"];

  const [
    hasLiveColumns,
    hasLiveGranteeQueryColumns,
    hasListFiltersSortColumns,
    hasActiveListFiltersColumns,
    hasHistoryColumns,
    hasHistoryRankingColumns,
    hasHistoryLatestColumns,
    hasHistoryLatestActiveColumns,
    hasLiveActiveDidColumns,
    hasHistoryByGranteeColumns,
    hasHistoryByValidatorColumns,
    hasHistoryGranteeSchemaColumns,
    hasPhSchemaPermissionHeightColumns,
    hasPhSchemaEventHeightPermissionColumns,
    hasSessionHistoryRankingColumns,
    hasPermSessionsModifiedColumns,
    hasPermSessionHistorySessionHeightModifiedColumns,
    hasPermissionsCountryColumns,
  ] = await Promise.all([
    hasColumns(knex, "permissions", liveColumns),
    hasColumns(knex, "permissions", liveGranteeQueryColumns),
    hasColumns(knex, "permissions", listFiltersSortColumns),
    hasColumns(knex, "permissions", activeListFiltersColumns),
    hasColumns(knex, "permission_history", historyColumns),
    hasColumns(knex, "permission_history", historyRankingColumns),
    hasColumns(knex, "permission_history", historyLatestColumns),
    hasColumns(knex, "permission_history", historyLatestActiveColumns),
    hasColumns(knex, "permissions", liveActiveDidColumns),
    hasColumns(knex, "permission_history", historyByGranteeColumns),
    hasColumns(knex, "permission_history", historyByValidatorColumns),
    hasColumns(knex, "permission_history", historyGranteeSchemaColumns),
    hasColumns(knex, "permission_history", phSchemaPermissionHeightColumns),
    hasColumns(knex, "permission_history", phSchemaEventHeightPermissionColumns),
    hasColumns(knex, "permission_session_history", sessionHistoryRankingColumns),
    hasColumns(knex, "permission_sessions", permSessionsModifiedColumns),
    hasColumns(knex, "permission_session_history", permSessionHistorySessionHeightModifiedColumns),
    hasColumns(knex, "permissions", permissionsCountryColumns),
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
    }

    if (hasLiveGranteeQueryColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIVE_GRANTEE_QUERY_INDEX_NAME}
        ON permissions (grantee, schema_id, type, vp_state, validator_perm_id, modified ASC, id DESC)
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

    if (hasHistoryByGranteeColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_GRANTEE_HEIGHT_PERMISSION_IDX}
        ON permission_history (grantee, height DESC, permission_id, created_at DESC, id DESC)
      `);
    }

    if (hasHistoryByValidatorColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX}
        ON permission_history (validator_perm_id, height DESC, permission_id, created_at DESC, id DESC)
      `);
    }

    if (hasHistoryGranteeSchemaColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PH_GRANTEE_SCHEMA_HEIGHT_CREATED_IDX}
        ON permission_history (grantee, schema_id, height DESC, created_at DESC, id DESC)
      `);
    }

    if (hasPhSchemaPermissionHeightColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX}
        ON permission_history (schema_id, permission_id, height DESC, created_at DESC, id DESC)
      `);
    }

    if (hasPhSchemaEventHeightPermissionColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX}
        ON permission_history (schema_id, event_type, height DESC, permission_id, created_at DESC, id DESC)
      `);
    }

    if (hasSessionHistoryRankingColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PERM_SESSION_HISTORY_RANKING_IDX}
        ON permission_session_history (session_id, height DESC, created_at DESC, id DESC)
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

    if (hasPermissionsCountryColumns) {
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PERMISSIONS_COUNTRY_MODIFIED_ID_IDX}
        ON permissions (country, modified DESC, id DESC)
      `);
    }

    return;
  }

  if (hasLiveColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(liveColumns, LIVE_INDEX_NAME);
    });
  }

  if (hasLiveGranteeQueryColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(liveGranteeQueryColumns, LIVE_GRANTEE_QUERY_INDEX_NAME);
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

  if (hasHistoryByGranteeColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyByGranteeColumns, HISTORY_GRANTEE_HEIGHT_PERMISSION_IDX);
    });
  }

  if (hasHistoryByValidatorColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyByValidatorColumns, HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX);
    });
  }

  if (hasHistoryGranteeSchemaColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(historyGranteeSchemaColumns, PH_GRANTEE_SCHEMA_HEIGHT_CREATED_IDX);
    });
  }

  if (hasPhSchemaPermissionHeightColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(phSchemaPermissionHeightColumns, PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX);
    });
  }

  if (hasPhSchemaEventHeightPermissionColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.index(phSchemaEventHeightPermissionColumns, PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX);
    });
  }

  if (hasSessionHistoryRankingColumns) {
    await knex.schema.table("permission_session_history", (table) => {
      table.index(sessionHistoryRankingColumns, PERM_SESSION_HISTORY_RANKING_IDX);
    });
  }

  if (hasPermSessionsModifiedColumns) {
    await knex.schema.table("permission_sessions", (table) => {
      table.index(permSessionsModifiedColumns, PERM_SESSIONS_MODIFIED_ID_IDX);
    });
  }

  if (hasPermSessionHistorySessionHeightModifiedColumns) {
    await knex.schema.table("permission_session_history", (table) => {
      table.index(permSessionHistorySessionHeightModifiedColumns, PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX);
    });
  }

  if (hasPermissionsCountryColumns) {
    await knex.schema.table("permissions", (table) => {
      table.index(permissionsCountryColumns, PERMISSIONS_COUNTRY_MODIFIED_ID_IDX);
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  const pg = isPostgres(knex);

  const liveColumns = ["did", "schema_id", "type", "modified", "id"];
  const liveGranteeQueryColumns = ["grantee", "schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const listFiltersSortColumns = ["schema_id", "type", "vp_state", "validator_perm_id", "modified", "id"];
  const historyColumns = ["did", "schema_id", "type", "height", "modified", "created_at", "id"];
  const historyRankingColumns = ["permission_id", "height", "created_at", "id"];
  const historyLatestColumns = ["did", "schema_id", "type", "permission_id", "height", "created_at", "id"];
  const liveActiveDidDropColumns = ["did", "schema_id", "type", "modified", "id"];
  const historyByGranteeColumns = ["grantee", "height", "permission_id", "created_at", "id"];
  const historyByValidatorColumns = ["validator_perm_id", "height", "permission_id", "created_at", "id"];
  const historyGranteeSchemaColumns = ["grantee", "schema_id", "height", "created_at", "id"];
  const phSchemaPermissionHeightColumns = ["schema_id", "permission_id", "height", "created_at", "id"];
  const phSchemaEventHeightPermissionColumns = ["schema_id", "event_type", "height", "permission_id", "created_at", "id"];
  const sessionHistoryRankingColumns = ["session_id", "height", "created_at", "id"];
  const permSessionsModifiedColumns = ["modified", "id"];
  const permSessionHistorySessionHeightModifiedColumns = ["session_id", "height", "modified", "created_at", "id"];
  const permissionsCountryColumns = ["country", "modified", "id"];

  if (pg) {
    for (const indexName of [
      LIVE_INDEX_NAME,
      LIVE_GRANTEE_QUERY_INDEX_NAME,
      LIST_FILTERS_SORT_INDEX,
      LIST_FILTERS_SORT_ACTIVE_INDEX,
      HISTORY_LATEST_DID_SCHEMA_TYPE_IDX,
      HISTORY_LATEST_ACTIVE_DID_SCHEMA_TYPE_IDX,
      LIVE_ACTIVE_DID_SCHEMA_TYPE_IDX,
      ...LEGACY_LIVE_INDEX_NAMES,
      ...LEGACY_REDUNDANT_HISTORY_INDEXES,
      HISTORY_INDEX_NAME,
      HISTORY_RANKING_INDEX_NAME,
      HISTORY_GRANTEE_HEIGHT_PERMISSION_IDX,
      HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX,
      PH_GRANTEE_SCHEMA_HEIGHT_CREATED_IDX,
      PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX,
      PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX,
      PERM_SESSION_HISTORY_RANKING_IDX,
      PERM_SESSIONS_MODIFIED_ID_IDX,
      PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX,
      PERMISSIONS_COUNTRY_MODIFIED_ID_IDX,
    ]) {
      await knex.raw(`DROP INDEX IF EXISTS ${indexName}`);
    }
    return;
  }

  const [
    hasLiveColumns,
    hasLiveGranteeQueryColumns,
    hasListFiltersSortColumns,
    hasHistoryColumns,
    hasHistoryRankingColumns,
    hasHistoryLatestColumns,
    hasLiveActiveDidColumns,
    hasHistoryByGranteeColumns,
    hasHistoryByValidatorColumns,
    hasHistoryGranteeSchemaColumns,
    hasPhSchemaPermissionHeightColumns,
    hasPhSchemaEventHeightPermissionColumns,
    hasSessionHistoryRankingColumns,
    hasPermSessionsModifiedColumns,
    hasPermSessionHistorySessionHeightModifiedColumns,
    hasPermissionsCountryColumns,
  ] = await Promise.all([
    hasColumns(knex, "permissions", liveColumns),
    hasColumns(knex, "permissions", liveGranteeQueryColumns),
    hasColumns(knex, "permissions", listFiltersSortColumns),
    hasColumns(knex, "permission_history", historyColumns),
    hasColumns(knex, "permission_history", historyRankingColumns),
    hasColumns(knex, "permission_history", historyLatestColumns),
    hasColumns(knex, "permissions", liveActiveDidDropColumns),
    hasColumns(knex, "permission_history", historyByGranteeColumns),
    hasColumns(knex, "permission_history", historyByValidatorColumns),
    hasColumns(knex, "permission_history", historyGranteeSchemaColumns),
    hasColumns(knex, "permission_history", phSchemaPermissionHeightColumns),
    hasColumns(knex, "permission_history", phSchemaEventHeightPermissionColumns),
    hasColumns(knex, "permission_session_history", sessionHistoryRankingColumns),
    hasColumns(knex, "permission_sessions", permSessionsModifiedColumns),
    hasColumns(knex, "permission_session_history", permSessionHistorySessionHeightModifiedColumns),
    hasColumns(knex, "permissions", permissionsCountryColumns),
  ]);

  if (hasLiveColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(liveColumns, LIVE_INDEX_NAME);
    });
  }

  if (hasLiveGranteeQueryColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(liveGranteeQueryColumns, LIVE_GRANTEE_QUERY_INDEX_NAME);
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

  if (hasHistoryByGranteeColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyByGranteeColumns, HISTORY_GRANTEE_HEIGHT_PERMISSION_IDX);
    });
  }

  if (hasHistoryByValidatorColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyByValidatorColumns, HISTORY_VALIDATOR_HEIGHT_PERMISSION_IDX);
    });
  }

  if (hasHistoryGranteeSchemaColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(historyGranteeSchemaColumns, PH_GRANTEE_SCHEMA_HEIGHT_CREATED_IDX);
    });
  }

  if (hasPhSchemaPermissionHeightColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(phSchemaPermissionHeightColumns, PH_SCHEMA_PERMISSION_HEIGHT_CREATED_ID_IDX);
    });
  }

  if (hasPhSchemaEventHeightPermissionColumns) {
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(phSchemaEventHeightPermissionColumns, PH_SCHEMA_EVENT_HEIGHT_PERMISSION_CREATED_ID_IDX);
    });
  }

  if (hasSessionHistoryRankingColumns) {
    await knex.schema.table("permission_session_history", (table) => {
      table.dropIndex(sessionHistoryRankingColumns, PERM_SESSION_HISTORY_RANKING_IDX);
    });
  }

  if (hasPermSessionsModifiedColumns) {
    await knex.schema.table("permission_sessions", (table) => {
      table.dropIndex(permSessionsModifiedColumns, PERM_SESSIONS_MODIFIED_ID_IDX);
    });
  }

  if (hasPermSessionHistorySessionHeightModifiedColumns) {
    await knex.schema.table("permission_session_history", (table) => {
      table.dropIndex(permSessionHistorySessionHeightModifiedColumns, PERM_SESSION_HISTORY_SESSION_HEIGHT_MODIFIED_IDX);
    });
  }

  if (hasPermissionsCountryColumns) {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(permissionsCountryColumns, PERMISSIONS_COUNTRY_MODIFIED_ID_IDX);
    });
  }
}
