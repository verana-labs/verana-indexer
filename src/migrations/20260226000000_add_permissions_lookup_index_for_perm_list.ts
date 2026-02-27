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

  const [hasLiveColumns, hasLiveGranteeQueryColumns, hasListFiltersSortColumns, hasActiveListFiltersColumns, hasHistoryColumns, hasHistoryRankingColumns, hasHistoryLatestColumns, hasHistoryLatestActiveColumns, hasLiveActiveDidColumns] = await Promise.all([
    hasColumns(knex, "permissions", liveColumns),
    hasColumns(knex, "permissions", liveGranteeQueryColumns),
    hasColumns(knex, "permissions", listFiltersSortColumns),
    hasColumns(knex, "permissions", activeListFiltersColumns),
    hasColumns(knex, "permission_history", historyColumns),
    hasColumns(knex, "permission_history", historyRankingColumns),
    hasColumns(knex, "permission_history", historyLatestColumns),
    hasColumns(knex, "permission_history", historyLatestActiveColumns),
    hasColumns(knex, "permissions", liveActiveDidColumns),
  ]);

  if (pg) {
    if (hasLiveColumns) {
      for (const legacyIndexName of LEGACY_LIVE_INDEX_NAMES) {
        await knex.raw(`DROP INDEX CONCURRENTLY IF EXISTS ${legacyIndexName}`);
      }
      await knex.raw(`DROP INDEX CONCURRENTLY IF EXISTS ${LIVE_INDEX_NAME}`);
      await knex.raw(`
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ${LIVE_INDEX_NAME}
        ON permissions (did, schema_id, type, modified DESC, id DESC)
      `);
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
      HISTORY_INDEX_NAME,
      HISTORY_RANKING_INDEX_NAME,
    ]) {
      await knex.raw(`DROP INDEX IF EXISTS ${indexName}`);
    }
    return;
  }

  const [hasLiveColumns, hasLiveGranteeQueryColumns, hasListFiltersSortColumns, hasHistoryColumns, hasHistoryRankingColumns, hasHistoryLatestColumns, hasLiveActiveDidColumns] = await Promise.all([
    hasColumns(knex, "permissions", liveColumns),
    hasColumns(knex, "permissions", liveGranteeQueryColumns),
    hasColumns(knex, "permissions", listFiltersSortColumns),
    hasColumns(knex, "permission_history", historyColumns),
    hasColumns(knex, "permission_history", historyRankingColumns),
    hasColumns(knex, "permission_history", historyLatestColumns),
    hasColumns(knex, "permissions", liveActiveDidDropColumns),
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
}
