import knex from "./db_connection";
import {
  VeranaTrustRegistryMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaDidMessageTypes,
  VeranaPermissionMessageTypes,
} from "../verana-message-types";

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

export interface RelatedEntityConfig {
  entityType: string;
  historyTable: string;
  idField: string;
  entityIdField: string;
  msgTypePrefixes: string[];
}

export interface ActivityTimelineConfig {
  entityType: string;
  historyTable: string;
  idField: string;
  entityId: string | number;
  msgTypePrefixes: string[];
  relatedEntities?: RelatedEntityConfig[];
}

async function buildHistoryQuery(
  config: {
    entityType: string;
    historyTable: string;
    idField: string;
    entityId: string | number;
    msgTypePrefixes: string[];
    entityIdField?: string;
  },
  options: {
    transactionTimestampOlderThan?: string;
    atBlockHeight?: string;
  }
) {
  try {
    const { entityType, historyTable, idField, entityId, msgTypePrefixes, entityIdField } = config;
    const { transactionTimestampOlderThan, atBlockHeight } = options;

  const prefixes = msgTypePrefixes || [];
  const msgTypeCondition = prefixes.length > 0 
    ? `AND (${prefixes.map((p, idx) => {
        const escapedPrefix = p.replace(/'/g, "''");
        return idx === 0 ? `transaction_message.type LIKE '${escapedPrefix}%'` : `OR transaction_message.type LIKE '${escapedPrefix}%'`;
      }).join(' ')})`
    : '';

  const isNumericId = typeof entityId === 'number' || !Number.isNaN(Number(entityId));

  const quotedTable = `"${historyTable}"`;
  
  let historyQuery = knex(historyTable)
    .select(
      `${historyTable}.*`,
      "transaction.timestamp",
      knex.raw('? as activity_entity_type', [entityType]),
      entityIdField 
        ? knex.raw(`COALESCE(CAST(${historyTable}.${entityIdField} AS TEXT), CAST(${historyTable}.id AS TEXT)) as activity_entity_id`)
        : knex.raw('? as activity_entity_id', [String(entityId)]),
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
      this.on(`${historyTable}.height`, "=", "transaction.height");
    })
    .where(function () {
      this.where(`${historyTable}.${idField}`, entityId);
    });

  if (atBlockHeight) {
    const blockHeight = Number(atBlockHeight);
    if (!Number.isNaN(blockHeight)) {
      historyQuery = historyQuery.where(function() {
        this.whereNotNull(`${historyTable}.height`)
            .andWhere(`${historyTable}.height`, "<=", blockHeight);
      });
    }
  }

  if (transactionTimestampOlderThan) {
    historyQuery = historyQuery.where("transaction.timestamp", "<", transactionTimestampOlderThan);
  }

  return historyQuery;
  } catch (error: any) {
    console.error("Error building history query:", error);
    console.error("Query config:", config);
    throw error;
  }
}

export async function buildActivityTimeline(
  config: ActivityTimelineConfig,
  options: {
    responseMaxSize?: number;
    transactionTimestampOlderThan?: string;
    atBlockHeight?: string;
  } = {}
): Promise<any[]> {
  try {
    const { entityType, historyTable, idField, entityId, msgTypePrefixes, relatedEntities } = config;
    const { responseMaxSize = 64, transactionTimestampOlderThan, atBlockHeight } = options;

  const queries: any[] = [];

  queries.push(
    buildHistoryQuery(
      {
        entityType,
        historyTable,
        idField,
        entityId,
        msgTypePrefixes,
        entityIdField: idField,
      },
      { transactionTimestampOlderThan, atBlockHeight }
    )
  );

  if (relatedEntities && relatedEntities.length > 0) {
    for (const related of relatedEntities) {
      queries.push(
        buildHistoryQuery(
          {
            entityType: related.entityType,
            historyTable: related.historyTable,
            idField: related.idField,
            entityId,
            msgTypePrefixes: related.msgTypePrefixes,
            entityIdField: related.entityIdField,
          },
          { transactionTimestampOlderThan, atBlockHeight }
        )
      );
    }
  }

  const allResults = await Promise.all(queries.map(async (q, index) => {
    try {
      if (process.env.NODE_ENV !== "production") {
        try {
          const querySql = typeof q.toQuery === 'function' ? q.toQuery() : (typeof q.toString === 'function' ? q.toString() : JSON.stringify(q));
          console.log(`[DEBUG] History query ${index} SQL:`, querySql);
        } catch (e) {
          console.log(`[DEBUG] Could not generate query SQL for query ${index}`);
        }
      }
      const result = await q;
      if (process.env.NODE_ENV !== "production") {
        console.log(`[DEBUG] History query ${index} returned ${result?.length || 0} records`);
      }
      return result;
    } catch (err: any) {
      console.error(`Query error for query ${index}:`, err);
      console.error("Error details:", {
        message: err?.message,
        stack: err?.stack,
        code: err?.code,
        sql: err?.sql,
        bindings: err?.bindings,
      });
      console.error("Query config:", config);
      if (process.env.NODE_ENV !== "production") {
        try {
          const querySql = typeof q.toQuery === 'function' ? q.toQuery() : (typeof q.toString === 'function' ? q.toString() : JSON.stringify(q));
          console.error(`[DEBUG] Failed query SQL:`, querySql);
        } catch (e) {
          console.error(`[DEBUG] Could not generate query SQL:`, e);
        }
      }
      return [];
    }
  }));
  
  const allRecords: any[] = [];

  for (const result of allResults) {
    if (Array.isArray(result)) {
      allRecords.push(...result);
    }
  }
  
  allResults.length = 0;

  const sortedRecords = allRecords.sort((a, b) => {
    if (!a.timestamp && !b.timestamp) {
      const heightDiff = (b.height || 0) - (a.height || 0);
      if (heightDiff !== 0) return heightDiff;
      const aCreated = a.created_at ? (a.created_at instanceof Date ? a.created_at.getTime() : new Date(a.created_at).getTime()) : 0;
      const bCreated = b.created_at ? (b.created_at instanceof Date ? b.created_at.getTime() : new Date(b.created_at).getTime()) : 0;
      return bCreated - aCreated;
    }
    if (!a.timestamp) return 1;
    if (!b.timestamp) return -1;
    const timeDiff = new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
    if (timeDiff !== 0) return timeDiff;
    const heightDiff = (b.height || 0) - (a.height || 0);
    if (heightDiff !== 0) return heightDiff;
    const aCreated = a.created_at ? (a.created_at instanceof Date ? a.created_at.getTime() : new Date(a.created_at).getTime()) : 0;
    const bCreated = b.created_at ? (b.created_at instanceof Date ? b.created_at.getTime() : new Date(b.created_at).getTime()) : 0;
    return bCreated - aCreated;
  });

  const limitedRecords = sortedRecords.slice(0, responseMaxSize);
  
  sortedRecords.length = 0;
  allRecords.length = 0;

  return limitedRecords.map((record: any) => {
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

    let activityEntityId: string | number = entityId;
    if (record.activity_entity_id !== undefined && record.activity_entity_id !== null) {
      activityEntityId = record.activity_entity_id;
    } else if (record.gfd_id !== undefined && record.gfd_id !== null) {
      activityEntityId = record.gfd_id;
    } else if (record.gfv_id !== undefined && record.gfv_id !== null) {
      activityEntityId = record.gfv_id;
    }

    const activityEntityType = record.activity_entity_type || entityType;
    const activityEntityIdStr = String(activityEntityId);
    
    const activityItem: any = {
      timestamp: record.timestamp ? new Date(record.timestamp).toISOString() : null,
      block_height: String(record.height || ""),
      entity_type: activityEntityType,
      entity_id: activityEntityIdStr,
      msg: action,
      changes: changes,
    };

    if (record.sender) {
      activityItem.account = record.sender;
    }

    return activityItem;
  });
  } catch (error: any) {
    console.error("Error in buildActivityTimeline:", error);
    console.error("Config:", JSON.stringify(config, null, 2));
    console.error("Options:", JSON.stringify(options, null, 2));
    throw error;
  }
}
