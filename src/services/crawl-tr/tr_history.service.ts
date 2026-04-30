/* eslint-disable @typescript-eslint/no-explicit-any */
import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { isValidISO8601UTC } from "../../common/utils/date_utils";
import { buildActivityTimeline } from "../../common/utils/activity_timeline_helper";

@Service({
  name: SERVICE.V1.TrustRegistryHistoryService.key,
  version: 1,
})
export default class TrustRegistryHistoryService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private async hasHistoryColumn(tableName: string, columnName: string): Promise<boolean> {
    try {
      const info = await knex(tableName).columnInfo();
      return Object.prototype.hasOwnProperty.call(info, columnName);
    } catch (_err) {
      return false;
    }
  }

  private async buildVersionsAtHeight(trId: number, blockHeight: number | string | null): Promise<any[]> {
    if (!blockHeight) {
      return [];
    }

    const height = typeof blockHeight === "string" ? parseInt(blockHeight, 10) : blockHeight;
    if (Number.isNaN(height)) {
      return [];
    }

    const gfvHistory = await knex("governance_framework_version_history")
      .select("id", "tr_id", "created", "version", "active_since", "height", "created_at")
      .where({ tr_id: trId })
      .where("height", "<=", height)
      .orderBy("version", "asc")
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .orderBy("id", "desc");

    const gfvRows = await knex("governance_framework_version")
      .select("id", "tr_id", "version")
      .where({ tr_id: trId });
    const gfvIdByVersion = new Map<number, number>();
    for (const row of gfvRows) {
      const version = Number(row.version);
      const id = Number(row.id);
      if (Number.isFinite(version) && version > 0 && Number.isFinite(id) && id > 0) {
        gfvIdByVersion.set(version, id);
      }
    }

    const versionMap = new Map<number, any>();
    for (const gfv of gfvHistory) {
      if (!versionMap.has(gfv.version)) {
        versionMap.set(gfv.version, gfv);
      }
    }

    const versions = Array.from(versionMap.values()).map((gfv: any) => {
      const version = Number(gfv.version);
      const actualGfvId = gfvIdByVersion.get(version);
      return {
        id: Number.isFinite(actualGfvId as number) ? Number(actualGfvId) : Number(gfv.id),
        tr_id: Number(trId),
        created: gfv.created ? (gfv.created instanceof Date ? gfv.created.toISOString() : new Date(gfv.created).toISOString()) : null,
        version,
        active_since: gfv.active_since ? (gfv.active_since instanceof Date ? gfv.active_since.toISOString() : new Date(gfv.active_since).toISOString()) : null,
        documents: [] as any[],
      };
    });

    const gfvIds = Array.from(new Set(versions.map((v: any) => Number(v.id)).filter((id: number) => Number.isFinite(id) && id > 0)));
    if (gfvIds.length > 0) {
      const hasGfdIdColumn = await this.hasHistoryColumn("governance_framework_document_history", "gfd_id");
      const gfdColumns = [
        "id",
        "gfv_id",
        "tr_id",
        "created",
        "language",
        "url",
        "digest_sri",
        "height",
        "created_at",
      ];
      if (hasGfdIdColumn) {
        gfdColumns.splice(1, 0, "gfd_id");
      }

      const gfdHistory = await knex("governance_framework_document_history")
        .select(...gfdColumns)
        .where({ tr_id: trId })
        .whereIn("gfv_id", gfvIds)
        .where("height", "<=", height)
        .orderBy("gfv_id", "asc")
        .orderBy("height", "desc")
        .orderBy("created_at", "desc")
        .orderBy("id", "desc");

      const entryByGfvId = new Map<number, any>();
      for (const v of versions) {
        entryByGfvId.set(Number(v.id), v);
      }

      const seenDocs = new Set<string>();
      for (const gfd of gfdHistory as any[]) {
        const gfvId = Number(gfd.gfv_id);
        const versionEntry = entryByGfvId.get(gfvId);
        if (!versionEntry) continue;

        const docId = Number(hasGfdIdColumn ? (gfd as any).gfd_id : gfd.id);
        const dedupeId = Number.isFinite(docId) && docId > 0 ? `doc:${docId}` : `url:${gfd.url || ""}:${gfd.language || ""}`;
        const dedupeKey = `${Number(versionEntry.tr_id)}::${gfvId}::${dedupeId}`;
        if (seenDocs.has(dedupeKey)) continue;
        seenDocs.add(dedupeKey);

        versionEntry.documents.push({
          id: Number.isFinite(docId) && docId > 0 ? docId : Number(gfd.id),
          gfv_id: gfvId,
          created: gfd.created ? (gfd.created instanceof Date ? gfd.created.toISOString() : new Date(gfd.created).toISOString()) : null,
          language: gfd.language,
          url: gfd.url,
          digest_sri: gfd.digest_sri,
        });
      }
    }

    return versions.sort((a, b) => a.version - b.version);
  }

  private async buildChangedVersions(
    trId: number,
    blockHeight: number | string | null,
    addedGfvs: any[],
    addedGfds: any[]
  ): Promise<any[]> {
    if ((!addedGfvs || addedGfvs.length === 0) && (!addedGfds || addedGfds.length === 0)) {
      return [];
    }

    const height = typeof blockHeight === "string" ? parseInt(blockHeight, 10) : blockHeight;
    if (!height || Number.isNaN(height)) {
      return [];
    }

    const targetVersions = new Set<number>();
    
    if (addedGfvs && addedGfvs.length > 0) {
      const gfvHistoryIds = addedGfvs
        .map((gfv: any) => parseInt(String(gfv.id), 10))
        .filter((id: number) => !Number.isNaN(id));

      if (gfvHistoryIds.length > 0) {
        const gfvHistoryRows = await knex("governance_framework_version_history")
          .select("id", "version")
          .whereIn("id", gfvHistoryIds)
          .where({ tr_id: trId })
          .where("height", "<=", height)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .orderBy("id", "desc");

        for (const row of gfvHistoryRows) {
          const version = Number(row.version);
          if (Number.isFinite(version) && version > 0) {
            targetVersions.add(version);
          }
        }
      }
    }

    if (addedGfds && addedGfds.length > 0) {
      const docIds = addedGfds.map((gfd: any) => parseInt(String(gfd.id), 10)).filter((id: number) => !Number.isNaN(id));
      if (docIds.length > 0) {
        const docHistoryRecords = await knex("governance_framework_document_history")
          .whereIn("id", docIds)
          .where({ tr_id: trId })
          .where("height", height);

        const gfvIds = Array.from(
          new Set(
            docHistoryRecords
              .map((doc: any) => parseInt(String(doc.gfv_id), 10))
              .filter((id: number) => !Number.isNaN(id))
          )
        );

        if (gfvIds.length > 0) {
          const gfvRows = await knex("governance_framework_version")
            .select("id", "version")
            .where({ tr_id: trId })
            .whereIn("id", gfvIds);

          for (const row of gfvRows) {
            const version = Number(row.version);
            if (Number.isFinite(version) && version > 0) {
              targetVersions.add(version);
            }
          }
        }
      }
    }

    if (targetVersions.size === 0) {
      return [];
    }

    const versionsAtHeight = await this.buildVersionsAtHeight(trId, height);
    if (!versionsAtHeight || versionsAtHeight.length === 0) {
      return [];
    }

    return versionsAtHeight
      .filter((v: any) => targetVersions.has(Number(v.version)))
      .sort((a: any, b: any) => (a?.version || 0) - (b?.version || 0));
  }

  private async getTRHistoryFromSnapshot(
    trId: number,
    responseMaxSize: number,
    transactionTimestampOlderThan?: string,
    atBlockHeight?: string
  ): Promise<any[]> {
    let query = knex("trust_registry_snapshot")
      .where({ tr_id: trId })
      .orderBy("height", "desc")
      .limit(responseMaxSize);

    if (atBlockHeight) {
      const h = parseInt(String(atBlockHeight), 10);
      if (!Number.isNaN(h)) query = query.where("height", "<=", h);
    }

    if (transactionTimestampOlderThan) {
      const ts = new Date(transactionTimestampOlderThan);
      if (!Number.isNaN(ts.getTime())) {
        query = query.where("created_at", "<", ts);
      }
    }

    const rows = await query.select(
      "id", "tr_id", "height", "event_type", "created_at",
      "did", "corporation", "created", "modified", "archived", "aka", "language", "active_version",
      "participants", "participants_ecosystem", "participants_issuer_grantor", "participants_issuer",
      "participants_verifier_grantor", "participants_verifier", "participants_holder",
      "active_schemas", "archived_schemas", "weight", "issued", "verified",
      "ecosystem_slash_events", "ecosystem_slashed_amount", "ecosystem_slashed_amount_repaid",
      "network_slash_events", "network_slashed_amount", "network_slashed_amount_repaid",
      "versions_snapshot"
    );

    const byKey = new Map<string, any>();
    for (const row of rows || []) {
      const key = `${row.height}::${row.event_type}`;
      const existing = byKey.get(key);
      if (!existing) {
        byKey.set(key, row);
      } else {
        const existingTs = existing.created_at ? new Date(existing.created_at).getTime() : 0;
        const currentTs = row.created_at ? new Date(row.created_at).getTime() : 0;
        if (currentTs > existingTs || (!existingTs && Number(row.id) > Number(existing.id))) {
          byKey.set(key, row);
        }
      }
    }
    const deduped = Array.from(byKey.values()).sort((a, b) => {
      if (Number(b.height) !== Number(a.height)) return Number(b.height) - Number(a.height);
      const at = a.created_at ? new Date(a.created_at).getTime() : 0;
      const bt = b.created_at ? new Date(b.created_at).getTime() : 0;
      return bt - at;
    });

    return deduped.map((row: any) => {
      const msg = row.event_type === "Create" ? "CreateTrustRegistry" : row.event_type === "Archive" ? "ArchiveTrustRegistry" : row.event_type;
      let versions = Array.isArray(row.versions_snapshot) ? row.versions_snapshot : [];
      versions = versions.map((v: any) => ({
        id: v.id ?? v.version_id,
        tr_id: Number(trId),
        created: v.created != null ? (v.created instanceof Date ? v.created.toISOString() : new Date(v.created).toISOString()) : null,
        version: Number(v.version ?? 0),
        active_since: v.active_since != null ? (v.active_since instanceof Date ? v.active_since.toISOString() : new Date(v.active_since).toISOString()) : null,
        documents: (Array.isArray(v.documents) ? v.documents : []).map((d: any) => ({
          id: d.id,
          url: d.url,
          created: d.created != null ? (d.created instanceof Date ? d.created.toISOString() : new Date(d.created).toISOString()) : null,
          language: d.language ?? null,
          digest_sri: d.digest_sri ?? null,
          version_id: d.version_id ?? v.id ?? v.version_id,
        })),
      })).filter((v: any) => v.version > 0);

      const changes: Record<string, unknown> = {
        did: row.did,
        corporation: row.corporation,
        created: row.created != null ? (row.created instanceof Date ? row.created.toISOString() : new Date(row.created).toISOString()) : null,
        modified: row.modified != null ? (row.modified instanceof Date ? row.modified.toISOString() : new Date(row.modified).toISOString()) : null,
        archived: row.archived != null ? (row.archived instanceof Date ? row.archived.toISOString() : new Date(row.archived).toISOString()) : null,
        aka: row.aka ?? null,
        language: row.language ?? null,
        active_version: row.active_version ?? null,
        participants: Number(row.participants ?? 0),
        participants_ecosystem: Number(row.participants_ecosystem ?? 0),
        participants_issuer_grantor: Number(row.participants_issuer_grantor ?? 0),
        participants_issuer: Number(row.participants_issuer ?? 0),
        participants_verifier_grantor: Number(row.participants_verifier_grantor ?? 0),
        participants_verifier: Number(row.participants_verifier ?? 0),
        participants_holder: Number(row.participants_holder ?? 0),
        active_schemas: Number(row.active_schemas ?? 0),
        archived_schemas: Number(row.archived_schemas ?? 0),
        weight: Number(row.weight ?? 0),
        issued: Number(row.issued ?? 0),
        verified: Number(row.verified ?? 0),
        ecosystem_slash_events: Number(row.ecosystem_slash_events ?? 0),
        ecosystem_slashed_amount: Number(row.ecosystem_slashed_amount ?? 0),
        ecosystem_slashed_amount_repaid: Number(row.ecosystem_slashed_amount_repaid ?? 0),
        network_slash_events: Number(row.network_slash_events ?? 0),
        network_slashed_amount: Number(row.network_slashed_amount ?? 0),
        network_slashed_amount_repaid: Number(row.network_slashed_amount_repaid ?? 0),
      };
      if (versions.length > 0) {
        changes.versions = versions;
      }
      return {
        block_height: String(row.height),
        msg,
        changes,
        created_at: row.created_at,
      };
    });
  }

  @Action()
  public async getTRHistory(ctx: Context<{ tr_id: number; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { tr_id: trId, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;

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
      const useHeightSync = process.env.USE_HEIGHT_SYNC_TR === "true";

      const tr = await knex("trust_registry").where("id", trId).first();
      if (!tr) return ApiResponder.error(ctx, "Trust Registry not found", 404);

      if (useHeightSync) {
        const activity = await this.getTRHistoryFromSnapshot(
          trId,
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight
        );
        return ApiResponder.success(ctx, {
          entity_type: "TrustRegistry",
          entity_id: String(trId),
          activity,
        }, 200);
      }

      const activity = await buildActivityTimeline(
        {
          entityType: "TrustRegistry",
          historyTable: "trust_registry_history",
          idField: "tr_id",
          entityId: trId,
          msgTypePrefixes: ["/verana.tr.v1"],
          relatedEntities: [
            {
              entityType: "GovernanceFrameworkVersion",
              historyTable: "governance_framework_version_history",
              idField: "tr_id",
              entityIdField: "id",
              msgTypePrefixes: ["/verana.tr.v1"],
            },
            {
              entityType: "GovernanceFrameworkDocument",
              historyTable: "governance_framework_document_history",
              idField: "tr_id",
              entityIdField: "id",
              msgTypePrefixes: ["/verana.tr.v1"],
            },
          ],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      );

      const enrichedActivity = await Promise.all(
        (activity || []).map(async (item: any) => {
          const blockHeight = item.block_height ? parseInt(item.block_height, 10) : null;

          const originalChanges = item.changes || {};
          const addedGfvs = originalChanges.added_governance_framework_versions;
          const addedGfds = originalChanges.added_governance_framework_documents;

          const cleanedChanges = { ...originalChanges };
          delete cleanedChanges.added_governance_framework_versions;
          delete cleanedChanges.added_governance_framework_documents;

          if ((addedGfvs && addedGfvs.length > 0) || (addedGfds && addedGfds.length > 0) || item.msg === "CreateTrustRegistry") {
            if (blockHeight && !Number.isNaN(blockHeight)) {
              let versionsToAdd: any[] = [];

              if (item.msg === "CreateTrustRegistry") {
                versionsToAdd = await this.buildVersionsAtHeight(trId, blockHeight);
              } else if ((addedGfvs && addedGfvs.length > 0) || (addedGfds && addedGfds.length > 0)) {
                versionsToAdd = await this.buildChangedVersions(
                  trId,
                  blockHeight,
                  addedGfvs || [],
                  addedGfds || []
                );
              }

              if (versionsToAdd && versionsToAdd.length > 0) {
                cleanedChanges.versions = versionsToAdd;
              }
            }
          }

          const { versions: unusedVersions, ...itemWithoutVersions } = item;

          return {
            ...itemWithoutVersions,
            changes: cleanedChanges,
          };
        })
      );

      const result = {
        entity_type: "TrustRegistry",
        entity_id: String(trId),
        activity: enrichedActivity || [],
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error fetching TR history:", err);
      this.logger.error("Error stack:", err?.stack);
      this.logger.error("Error details:", {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      });
      return ApiResponder.error(ctx, `Failed to get Trust Registry history: ${err?.message || "Unknown error"}`, 500);
    }
  }
}
