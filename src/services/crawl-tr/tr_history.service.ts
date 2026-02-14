/* eslint-disable @typescript-eslint/no-explicit-any */
import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";

@Service({
  name: SERVICE.V1.TrustRegistryHistoryService.key,
  version: 1,
})
export default class TrustRegistryHistoryService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
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
      .where({ tr_id: trId })
      .where("height", "<=", height)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc");

    const versionMap = new Map<number, any>();
    for (const gfv of gfvHistory) {
      if (!versionMap.has(gfv.version)) {
        versionMap.set(gfv.version, gfv);
      }
    }

    const versions = await Promise.all(
      Array.from(versionMap.values()).map(async (gfv: any) => {
        const gfdHistory = await knex("governance_framework_document_history")
          .where({ gfv_id: gfv.id, tr_id: trId })
          .where("height", "<=", height)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc");

        const docMap = new Map<string, any>();
        for (const gfd of gfdHistory) {
          const key = `${gfd.url || ""}-${gfd.language || ""}`;
          if (!docMap.has(key)) {
            docMap.set(key, gfd);
          }
        }

        const documents = Array.from(docMap.values()).map((gfd: any) => ({
          id: typeof gfd.id === 'number' ? gfd.id : Number(gfd.id),
          gfv_id: typeof gfv.id === 'number' ? gfv.id : Number(gfv.id),
          created: gfd.created ? (gfd.created instanceof Date ? gfd.created.toISOString() : new Date(gfd.created).toISOString()) : null,
          language: gfd.language,
          url: gfd.url,
          digest_sri: gfd.digest_sri,
        }));

        return {
          id: typeof gfv.id === 'number' ? gfv.id : Number(gfv.id),
          tr_id: typeof trId === 'number' ? trId : Number(trId),
          created: gfv.created ? (gfv.created instanceof Date ? gfv.created.toISOString() : new Date(gfv.created).toISOString()) : null,
          version: gfv.version,
          active_since: gfv.active_since ? (gfv.active_since instanceof Date ? gfv.active_since.toISOString() : new Date(gfv.active_since).toISOString()) : null,
          documents,
        };
      })
    );

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

    const gfvHistoryIds = new Set<number>();
    
    if (addedGfvs && addedGfvs.length > 0) {
      addedGfvs.forEach((gfv: any) => {
        const id = parseInt(String(gfv.id), 10);
        if (!Number.isNaN(id)) {
          gfvHistoryIds.add(id);
        }
      });
    }

    if (addedGfds && addedGfds.length > 0) {
      const docIds = addedGfds.map((gfd: any) => parseInt(String(gfd.id), 10)).filter((id: number) => !Number.isNaN(id));
      if (docIds.length > 0) {
        const docHistoryRecords = await knex("governance_framework_document_history")
          .whereIn("id", docIds)
          .where({ tr_id: trId })
          .where("height", height);
        
        docHistoryRecords.forEach((doc: any) => {
          const gfvId = parseInt(String(doc.gfv_id), 10);
          if (!Number.isNaN(gfvId)) {
            gfvHistoryIds.add(gfvId);
          }
        });
      }
    }

    if (gfvHistoryIds.size === 0) {
      return [];
    }

    const versions = await Promise.all(
      Array.from(gfvHistoryIds).map(async (gfvHistoryId: number) => {
        const gfvHistory = await knex("governance_framework_version_history")
          .where({ id: gfvHistoryId, tr_id: trId })
          .where("height", "<=", height)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!gfvHistory) return null;

        const gfdHistory = await knex("governance_framework_document_history")
          .where({ gfv_id: gfvHistoryId, tr_id: trId })
          .where("height", "<=", height)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc");

        const changedDocs = gfdHistory.filter((gfd: any) => {
          const gfdHeight = typeof gfd.height === "string" ? parseInt(gfd.height, 10) : gfd.height;
          return gfdHeight === height;
        });

        const versionAddedAtHeight = gfvHistory.height === height;
        const docsToInclude = versionAddedAtHeight ? gfdHistory : changedDocs;

        const docMap = new Map<string, any>();
        for (const gfd of docsToInclude) {
          const key = `${gfd.url || ""}-${gfd.language || ""}`;
          if (!docMap.has(key)) {
            docMap.set(key, gfd);
          }
        }

        const documents = Array.from(docMap.values()).map((gfd: any) => ({
          id: typeof gfd.id === 'number' ? gfd.id : Number(gfd.id),
          gfv_id: typeof gfvHistoryId === 'number' ? gfvHistoryId : Number(gfvHistoryId),
          created: gfd.created ? (gfd.created instanceof Date ? gfd.created.toISOString() : new Date(gfd.created).toISOString()) : null,
          language: gfd.language,
          url: gfd.url,
          digest_sri: gfd.digest_sri,
        }));

        return {
          id: typeof gfvHistoryId === 'number' ? gfvHistoryId : Number(gfvHistoryId),
          tr_id: typeof trId === 'number' ? trId : Number(trId),
          created: gfvHistory.created ? (gfvHistory.created instanceof Date ? gfvHistory.created.toISOString() : new Date(gfvHistory.created).toISOString()) : null,
          version: gfvHistory.version,
          active_since: gfvHistory.active_since ? (gfvHistory.active_since instanceof Date ? gfvHistory.active_since.toISOString() : new Date(gfvHistory.active_since).toISOString()) : null,
          documents,
        };
      })
    );

    return versions.filter((v: any) => v !== null).sort((a: any, b: any) => (a?.version || 0) - (b?.version || 0));
  }

  @Action()
  public async getTRHistory(ctx: Context<{ tr_id: number; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { tr_id: trId, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;
      
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

      const tr = await knex("trust_registry").where("id", trId).first();
      if (!tr) return ApiResponder.error(ctx, "Trust Registry not found", 404);

      const { buildActivityTimeline } = await import("../../common/utils/activity_timeline_helper");
      const activity = await buildActivityTimeline(
        {
          entityType: "TrustRegistry",
          historyTable: "trust_registry_history",
          idField: "tr_id",
          entityId: trId,
          msgTypePrefixes: ["/verana.tr.v1", "/veranablockchain.trustregistry"],
          relatedEntities: [
            {
              entityType: "GovernanceFrameworkVersion",
              historyTable: "governance_framework_version_history",
              idField: "tr_id",
              entityIdField: "id",
              msgTypePrefixes: ["/verana.tr.v1", "/veranablockchain.trustregistry"],
            },
            {
              entityType: "GovernanceFrameworkDocument",
              historyTable: "governance_framework_document_history",
              idField: "tr_id",
              entityIdField: "id",
              msgTypePrefixes: ["/verana.tr.v1", "/veranablockchain.trustregistry"],
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
