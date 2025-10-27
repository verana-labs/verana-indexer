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

  @Action()
  public async getTRHistory(ctx: Context<{ tr_id: number; active_gf_only?: boolean; preferred_language?: string }>) {
    try {
      const { tr_id: trID, active_gf_only: activeGFOnly, preferred_language: preferredLanguage } = ctx.params;

      const tr = await knex("trust_registry").where("id", trID).first();
      if (!tr) return ApiResponder.error(ctx, "Trust Registry not found", 404);

      const trHistoryRows = await knex("trust_registry_history")
        .where("tr_id", trID)
        .orderBy("id", "asc");

      let versionQuery = knex("governance_framework_version").where("tr_id", trID).orderBy("version", "asc");
      if (activeGFOnly) versionQuery = versionQuery.where("version", tr.active_version);
      const versions = await versionQuery;

      const resultVersions: any[] = [];

      for (const version of versions) {
        const versionHistory = await knex("governance_framework_version_history")
          .where({ tr_id: trID, version: version.version })
          .orderBy("id", "asc");

        let docsQuery = knex("governance_framework_document").where("gfv_id", version.id);
        if (preferredLanguage) docsQuery = docsQuery.where("language", preferredLanguage);
        let docs = await docsQuery.orderBy("created", "asc");

        if (preferredLanguage && docs.length === 0) {
          docs = await knex("governance_framework_document").where("gfv_id", version.id).orderBy("created", "asc");
        }

        const docHistories = await knex("governance_framework_document_history")
          .where("gfv_id", version.id)
          .orderBy("id", "asc");

        const docsWithHistory = docs.map((doc: any) => ({
          id: doc.id,
          gfv_id: doc.gfv_id,
          created: doc.created,
          language: doc.language,
          url: doc.url,
          digest_sri: doc.digest_sri,
          changes: docHistories
            .filter((dh: any) => dh.gfv_id === version.id && dh.url === doc.url)
            .map((dh: any) => ({
              event_type: dh.event_type,
              height: dh.height,
              changes: dh.changes,
              created: dh.created,
            })),
        }));

        resultVersions.push({
          id: version.id,
          tr_id: version.tr_id,
          created: version.created,
          version: version.version,
          active_since: version.active_since,
          changes: version.changes,
          history: versionHistory.map((vh: any) => ({
            event_type: vh.event_type,
            height: vh.height,
            changes: vh.changes,
            created: vh.created,
            active_since: vh.active_since,
          })),
          documents: docsWithHistory,
        });
      }

      const response = {
        id: tr.id,
        did: tr.did,
        controller: tr.controller,
        created: tr.created,
        modified: tr.modified,
        archived: tr.archived,
        deposit: tr.deposit,
        aka: tr.aka,
        language: tr.language,
        active_version: tr.active_version,
        changes: tr.changes,
        history: trHistoryRows.map((h: any) => ({
          event_type: h.event_type,
          height: h.height,
          changes: h.changes,
          created: h.created,
          modified: h.modified,
        })),
        versions: resultVersions,
      };

      return ApiResponder.success(ctx, { trust_registry: response });
    } catch (err: any) {
      this.logger.error("Error fetching TR history", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }
}
