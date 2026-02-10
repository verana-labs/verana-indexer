import knex from "../../common/utils/db_connection";
import { calculateCredentialSchemaStats } from "../crawl-cs/cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";
import { getBlockHeight } from "../../common/utils/blockHeight";
import { calculatePermState } from "../crawl-perm/perm_state_utils";

export async function computeGlobalMetrics(blockHeight?: number) {
  const useHistory = typeof blockHeight === "number";

  if (!useHistory) {
    const trCounts = await knex("trust_registry")
      .select(
        knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_trust_registries"),
        knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_trust_registries")
      )
      .first();
    let activeTrustRegistries = Number(trCounts?.active_trust_registries || 0);
    let archivedTrustRegistries = Number(trCounts?.archived_trust_registries || 0);

    if (activeTrustRegistries + archivedTrustRegistries === 0) {
      const trHistoryCount = await knex("trust_registry_history").count("* as count").first();
      const hasHistory = Number((trHistoryCount as any)?.count || 0) > 0;
      if (hasHistory) {
        const trSub = knex("trust_registry_history")
          .select("tr_id", "archived")
          .select(
            knex.raw(
              "ROW_NUMBER() OVER (PARTITION BY tr_id ORDER BY height DESC, created_at DESC) as rn"
            )
          )
          .as("ranked_tr");
        const latest = await knex.from(trSub).select("archived").where("rn", 1);
        activeTrustRegistries = latest.filter((r: any) => !r.archived).length;
        archivedTrustRegistries = latest.filter((r: any) => r.archived).length;
      }
    }

    const csAgg = await knex("credential_schemas")
      .select(
        knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_schemas"),
        knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_schemas"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(weight,'') AS numeric)), 0) as total_weight"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(issued,'0') AS numeric)), 0) as issued_sum"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(verified,'0') AS numeric)), 0) as verified_sum"),
        knex.raw("COALESCE(SUM(COALESCE(ecosystem_slash_events,0)), 0) as ecosystem_slash_events_sum"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(ecosystem_slashed_amount,'') AS numeric)), 0) as ecosystem_slashed_amount_sum"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(ecosystem_slashed_amount_repaid,'') AS numeric)), 0) as ecosystem_slashed_amount_repaid_sum"),
        knex.raw("COALESCE(SUM(COALESCE(network_slash_events,0)), 0) as network_slash_events_sum"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(network_slashed_amount,'') AS numeric)), 0) as network_slashed_amount_sum"),
        knex.raw("COALESCE(SUM(CAST(NULLIF(network_slashed_amount_repaid,'') AS numeric)), 0) as network_slashed_amount_repaid_sum")
      )
      .first();

    const nowIso = new Date().toISOString();
    const activeParticipantsRow: any = await knex("permissions")
      .whereNull("revoked")
      .andWhere(function () {
        this.whereNull("slashed").orWhereNotNull("repaid");
      })
      .andWhere(function () {
        this.whereNull("effective_from").orWhere("effective_from", "<=", nowIso);
      })
      .andWhere(function () {
        this.whereNull("effective_until").orWhere("effective_until", ">", nowIso);
      })
      .andWhere(function () {
        this.where("vp_state", "VALIDATED").orWhere("type", "ECOSYSTEM");
      })
      .countDistinct("grantee as count")
      .first();

    const participants = Number((activeParticipantsRow && (activeParticipantsRow.count || activeParticipantsRow.count_distinct)) || 0);

    return {
      participants,
      active_trust_registries: activeTrustRegistries,
      archived_trust_registries: archivedTrustRegistries,
      active_schemas: Number(csAgg.active_schemas || 0),
      archived_schemas: Number(csAgg.archived_schemas || 0),
      weight: (csAgg.total_weight || 0).toString(),
      issued: Number(csAgg.issued_sum || 0),
      verified: Number(csAgg.verified_sum || 0),
      ecosystem_slash_events: Number(csAgg.ecosystem_slash_events_sum || 0),
      ecosystem_slashed_amount: (csAgg.ecosystem_slashed_amount_sum || 0).toString(),
      ecosystem_slashed_amount_repaid: (csAgg.ecosystem_slashed_amount_repaid_sum || 0).toString(),
      network_slash_events: Number(csAgg.network_slash_events_sum || 0),
      network_slashed_amount: (csAgg.network_slashed_amount_sum || 0).toString(),
      network_slashed_amount_repaid: (csAgg.network_slashed_amount_repaid_sum || 0).toString(),
    };
  }

  const trSub = knex("trust_registry_history")
    .select("tr_id")
    .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY tr_id ORDER BY height DESC, created_at DESC) as rn"))
    .where("height", "<=", blockHeight)
    .as("ranked_tr");

  const trLatest = await knex.from(trSub).select("tr_id").where("rn", 1);
  const trIds = trLatest.map((r: any) => Number(r.tr_id));
  let activeTrustRegistries = 0;
  let archivedTrustRegistries = 0;
  for (const trId of trIds) {
    const trHistory = await knex("trust_registry_history")
      .where("tr_id", trId)
      .where("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .first();
    if (trHistory) {
      if (trHistory.archived) archivedTrustRegistries++;
      else activeTrustRegistries++;
    }
  }

  const csSub = knex("credential_schema_history")
    .select("credential_schema_id")
    .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn"))
    .where("height", "<=", blockHeight)
    .as("ranked_cs");

  const csLatest = await knex.from(csSub).select("credential_schema_id").where("rn", 1);
  const schemaIds = csLatest.map((r: any) => Number(r.credential_schema_id));

  let activeSchemas = 0;
  let archivedSchemas = 0;
  let totalWeight = BigInt(0);
  let issued = 0;
  let verified = 0;
  let ecosystemSlashEvents = 0;
  let ecosystemSlashedAmount = BigInt(0);
  let ecosystemSlashedAmountRepaid = BigInt(0);
  let networkSlashEvents = 0;
  let networkSlashedAmount = BigInt(0);
  let networkSlashedAmountRepaid = BigInt(0);

  for (const sid of schemaIds) {
    const schHistory = await knex("credential_schema_history")
      .where("credential_schema_id", sid)
      .where("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .first();
    if (!schHistory) continue;
    if (schHistory.archived) archivedSchemas++;
    else activeSchemas++;

    try {
      const stats = await calculateCredentialSchemaStats(sid, blockHeight);
      try {
        totalWeight += BigInt(stats.weight || "0");
      } catch {}
      issued += Number(stats.issued || 0);
      verified += Number(stats.verified || 0);
      ecosystemSlashEvents += Number(stats.ecosystem_slash_events || 0);
      ecosystemSlashedAmount += BigInt(stats.ecosystem_slashed_amount || "0");
      ecosystemSlashedAmountRepaid += BigInt(stats.ecosystem_slashed_amount_repaid || "0");
      networkSlashEvents += Number(stats.network_slash_events || 0);
      networkSlashedAmount += BigInt(stats.network_slashed_amount || "0");
      networkSlashedAmountRepaid += BigInt(stats.network_slashed_amount_repaid || "0");
    } catch (err: any) {
      console.warn(`Failed to calculate stats for schema ${sid} at height ${blockHeight}: ${err?.message || err}`);
    }
  }

  const participantsSet = new Set<string>();
  const latestHistorySubquery = knex("permission_history")
    .select("permission_id")
    .select(
      knex.raw(
        `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
      )
    )
    .where("height", "<=", blockHeight)
    .as("ranked");

  const permIdsAtHeight = await knex
    .from(latestHistorySubquery)
    .select("permission_id")
    .where("rn", 1)
    .then((rows: any[]) => rows.map((r: any) => String(r.permission_id)));

  for (const permId of permIdsAtHeight) {
    const historyRecord = await knex("permission_history")
      .where({ permission_id: String(permId) })
      .where("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .first();
    if (!historyRecord) continue;
    const permState = calculatePermState(
      {
        repaid: historyRecord.repaid,
        slashed: historyRecord.slashed,
        revoked: historyRecord.revoked,
        effective_from: historyRecord.effective_from,
        effective_until: historyRecord.effective_until,
        type: historyRecord.type,
        vp_state: historyRecord.vp_state,
        vp_exp: historyRecord.vp_exp,
        validator_perm_id: historyRecord.validator_perm_id,
      },
      new Date()
    );
    if (permState === "ACTIVE" && historyRecord.grantee) {
      participantsSet.add(historyRecord.grantee);
    }
  }

  return {
    participants: participantsSet.size,
    active_trust_registries: activeTrustRegistries,
    archived_trust_registries: archivedTrustRegistries,
    active_schemas: activeSchemas,
    archived_schemas: archivedSchemas,
    weight: totalWeight.toString(),
    issued,
    verified,
    ecosystem_slash_events: ecosystemSlashEvents,
    ecosystem_slashed_amount: ecosystemSlashedAmount.toString(),
    ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid.toString(),
    network_slash_events: networkSlashEvents,
    network_slashed_amount: networkSlashedAmount.toString(),
    network_slashed_amount_repaid: networkSlashedAmountRepaid.toString(),
  };
}
