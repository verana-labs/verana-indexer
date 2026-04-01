import type { Knex } from "knex";
import knex from "../../common/utils/db_connection";
import {
  calculateCredentialSchemaStats,
  calculateCredentialSchemaStatsBatch,
} from "../crawl-cs/cs_stats";
import { calculatePermState } from "../crawl-perm/perm_state_utils";
import { getBlockChainTimeAsOf } from "../../common/utils/block_time";

function isMetricsPgClient(db: Knex): boolean {
  return String((db as any)?.client?.config?.client || "").includes("pg");
}

async function aggregateLiveTrustWeightFromPermissions(db: Knex): Promise<{
  totalWeight: number;
  maxSinglePermWeight: number;
}> {
  const sumExpr = isMetricsPgClient(db)
    ? `COALESCE(SUM(
         CASE
           WHEN p.weight IS NOT NULL THEN p.weight::numeric
           ELSE COALESCE(p.deposit::numeric, 0)
         END
       ), 0)`
    : `COALESCE(SUM(
         CASE
           WHEN p.weight IS NOT NULL THEN CAST(p.weight AS REAL)
           ELSE COALESCE(CAST(p.deposit AS REAL), 0)
         END
       ), 0)`;

  const maxExpr = isMetricsPgClient(db)
    ? `COALESCE(MAX(
         CASE
           WHEN p.weight IS NOT NULL THEN p.weight::numeric
           ELSE COALESCE(p.deposit::numeric, 0)
         END
       ), 0)`
    : `COALESCE(MAX(
         CASE
           WHEN p.weight IS NOT NULL THEN CAST(p.weight AS REAL)
           ELSE COALESCE(CAST(p.deposit AS REAL), 0)
         END
       ), 0)`;

  const row = await db("permissions as p")
    .join("credential_schemas as cs", "cs.id", "p.schema_id")
    .select(db.raw(`${sumExpr} as total_weight`), db.raw(`${maxExpr} as max_single`))
    .first();

  const totalWeight = parseMetricsNumeric((row as any)?.total_weight);
  const maxSinglePermWeight = parseMetricsNumeric((row as any)?.max_single);
  return { totalWeight, maxSinglePermWeight };
}

async function sumDenormalizedCredentialSchemaWeights(db: Knex): Promise<number> {
  const row = await db("credential_schemas")
    .select(db.raw("COALESCE(SUM(CAST(weight AS NUMERIC)), 0) as s"))
    .first();
  return parseMetricsNumeric((row as any)?.s);
}

function parseMetricsNumeric(v: unknown): number {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const n = Number(String(v).trim());
  return Number.isFinite(n) ? n : 0;
}

async function aggregateLiveTrustDepositWeight(db: Knex): Promise<{
  totalWeight: number;
  maxSingleDeposit: number;
}> {
  const castTotalExpr = isMetricsPgClient(db)
    ? `COALESCE(SUM(amount::numeric), 0)`
    : `COALESCE(SUM(CAST(amount AS REAL)), 0)`;

  const castMaxExpr = isMetricsPgClient(db)
    ? `COALESCE(MAX(amount::numeric), 0)`
    : `COALESCE(MAX(CAST(amount AS REAL)), 0)`;

  const row = await db("trust_deposits")
    .select(db.raw(`${castTotalExpr} as total_weight`), db.raw(`${castMaxExpr} as max_single`))
    .first();

  const totalWeight = parseMetricsNumeric((row as any)?.total_weight);
  const maxSingleDeposit = parseMetricsNumeric((row as any)?.max_single);
  return { totalWeight, maxSingleDeposit };
}

async function aggregateHistoricalTrustDepositWeight(
  db: Knex,
  blockHeight: number
): Promise<{
  totalWeight: number;
  maxSingleDeposit: number;
}> {
  if (isMetricsPgClient(db)) {
    const tdLatest = db("trust_deposit_history as tdh")
      .distinctOn("tdh.account")
      .select("tdh.account", "tdh.amount")
      .where("tdh.height", "<=", blockHeight)
      .orderBy("tdh.account", "asc")
      .orderBy("tdh.height", "desc")
      .orderBy("tdh.created_at", "desc")
      .orderBy("tdh.id", "desc")
      .as("latest_td");

    const row = await db.from(tdLatest)
      .select(
        db.raw(`COALESCE(SUM(latest_td.amount::numeric), 0) as total_weight`),
        db.raw(`COALESCE(MAX(latest_td.amount::numeric), 0) as max_single`)
      )
      .first();

    return {
      totalWeight: parseMetricsNumeric((row as any)?.total_weight),
      maxSingleDeposit: parseMetricsNumeric((row as any)?.max_single),
    };
  }

  const tdSub = db("trust_deposit_history as tdh")
    .select("tdh.account", "tdh.amount")
    .select(db.raw("ROW_NUMBER() OVER (PARTITION BY tdh.account ORDER BY tdh.height DESC, tdh.created_at DESC, tdh.id DESC) as rn"))
    .where("tdh.height", "<=", blockHeight)
    .as("ranked_td");

  const tdLatest = db.from(tdSub).select("account", "amount").where("rn", 1).as("latest_td");

  const row = await db.from(tdLatest)
    .select(
      db.raw(`COALESCE(SUM(CAST(latest_td.amount AS REAL)), 0) as total_weight`),
      db.raw(`COALESCE(MAX(CAST(latest_td.amount AS REAL)), 0) as max_single`)
    )
    .first();

  return {
    totalWeight: parseMetricsNumeric((row as any)?.total_weight),
    maxSingleDeposit: parseMetricsNumeric((row as any)?.max_single),
  };
}

export async function computeTotalLockedTrustDepositWeight(blockHeight?: number): Promise<{
  weight: number;
  maxSingleDeposit: number;
}> {
  const db = knex;
  const result =
    typeof blockHeight === "number"
      ? await aggregateHistoricalTrustDepositWeight(db, blockHeight)
      : await aggregateLiveTrustDepositWeight(db);

  if (result.maxSingleDeposit > result.totalWeight + 1e-9) {
    (global as any)?.logger?.warn?.(
      "[metrics] Invariant failed: totalWeight < maxSingleDeposit",
      result
    );
  }

  return { weight: result.totalWeight, maxSingleDeposit: result.maxSingleDeposit };
}

function logGlobalWeightSanity(opts: {
  totalWeightFromTrustDeposits: number;
  maxSingleDeposit: number;
  permissionsDerivedTotalWeight?: number;
  denormalizedCsSum?: number;
  logger?: { warn?: (...args: unknown[]) => void; debug?: (...args: unknown[]) => void };
}): void {
  const { totalWeightFromTrustDeposits, maxSingleDeposit, permissionsDerivedTotalWeight, denormalizedCsSum, logger } = opts;
  const log = logger?.warn ?? logger?.debug ?? ((...args: unknown[]) => console.warn(...args));

  if (maxSingleDeposit > totalWeightFromTrustDeposits + 1e-6) {
    log(
      "[metrics] Invariant failed: max single deposit exceeds global total (aggregation bug?)",
      { totalWeightFromTrustDeposits, maxSingleDeposit }
    );
  }

  if (
    typeof denormalizedCsSum === "number" &&
    Number.isFinite(denormalizedCsSum) &&
    denormalizedCsSum > 0 &&
    totalWeightFromTrustDeposits > 0
  ) {
    const relDiff = Math.abs(totalWeightFromTrustDeposits - denormalizedCsSum) / Math.max(totalWeightFromTrustDeposits, denormalizedCsSum);
    if (relDiff > 0.05) {
      log(
        "[metrics] Global trust weight from trust deposits differs from SUM(credential_schemas.weight) by >5% — denormalized CS columns may be stale (reindex or crawl lag).",
        { totalFromTrustDeposits: totalWeightFromTrustDeposits, sumCredentialSchemasWeight: denormalizedCsSum, relDiff }
      );
    }
  }

  if (
    typeof permissionsDerivedTotalWeight === "number" &&
    Number.isFinite(permissionsDerivedTotalWeight) &&
    permissionsDerivedTotalWeight > 0 &&
    totalWeightFromTrustDeposits > 0
  ) {
    const relDiff = Math.abs(permissionsDerivedTotalWeight - totalWeightFromTrustDeposits) / Math.max(
      permissionsDerivedTotalWeight,
      totalWeightFromTrustDeposits
    );
    if (relDiff > 0.05) {
      log(
        "[metrics] Global trust weight from permissions differs from trust_deposits SUM by >5% — permissions denormalized columns may be stale (reindex or crawl lag).",
        { totalFromPermissions: permissionsDerivedTotalWeight, totalFromTrustDeposits: totalWeightFromTrustDeposits, relDiff }
      );
    }
  }
}

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
        knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_schemas")
      )
      .first();
    const schemaIdRows = await knex("credential_schemas").select("id");
    const schemaIds = schemaIdRows
      .map((r: { id: unknown }) => Number(r.id))
      .filter((id) => Number.isFinite(id) && id > 0);

    const logger = (global as any).logger as { warn?: (...args: unknown[]) => void; debug?: (...args: unknown[]) => void } | undefined;

    const [trustDepositWeightAgg, permissionsWeightAgg, denormalizedCsWeightSum, csStatsBySchema] = await Promise.all([
      aggregateLiveTrustDepositWeight(knex),
      aggregateLiveTrustWeightFromPermissions(knex).catch(() => ({ totalWeight: 0, maxSinglePermWeight: 0 })),
      sumDenormalizedCredentialSchemaWeights(knex).catch(() => 0),
      calculateCredentialSchemaStatsBatch(schemaIds, undefined),
    ]);

    const totalWeight = trustDepositWeightAgg.totalWeight;
    logGlobalWeightSanity({
      totalWeightFromTrustDeposits: totalWeight,
      maxSingleDeposit: trustDepositWeightAgg.maxSingleDeposit,
      permissionsDerivedTotalWeight: permissionsWeightAgg.totalWeight,
      denormalizedCsSum: denormalizedCsWeightSum,
      logger,
    });

    let issuedSum = 0;
    let verifiedSum = 0;
    let ecosystemSlashEventsSum = 0;
    let ecosystemSlashedAmountSum = 0;
    let ecosystemSlashedAmountRepaidSum = 0;
    let networkSlashEventsSum = 0;
    let networkSlashedAmountSum = 0;
    let networkSlashedAmountRepaidSum = 0;

    for (const sid of schemaIds) {
      const s = csStatsBySchema.get(sid);
      if (!s) continue;
      issuedSum += Number(s.issued || 0);
      verifiedSum += Number(s.verified || 0);
      ecosystemSlashEventsSum += Number(s.ecosystem_slash_events || 0);
      ecosystemSlashedAmountSum += Number(s.ecosystem_slashed_amount || 0);
      ecosystemSlashedAmountRepaidSum += Number(s.ecosystem_slashed_amount_repaid || 0);
      networkSlashEventsSum += Number(s.network_slash_events || 0);
      networkSlashedAmountSum += Number(s.network_slashed_amount || 0);
      networkSlashedAmountRepaidSum += Number(s.network_slashed_amount_repaid || 0);
    }

    const nowIso = new Date().toISOString();
    const activePermsBase = () =>
      knex("permissions")
        .whereNotNull("grantee")
        .whereNull("repaid")
        .whereNull("slashed")
        .andWhere(function () {
          this.whereNull("revoked").orWhere("revoked", ">=", nowIso);
        })
        .andWhere(function () {
          this.whereNotNull("effective_from").andWhere("effective_from", "<=", nowIso);
        })
        .andWhere(function () {
          this.whereNull("effective_until").orWhere("effective_until", ">=", nowIso);
        });

    const participantsResult = await activePermsBase()
      .countDistinct("grantee as count")
      .first();
    const participants = Number((participantsResult as any)?.count ?? 0);

    const activeParticipantsByType = await activePermsBase()
      .select("type")
      .countDistinct("grantee as count")
      .groupBy("type");

    const participantsByType = {
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
    };
    for (const row of activeParticipantsByType as any[]) {
      const count = Number(row?.count || row?.count_distinct || 0);
      if (row.type === "ECOSYSTEM") participantsByType.participants_ecosystem = count;
      if (row.type === "ISSUER_GRANTOR") participantsByType.participants_issuer_grantor = count;
      if (row.type === "ISSUER") participantsByType.participants_issuer = count;
      if (row.type === "VERIFIER_GRANTOR") participantsByType.participants_verifier_grantor = count;
      if (row.type === "VERIFIER") participantsByType.participants_verifier = count;
      if (row.type === "HOLDER") participantsByType.participants_holder = count;
    }

    return {
      participants,
      ...participantsByType,
      active_trust_registries: activeTrustRegistries,
      archived_trust_registries: archivedTrustRegistries,
      active_schemas: Number(csAgg.active_schemas || 0),
      archived_schemas: Number(csAgg.archived_schemas || 0),
      weight: totalWeight,
      issued: issuedSum,
      verified: verifiedSum,
      ecosystem_slash_events: ecosystemSlashEventsSum,
      ecosystem_slashed_amount: ecosystemSlashedAmountSum,
      ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaidSum,
      network_slash_events: networkSlashEventsSum,
      network_slashed_amount: networkSlashedAmountSum,
      network_slashed_amount_repaid: networkSlashedAmountRepaidSum,
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
  const trustDepositWeightAgg = await aggregateHistoricalTrustDepositWeight(knex, blockHeight);
  const totalWeight = trustDepositWeightAgg.totalWeight;
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

  const allParticipantsSet = new Set<string>();
  const participantsEcosystemSet = new Set<string>();
  const participantsIssuerGrantorSet = new Set<string>();
  const participantsIssuerSet = new Set<string>();
  const participantsVerifierGrantorSet = new Set<string>();
  const participantsVerifierSet = new Set<string>();
  const participantsHolderSet = new Set<string>();
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

  const asOfTime = await getBlockChainTimeAsOf(blockHeight, {
    db: knex,
    logContext: "[metrics_helper]",
    atOrBefore: true,
    fallback: new Date(),
  });

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
      asOfTime
    );
    if (permState === "ACTIVE" && historyRecord.grantee) {
      allParticipantsSet.add(historyRecord.grantee);
      if (historyRecord.type === "ECOSYSTEM") participantsEcosystemSet.add(historyRecord.grantee);
      if (historyRecord.type === "ISSUER_GRANTOR") participantsIssuerGrantorSet.add(historyRecord.grantee);
      if (historyRecord.type === "ISSUER") participantsIssuerSet.add(historyRecord.grantee);
      if (historyRecord.type === "VERIFIER_GRANTOR") participantsVerifierGrantorSet.add(historyRecord.grantee);
      if (historyRecord.type === "VERIFIER") participantsVerifierSet.add(historyRecord.grantee);
      if (historyRecord.type === "HOLDER") participantsHolderSet.add(historyRecord.grantee);
    }
  }

  const participantsEcosystem = participantsEcosystemSet.size;
  const participantsIssuerGrantor = participantsIssuerGrantorSet.size;
  const participantsIssuer = participantsIssuerSet.size;
  const participantsVerifierGrantor = participantsVerifierGrantorSet.size;
  const participantsVerifier = participantsVerifierSet.size;
  const participantsHolder = participantsHolderSet.size;
  const participantsTotal = allParticipantsSet.size;

  return {
    participants: participantsTotal,
    participants_ecosystem: participantsEcosystem,
    participants_issuer_grantor: participantsIssuerGrantor,
    participants_issuer: participantsIssuer,
    participants_verifier_grantor: participantsVerifierGrantor,
    participants_verifier: participantsVerifier,
    participants_holder: participantsHolder,
    active_trust_registries: activeTrustRegistries,
    archived_trust_registries: archivedTrustRegistries,
    active_schemas: activeSchemas,
    archived_schemas: archivedSchemas,
    weight: Number(totalWeight),
    issued,
    verified,
    ecosystem_slash_events: ecosystemSlashEvents,
    ecosystem_slashed_amount: Number(ecosystemSlashedAmount),
    ecosystem_slashed_amount_repaid: Number(ecosystemSlashedAmountRepaid),
    network_slash_events: networkSlashEvents,
    network_slashed_amount: Number(networkSlashedAmount),
    network_slashed_amount_repaid: Number(networkSlashedAmountRepaid),
  };
}
