import type { Knex } from "knex";
import knex from "../../common/utils/db_connection";
import {
  calculateCredentialSchemaStats,
  calculateCredentialSchemaStatsBatch,
} from "../crawl-cs/cs_stats";
import { calculateParticipantState } from "../crawl-pp/pp_state_utils";
import { getBlockChainTimeAsOf } from "../../common/utils/block_time";
import { resolveParticipantsParticipantColumn } from "../../common/utils/installed_table_columns";

function isMetricsPgClient(db: Knex): boolean {
  return String((db as any)?.client?.config?.client || "").includes("pg");
}

async function pgTableColumnsLower(db: Knex, table: string): Promise<Set<string>> {
  if (!isMetricsPgClient(db)) {
    return new Set();
  }
  const r = await db.raw(
    `SELECT column_name FROM information_schema.columns
     WHERE table_schema = current_schema() AND table_name = ?`,
    [table]
  );
  const rows = (r as { rows?: { column_name: string }[] }).rows ?? [];
  return new Set(rows.map((x) => String(x.column_name).toLowerCase()));
}

async function aggregateLiveTrustWeightFromParticipants(db: Knex): Promise<{
  totalWeight: number;
  maxSingleParticipantWeight: number;
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

  const row = await db("participants as p")
    .join("credential_schemas as cs", "cs.id", "p.schema_id")
    .select(db.raw(`${sumExpr} as total_weight`), db.raw(`${maxExpr} as max_single`))
    .first();

  const totalWeight = parseMetricsNumeric((row as any)?.total_weight);
  const maxSingleParticipantWeight = parseMetricsNumeric((row as any)?.max_single);
  return { totalWeight, maxSingleParticipantWeight };
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

async function resolveTrustDepositsAmountColumn(db: Knex): Promise<"deposit" | "amount"> {
  if (isMetricsPgClient(db)) {
    const cols = await pgTableColumnsLower(db, "trust_deposits");
    if (cols.has("deposit")) return "deposit";
    if (cols.has("amount")) return "amount";
    return "amount";
  }
  if (await db.schema.hasColumn("trust_deposits", "deposit")) return "deposit";
  return "amount";
}

async function resolveTrustDepositHistoryColumns(
  db: Knex
): Promise<{ corp: "corporation" | "account"; amount: "deposit" | "amount" }> {
  if (isMetricsPgClient(db)) {
    const cols = await pgTableColumnsLower(db, "trust_deposit_history");
    return {
      corp: cols.has("corporation") ? "corporation" : "account",
      amount: cols.has("deposit") ? "deposit" : "amount",
    };
  }
  const hasCorp = await db.schema.hasColumn("trust_deposit_history", "corporation");
  const hasDeposit = await db.schema.hasColumn("trust_deposit_history", "deposit");
  return {
    corp: hasCorp ? "corporation" : "account",
    amount: hasDeposit ? "deposit" : "amount",
  };
}

async function aggregateLiveTrustDepositWeight(db: Knex): Promise<{
  totalWeight: number;
  maxSingleDeposit: number;
}> {
  let amountCol = await resolveTrustDepositsAmountColumn(db);
  const buildSelect = (col: "deposit" | "amount") => {
    const castTotalExpr = isMetricsPgClient(db)
      ? `COALESCE(SUM(${col}::numeric), 0)`
      : `COALESCE(SUM(CAST(${col} AS REAL)), 0)`;
    const castMaxExpr = isMetricsPgClient(db)
      ? `COALESCE(MAX(${col}::numeric), 0)`
      : `COALESCE(MAX(CAST(${col} AS REAL)), 0)`;
    return db("trust_deposits")
      .select(db.raw(`${castTotalExpr} as total_weight`), db.raw(`${castMaxExpr} as max_single`))
      .first();
  };

  let row: unknown;
  try {
    row = await buildSelect(amountCol);
  } catch (err: unknown) {
    const msg = err && typeof err === "object" && "message" in err ? String((err as Error).message) : "";
    const code = err && typeof err === "object" && "code" in err ? String((err as { code?: string }).code) : "";
    if (code === "42703" && amountCol === "deposit" && /column .*deposit/i.test(msg)) {
      amountCol = "amount";
      row = await buildSelect(amountCol);
    } else {
      throw err;
    }
  }

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
    const { corp, amount } = await resolveTrustDepositHistoryColumns(db);
    const corpQualified = `tdh.${corp}`;
    const amountQualified = `tdh.${amount}`;
    const tdLatest = db("trust_deposit_history as tdh")
      .distinctOn(corpQualified)
      .select(db.raw(`${corpQualified} as corporation`), db.raw(`${amountQualified} as deposit`))
      .where("tdh.height", "<=", blockHeight)
      .orderBy(corpQualified, "asc")
      .orderBy("tdh.height", "desc")
      .orderBy("tdh.created_at", "desc")
      .orderBy("tdh.id", "desc")
      .as("latest_td");

    const row = await db.from(tdLatest)
      .select(
        db.raw(`COALESCE(SUM(latest_td.deposit::numeric), 0) as total_weight`),
        db.raw(`COALESCE(MAX(latest_td.deposit::numeric), 0) as max_single`)
      )
      .first();

    return {
      totalWeight: parseMetricsNumeric((row as any)?.total_weight),
      maxSingleDeposit: parseMetricsNumeric((row as any)?.max_single),
    };
  }

  const { corp, amount } = await resolveTrustDepositHistoryColumns(db);
  const tdSub = db("trust_deposit_history as tdh")
    .select(db.raw(`tdh.${corp} as corporation`), db.raw(`tdh.${amount} as deposit`))
    .select(db.raw(`ROW_NUMBER() OVER (PARTITION BY tdh.${corp} ORDER BY tdh.height DESC, tdh.created_at DESC, tdh.id DESC) as rn`))
    .where("tdh.height", "<=", blockHeight)
    .as("ranked_td");

  const tdLatest = db.from(tdSub).select("corporation", "deposit").where("rn", 1).as("latest_td");

  const row = await db.from(tdLatest)
    .select(
      db.raw(`COALESCE(SUM(CAST(latest_td.deposit AS REAL)), 0) as total_weight`),
      db.raw(`COALESCE(MAX(CAST(latest_td.deposit AS REAL)), 0) as max_single`)
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
  participantsDerivedTotalWeight?: number;
  denormalizedCsSum?: number;
  logger?: { warn?: (...args: unknown[]) => void; debug?: (...args: unknown[]) => void };
}): void {
  const { totalWeightFromTrustDeposits, maxSingleDeposit, participantsDerivedTotalWeight, denormalizedCsSum, logger } = opts;
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
    typeof participantsDerivedTotalWeight === "number" &&
    Number.isFinite(participantsDerivedTotalWeight) &&
    participantsDerivedTotalWeight > 0 &&
    totalWeightFromTrustDeposits > 0
  ) {
    const relDiff = Math.abs(participantsDerivedTotalWeight - totalWeightFromTrustDeposits) / Math.max(
      participantsDerivedTotalWeight,
      totalWeightFromTrustDeposits
    );
    if (relDiff > 0.05) {
      log(
        "[metrics] Global trust weight from participants differs from trust_deposits SUM by >5% — participants denormalized columns may be stale (reindex or crawl lag).",
        { totalFromParticipants: participantsDerivedTotalWeight, totalFromTrustDeposits: totalWeightFromTrustDeposits, relDiff }
      );
    }
  }
}

export async function computeGlobalMetrics(blockHeight?: number) {
  const useHistory = typeof blockHeight === "number";

  if (!useHistory) {
    const trCounts = await knex("ecosystem")
      .select(
        knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_ecosystems"),
        knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_ecosystems")
      )
      .first();
    let activeEcosystems = Number(trCounts?.active_ecosystems || 0);
    let archivedEcosystems = Number(trCounts?.archived_ecosystems || 0);

    if (activeEcosystems + archivedEcosystems === 0) {
      const ecosystemHistoryCount = await knex("ecosystem_history").count("* as count").first();
      const hasHistory = Number((ecosystemHistoryCount as any)?.count || 0) > 0;
      if (hasHistory) {
        const trSub = knex("ecosystem_history")
          .select("ecosystem_id", "archived")
          .select(
            knex.raw(
              "ROW_NUMBER() OVER (PARTITION BY ecosystem_id ORDER BY height DESC, created_at DESC) as rn"
            )
          )
          .as("ranked_tr");
        const latest = await knex.from(trSub).select("archived").where("rn", 1);
        activeEcosystems = latest.filter((r: any) => !r.archived).length;
        archivedEcosystems = latest.filter((r: any) => r.archived).length;
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

    const [trustDepositWeightAgg, participantsWeightAgg, denormalizedCsWeightSum, csStatsBySchema] = await Promise.all([
      aggregateLiveTrustDepositWeight(knex),
      aggregateLiveTrustWeightFromParticipants(knex).catch(() => ({ totalWeight: 0, maxSingleParticipantWeight: 0 })),
      sumDenormalizedCredentialSchemaWeights(knex).catch(() => 0),
      calculateCredentialSchemaStatsBatch(schemaIds, undefined),
    ]);

    const totalWeight = trustDepositWeightAgg.totalWeight;
    logGlobalWeightSanity({
      totalWeightFromTrustDeposits: totalWeight,
      maxSingleDeposit: trustDepositWeightAgg.maxSingleDeposit,
      participantsDerivedTotalWeight: participantsWeightAgg.totalWeight,
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
    const participantParticipantCol = await resolveParticipantsParticipantColumn(knex);
    const activeParticipantsBase = () =>
      knex("participants")
        .whereNotNull(participantParticipantCol)
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

    const participantsResult = await activeParticipantsBase()
      .countDistinct(`${participantParticipantCol} as count`)
      .first();
    const participants = Number((participantsResult as any)?.count ?? 0);

    const activeParticipantsByType = await activeParticipantsBase()
      .select("role")
      .countDistinct(`${participantParticipantCol} as count`)
      .groupBy("role");

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
      if (row.role === "ECOSYSTEM") participantsByType.participants_ecosystem = count;
      if (row.role === "ISSUER_GRANTOR") participantsByType.participants_issuer_grantor = count;
      if (row.role === "ISSUER") participantsByType.participants_issuer = count;
      if (row.role === "VERIFIER_GRANTOR") participantsByType.participants_verifier_grantor = count;
      if (row.role === "VERIFIER") participantsByType.participants_verifier = count;
      if (row.role === "HOLDER") participantsByType.participants_holder = count;
    }

    return {
      participants,
      ...participantsByType,
      active_ecosystems: activeEcosystems,
      archived_ecosystems: archivedEcosystems,
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

  const trSub = knex("ecosystem_history")
    .select("ecosystem_id")
    .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY ecosystem_id ORDER BY height DESC, created_at DESC) as rn"))
    .where("height", "<=", blockHeight)
    .as("ranked_tr");

  const trLatest = await knex.from(trSub).select("ecosystem_id").where("rn", 1);
  const ecosystemIds = trLatest.map((r: any) => Number(r.ecosystem_id));
  let activeEcosystems = 0;
  let archivedEcosystems = 0;
  for (const ecosystemId of ecosystemIds) {
    const ecosystemHistory = await knex("ecosystem_history")
      .where("ecosystem_id", ecosystemId)
      .where("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .first();
    if (ecosystemHistory) {
      if (ecosystemHistory.archived) archivedEcosystems++;
      else activeEcosystems++;
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
  const latestHistorySubquery = knex("participant_history")
    .select("participant_id")
    .select(
      knex.raw(
        `ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
      )
    )
    .where("height", "<=", blockHeight)
    .as("ranked");

  const participantIdsAtHeight = await knex
    .from(latestHistorySubquery)
    .select("participant_id")
    .where("rn", 1)
    .then((rows: any[]) => rows.map((r: any) => String(r.participant_id)));

  const asOfTime = await getBlockChainTimeAsOf(blockHeight, {
    db: knex,
    logContext: "[metrics_helper]",
    atOrBefore: true,
    fallback: new Date(),
  });

  for (const participantId of participantIdsAtHeight) {
    const historyRecord = await knex("participant_history")
      .where({ participant_id: String(participantId) })
      .where("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .first();
    if (!historyRecord) continue;
    const participantState = calculateParticipantState(
      {
        repaid: historyRecord.repaid,
        slashed: historyRecord.slashed,
        revoked: historyRecord.revoked,
        effective_from: historyRecord.effective_from,
        effective_until: historyRecord.effective_until,
        role: historyRecord.role,
        op_state: historyRecord.op_state,
        op_exp: historyRecord.op_exp,
        validator_participant_id: historyRecord.validator_participant_id,
      },
      asOfTime
    );
    const corp = (historyRecord as { corporation?: string }).corporation;
    if (participantState === "ACTIVE" && corp) {
      allParticipantsSet.add(corp);
      if (historyRecord.role === "ECOSYSTEM") participantsEcosystemSet.add(corp);
      if (historyRecord.role === "ISSUER_GRANTOR") participantsIssuerGrantorSet.add(corp);
      if (historyRecord.role === "ISSUER") participantsIssuerSet.add(corp);
      if (historyRecord.role === "VERIFIER_GRANTOR") participantsVerifierGrantorSet.add(corp);
      if (historyRecord.role === "VERIFIER") participantsVerifierSet.add(corp);
      if (historyRecord.role === "HOLDER") participantsHolderSet.add(corp);
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
    active_ecosystems: activeEcosystems,
    archived_ecosystems: archivedEcosystems,
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
