import knex from "../../common/utils/db_connection";

export const INDEXED_DIDS_DEFAULT_LIMIT = 1000;
export const INDEXED_DIDS_MAX_LIMIT = 10000;

export function parseIndexedDidsLimit(
  raw: unknown
): { ok: true; value: number } | { ok: false; error: string } {
  if (raw === undefined || raw === null || raw === "") {
    return { ok: true, value: INDEXED_DIDS_DEFAULT_LIMIT };
  }
  const n = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isInteger(n) || n < 1 || n > INDEXED_DIDS_MAX_LIMIT) {
    return {
      ok: false,
      error: `'limit' must be an integer between 1 and ${INDEXED_DIDS_MAX_LIMIT}; got ${String(raw)}`,
    };
  }
  return { ok: true, value: n };
}

export function encodeOffsetCursor(offset: number): string {
  return Buffer.from(JSON.stringify({ offset }), "utf-8").toString("base64");
}

export function decodeOffsetCursor(
  raw: unknown
): { ok: true; value: number } | { ok: false; error: string } {
  if (raw === undefined || raw === null || raw === "") return { ok: true, value: 0 };
  if (typeof raw !== "string") return { ok: false, error: "'cursor' must be a string" };
  try {
    const decoded = JSON.parse(Buffer.from(raw, "base64").toString("utf-8")) as { offset?: unknown };
    const offset = Number(decoded.offset);
    if (!Number.isInteger(offset) || offset < 0) {
      return { ok: false, error: "'cursor' is not a valid pagination cursor" };
    }
    return { ok: true, value: offset };
  } catch {
    return { ok: false, error: "'cursor' is not a valid pagination cursor" };
  }
}

function universeSubqueries(atBlock: number) {
  const base = (table: string) =>
    knex(table).distinct("did").whereNotNull("did").andWhere("height", "<=", atBlock);
  return [
    base("corporation_history"),
    base("ecosystem_history"),
    base("participant_history"),
    base("trust_results"),
  ];
}

function corporationSubqueries(atBlock: number, corporationId: number) {
  const scoped = (table: string) =>
    knex(table)
      .distinct("did")
      .whereNotNull("did")
      .andWhere("height", "<=", atBlock)
      .andWhere("corporation_id", corporationId);
  const validatorsAtBlock = knex("participant_history as vph")
    .distinctOn("vph.participant_id")
    .select("vph.participant_id", "vph.corporation_id")
    .where("vph.height", "<=", atBlock)
    .orderBy("vph.participant_id", "asc")
    .orderBy("vph.height", "desc")
    .orderBy("vph.id", "desc");

  const validatorIdsOwnedByCorp = knex
    .from(validatorsAtBlock.as("v"))
    .select("v.participant_id")
    .where("v.corporation_id", corporationId);

  const validatorBranch = knex("participant_history as ph")
    .distinct("ph.did as did")
    .whereNotNull("ph.did")
    .andWhere("ph.height", "<=", atBlock)
    .whereIn("ph.validator_participant_id", validatorIdsOwnedByCorp);
  return [
    scoped("corporation_history"),
    scoped("ecosystem_history"),
    scoped("participant_history"),
    validatorBranch,
  ];
}

export async function listIndexedDidsPage(opts: {
  atBlock: number;
  corporationId: number | null;
  offset: number;
  limit: number;
}): Promise<{ dids: string[]; nextCursor: string | null }> {
  const { atBlock, corporationId, offset, limit } = opts;
  if (!Number.isInteger(atBlock) || atBlock < 0) {
    return { dids: [], nextCursor: null };
  }

  const subqueries =
    corporationId === null
      ? universeSubqueries(atBlock)
      : corporationSubqueries(atBlock, corporationId);

  const union = knex.union(subqueries, true);
  const rows = (await knex
    .from(union.as("u"))
    .distinct("did")
    .orderBy("did", "asc")
    .offset(offset)
    .limit(limit + 1)) as Array<{ did: string }>;

  const hasMore = rows.length > limit;
  const dids = rows.slice(0, limit).map((r) => r.did);
  const nextCursor = hasMore ? encodeOffsetCursor(offset + limit) : null;
  return { dids, nextCursor };
}
