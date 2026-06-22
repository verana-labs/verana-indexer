import knex from "../../common/utils/db_connection";
import { isValidDid, toIsoSeconds } from "../api/api_shared";
import { getTrustResultLatestByDidAtOrBeforeHeight, type TrustResultsRow } from "./trust-resolve";
import type { VtRawChange, VtTrustCore } from "../api/vt_subscribe_protocol";

const ROW_BOOKKEEPING_KEYS = new Set([
  "id",
  "height",
  "event_type",
  "action",
  "created",
  "created_at",
  "modified",
]);

const PARTICIPANT_COUNT_KEYS = [
  "participants",
  "participants_ecosystem",
  "participants_issuer_grantor",
  "participants_issuer",
  "participants_verifier_grantor",
  "participants_verifier",
  "participants_holder",
];

const PARTICIPATIONS_METRICS = {
  weight: new Set(["weight"]),
  counts: new Set(PARTICIPANT_COUNT_KEYS),
  issued: new Set(["issued"]),
  verified: new Set(["verified"]),
};

const ECOSYSTEMS_METRICS = {
  counts: new Set(PARTICIPANT_COUNT_KEYS),
  issued: new Set(["issued"]),
  verified: new Set(["verified"]),
};

const DEPOSIT_KEYS = new Set([
  "deposit",
  "share",
  "claimable",
  "slashed_deposit",
  "repaid_deposit",
  "last_slashed",
  "last_repaid",
  "slash_count",
]);

function changeKeys(changes: unknown): string[] {
  if (!changes || typeof changes !== "object") return [];
  return Object.keys(changes as Record<string, unknown>);
}

type ParticipationsCats = { structural: boolean; weight: boolean; counts: boolean; issued: boolean; verified: boolean };
type EcosystemsCats = { structural: boolean; counts: boolean; issued: boolean; verified: boolean };

function classifyParticipations(changes: unknown): ParticipationsCats {
  const keys = changeKeys(changes);
  if (keys.length === 0) return { structural: true, weight: false, counts: false, issued: false, verified: false };
  const cats: ParticipationsCats = { structural: false, weight: false, counts: false, issued: false, verified: false };
  for (const key of keys) {
    if (ROW_BOOKKEEPING_KEYS.has(key)) continue;
    if (PARTICIPATIONS_METRICS.weight.has(key)) cats.weight = true;
    else if (PARTICIPATIONS_METRICS.counts.has(key)) cats.counts = true;
    else if (PARTICIPATIONS_METRICS.issued.has(key)) cats.issued = true;
    else if (PARTICIPATIONS_METRICS.verified.has(key)) cats.verified = true;
    else cats.structural = true;
  }
  return cats;
}

function classifyEcosystems(changes: unknown): EcosystemsCats {
  const keys = changeKeys(changes);
  if (keys.length === 0) return { structural: true, counts: false, issued: false, verified: false };
  const cats: EcosystemsCats = { structural: false, counts: false, issued: false, verified: false };
  for (const key of keys) {
    if (ROW_BOOKKEEPING_KEYS.has(key)) continue;
    if (ECOSYSTEMS_METRICS.counts.has(key)) cats.counts = true;
    else if (ECOSYSTEMS_METRICS.issued.has(key)) cats.issued = true;
    else if (ECOSYSTEMS_METRICS.verified.has(key)) cats.verified = true;
    else cats.structural = true;
  }
  return cats;
}

function classifyDeposit(changes: unknown): boolean {
  const keys = changeKeys(changes);
  if (keys.length === 0) return true;
  return keys.some((key) => DEPOSIT_KEYS.has(key));
}

function trustCoreFromRow(row: TrustResultsRow, corporationId: number | null): VtTrustCore {
  const status = String(row.trust_status ?? "UNTRUSTED").toUpperCase();
  return {
    trusted: status === "TRUSTED" || status === "PARTIAL",
    evaluatedAtTime: toIsoSeconds(row.evaluated_at ?? row.created_at),
    evaluatedAtBlock: Number(row.height ?? 0),
    expiresAtTime: toIsoSeconds(row.expires_at),
    corporationId,
  };
}

function trustResultChanged(next: VtTrustCore, prev: VtTrustCore | null): boolean {
  if (!prev) return true;
  return next.trusted !== prev.trusted || next.corporationId !== prev.corporationId;
}

export class VtChangeAccumulator {
  private byDid = new Map<string, VtRawChange>();

  ensure(did: string): VtRawChange {
    let rc = this.byDid.get(did);
    if (!rc) {
      rc = {
        did,
        relatedDids: new Set(),
        corporationIds: new Set(),
        trust: null,
        corporation: null,
        participations: null,
        ecosystems: null,
        content: false,
      };
      this.byDid.set(did, rc);
    }
    return rc;
  }

  values(): VtRawChange[] {
    return [...this.byDid.values()];
  }
}

function mergeParticipations(
  cur: VtRawChange["participations"],
  cats: ParticipationsCats
): ParticipationsCats {
  const base = cur ?? { structural: false, weight: false, counts: false, issued: false, verified: false };
  return {
    structural: base.structural || cats.structural,
    weight: base.weight || cats.weight,
    counts: base.counts || cats.counts,
    issued: base.issued || cats.issued,
    verified: base.verified || cats.verified,
  };
}

function mergeEcosystems(cur: VtRawChange["ecosystems"], cats: EcosystemsCats): EcosystemsCats {
  const base = cur ?? { structural: false, counts: false, issued: false, verified: false };
  return {
    structural: base.structural || cats.structural,
    counts: base.counts || cats.counts,
    issued: base.issued || cats.issued,
    verified: base.verified || cats.verified,
  };
}

function mergeCorporation(
  cur: VtRawChange["corporation"],
  structural: boolean,
  deposit: boolean
): { structural: boolean; deposit: boolean } {
  const base = cur ?? { structural: false, deposit: false };
  return { structural: base.structural || structural, deposit: base.deposit || deposit };
}

function addCorporationId(ids: Set<number>, value: unknown): void {
  const n = Number(value);
  if (Number.isInteger(n) && n > 0) ids.add(n);
}

const VT_CHANGE_HEIGHT_TABLES = [
  "participant_history",
  "ecosystem_history",
  "credential_schema_history",
  "corporation_history",
  "trust_deposit_history",
  "trust_results",
];

export async function listVtChangeHeights(
  fromBlock: number,
  toBlock: number,
  limit: number
): Promise<number[]> {
  if (!Number.isInteger(fromBlock) || !Number.isInteger(toBlock) || fromBlock > toBlock) return [];
  if (!Number.isInteger(limit) || limit <= 0) return [];

  const subqueries = VT_CHANGE_HEIGHT_TABLES.map((table) =>
    knex(table).distinct("height").where("height", ">=", fromBlock).andWhere("height", "<=", toBlock)
  );
  const union = knex.union(subqueries, true);
  const rows = (await knex
    .from(union.as("t"))
    .distinct("height")
    .orderBy("height", "asc")
    .limit(limit)) as Array<{ height: number | string }>;

  return rows.map((r) => Number(r.height)).filter((h) => Number.isInteger(h));
}

export async function buildVtChangesForBlock(blockHeight: number): Promise<VtRawChange[]> {
  if (!Number.isInteger(blockHeight) || blockHeight < 0) return [];
  const acc = new VtChangeAccumulator();

  const participantRows = (await knex("participant_history")
    .select("did", "corporation_id", "validator_participant_id", "changes")
    .where("height", blockHeight)
    .whereNotNull("did")) as Array<{
    did: string;
    corporation_id: number | null;
    validator_participant_id: number | null;
    changes: unknown;
  }>;

  const validatorIds = [
    ...new Set(
      participantRows
        .map((r) => Number(r.validator_participant_id))
        .filter((id) => Number.isInteger(id) && id > 0)
    ),
  ];
  const validatorById = new Map<number, { did: string | null; corporation_id: number | null }>();
  if (validatorIds.length > 0) {
    const validators = (await knex("participants")
      .select("id", "did", "corporation_id")
      .whereIn("id", validatorIds)) as Array<{ id: number; did: string | null; corporation_id: number | null }>;
    for (const v of validators) {
      validatorById.set(Number(v.id), { did: v.did, corporation_id: v.corporation_id });
    }
  }

  for (const row of participantRows) {
    if (!isValidDid(row.did)) continue;
    const rc = acc.ensure(row.did);
    rc.participations = mergeParticipations(rc.participations, classifyParticipations(row.changes));
    addCorporationId(rc.corporationIds, row.corporation_id);
    const vid = Number(row.validator_participant_id);
    const validator = Number.isInteger(vid) ? validatorById.get(vid) : undefined;
    if (validator) {
      if (validator.did) rc.relatedDids.add(validator.did);
      addCorporationId(rc.corporationIds, validator.corporation_id);
    }
  }

  const ecosystemRows = (await knex("ecosystem_history")
    .select("did", "corporation_id", "changes")
    .where("height", blockHeight)
    .whereNotNull("did")) as Array<{ did: string; corporation_id: number | null; changes: unknown }>;
  for (const row of ecosystemRows) {
    if (!isValidDid(row.did)) continue;
    const rc = acc.ensure(row.did);
    rc.ecosystems = mergeEcosystems(rc.ecosystems, classifyEcosystems(row.changes));
    addCorporationId(rc.corporationIds, row.corporation_id);
  }

  const schemaRows = (await knex("credential_schema_history as csh")
    .join("ecosystem as e", "e.id", "csh.ecosystem_id")
    .select("e.did as did", "e.corporation_id as corporation_id", "csh.changes as changes")
    .where("csh.height", blockHeight)
    .whereNotNull("e.did")) as Array<{ did: string; corporation_id: number | null; changes: unknown }>;
  for (const row of schemaRows) {
    if (!isValidDid(row.did)) continue;
    const rc = acc.ensure(row.did);
    rc.ecosystems = mergeEcosystems(rc.ecosystems, classifyEcosystems(row.changes));
    addCorporationId(rc.corporationIds, row.corporation_id);
  }

  const corporationRows = (await knex("corporation_history")
    .select("did", "corporation_id", "changes")
    .where("height", blockHeight)
    .whereNotNull("did")) as Array<{ did: string; corporation_id: number | null; changes: unknown }>;
  for (const row of corporationRows) {
    if (!isValidDid(row.did)) continue;
    const rc = acc.ensure(row.did);
    rc.corporation = mergeCorporation(rc.corporation, true, false);
    addCorporationId(rc.corporationIds, row.corporation_id);
  }

  const depositRows = (await knex("trust_deposit_history")
    .select("corporation", "changes")
    .where("height", blockHeight)) as Array<{ corporation: string; changes: unknown }>;
  const accounts = [...new Set(depositRows.map((r) => r.corporation).filter((a) => typeof a === "string" && a))];
  const corpByAccount = new Map<string, { did: string; id: number }>();
  if (accounts.length > 0) {
    const corps = (await knex("corporation")
      .select("corporation", "did", "id")
      .whereIn("corporation", accounts)
      .whereNotNull("did")) as Array<{ corporation: string; did: string; id: number }>;
    for (const c of corps) corpByAccount.set(c.corporation, { did: c.did, id: Number(c.id) });
  }
  for (const row of depositRows) {
    const meta = corpByAccount.get(row.corporation);
    if (!meta || !isValidDid(meta.did)) continue;
    const rc = acc.ensure(meta.did);
    rc.corporation = mergeCorporation(rc.corporation, false, classifyDeposit(row.changes));
    addCorporationId(rc.corporationIds, meta.id);
  }

  const trustRows = (await knex("trust_results")
    .select("did", "height", "trust_status", "production", "evaluated_at", "expires_at", "created_at")
    .where("height", blockHeight)) as TrustResultsRow[];
  if (trustRows.length > 0) {
    const trustDids = [...new Set(trustRows.map((r) => r.did).filter(isValidDid))];
    const corpIdByDid = new Map<string, number>();
    if (trustDids.length > 0) {
      const corps = (await knex("corporation")
        .select("did", "id")
        .whereIn("did", trustDids)) as Array<{ did: string; id: number }>;
      for (const c of corps) corpIdByDid.set(c.did, Number(c.id));
    }

    for (const row of trustRows) {
      if (!isValidDid(row.did)) continue;
      const rc = acc.ensure(row.did);
      rc.content = true;
      const corporationId = corpIdByDid.get(row.did) ?? null;
      if (corporationId != null) rc.corporationIds.add(corporationId);
      const nextCore = trustCoreFromRow(row, corporationId);
      const prevRow = await getTrustResultLatestByDidAtOrBeforeHeight(row.did, blockHeight - 1);
      const prevCore = prevRow ? trustCoreFromRow(prevRow, corporationId) : null;
      if (trustResultChanged(nextCore, prevCore)) rc.trust = nextCore;
    }
  }

  return acc.values();
}
