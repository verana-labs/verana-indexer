import { createHash } from "node:crypto";
import knex from "../../common/utils/db_connection";
import { toDate, toIso } from "../../common/utils/date_utils";
import {
  ALL_PARTICIPANT_ROLES,
  type ParticipantRole,
  type ParticipantState,
} from "../../common/types/types";
import { canonicalizeJson, toCoin } from "../../common";

/**
 * Builders for the VPR v4 Verifiable Trust resolve response
 * (`POST /v4/verifiable-trust/resolve`). Each builder maps indexer tables onto
 * the normative shapes.
 */

/**
 * Derives the single `participant_state` enum from a permission row's
 * lifecycle timestamps, following the priority order defined in the indexer
 * spec (Participant state derivation table).
 */
export function deriveParticipantState(
  row: {
    slashed?: Date | string | null;
    repaid?: Date | string | null;
    revoked?: Date | string | null;
    effective_from?: Date | string | null;
    effective_until?: Date | string | null;
  },
  now: Date
): ParticipantState {
  const slashed = toDate(row.slashed);
  const repaid = toDate(row.repaid);
  const revoked = toDate(row.revoked);
  const effectiveFrom = toDate(row.effective_from);
  const effectiveUntil = toDate(row.effective_until);

  if (slashed && repaid && repaid.getTime() >= slashed.getTime()) return "REPAID";
  if (slashed && (!repaid || repaid.getTime() < slashed.getTime())) return "SLASHED";
  if (revoked && revoked.getTime() <= now.getTime()) return "REVOKED";
  if (effectiveUntil && effectiveUntil.getTime() <= now.getTime()) return "EXPIRED";
  if (effectiveFrom && effectiveFrom.getTime() > now.getTime()) return "FUTURE";
  if (
    (!effectiveFrom || effectiveFrom.getTime() <= now.getTime()) &&
    (!effectiveUntil || effectiveUntil.getTime() > now.getTime())
  ) {
    return "ACTIVE";
  }
  return "INACTIVE";
}

function participantsByRole(row: Record<string, unknown>): Record<string, number> | undefined {
  const mapping: Array<[ParticipantRole, string]> = [
    ["HOLDER", "participants_holder"],
    ["ISSUER", "participants_issuer"],
    ["VERIFIER", "participants_verifier"],
    ["ISSUER_GRANTOR", "participants_issuer_grantor"],
    ["VERIFIER_GRANTOR", "participants_verifier_grantor"],
    ["ECOSYSTEM", "participants_ecosystem"],
  ];
  const out: Record<string, number> = {};
  for (const [role, col] of mapping) {
    const n = Number(row[col]);
    if (Number.isFinite(n) && n > 0) out[role] = Math.trunc(n);
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function normalizeRole(type: unknown): ParticipantRole | null {
  const t = String(type ?? "").toUpperCase();
  return (ALL_PARTICIPANT_ROLES as string[]).includes(t) ? (t as ParticipantRole) : null;
}

/**
 * Builds `participations[]` from the `permissions` table for the resolved DID,
 * filtered to the requested participant states.
 */
export async function buildParticipations(
  did: string,
  now: Date,
  states: ParticipantState[]
): Promise<Array<Record<string, unknown>>> {
  const stateSet = new Set(states.length > 0 ? states : ["ACTIVE"]);

  const rows = (await knex("permissions").where({ did }).orderBy("id", "asc")) as Array<
    Record<string, unknown>
  >;
  if (rows.length === 0) return [];

  const schemaIds = [...new Set(rows.map((r) => Number(r.schema_id)).filter((n) => Number.isFinite(n)))];
  const schemaToTr = new Map<number, number>();
  if (schemaIds.length > 0) {
    const csRows = (await knex("credential_schemas")
      .select("id", "tr_id")
      .whereIn("id", schemaIds)) as Array<{ id: number; tr_id: number }>;
    for (const cs of csRows) schemaToTr.set(Number(cs.id), Number(cs.tr_id));
  }

  const out: Array<Record<string, unknown>> = [];
  for (const row of rows) {
    const role = normalizeRole(row.type);
    if (!role) continue;
    const state = deriveParticipantState(row, now);
    if (!stateSet.has(state)) continue;

    const schemaId = Number(row.schema_id);
    const ecosystemId = schemaToTr.get(schemaId);
    const isEcosystem = role === "ECOSYSTEM";

    const entry: Record<string, unknown> = {
      id: Number(row.id),
      vsOperator: typeof row.vs_operator === "string" ? row.vs_operator : null,
      role,
      state,
      credentialSchemaId: Number.isFinite(schemaId) ? schemaId : 0,
      ecosystemId: Number.isFinite(Number(ecosystemId)) ? Number(ecosystemId) : 0,
      weight: toCoin(row.weight ?? row.deposit),
      validatorParticipantId: isEcosystem
        ? null
        : row.validator_perm_id != null
          ? Number(row.validator_perm_id)
          : 0,
    };

    if (row.issued != null) entry.issuedCredentials = Math.trunc(Number(row.issued)) || 0;
    if (row.verified != null) entry.verifiedCredentials = Math.trunc(Number(row.verified)) || 0;
    const pbr = participantsByRole(row);
    if (pbr) entry.participants = pbr;

    out.push(entry);
  }
  return out;
}

export type EcosystemsOptions = {
  includeArchived: boolean;
  credentialSchemas: { include: boolean; includeArchived: boolean };
};

const SRI_ALG_MAP: Record<string, "sha256" | "sha384" | "sha512"> = {
  "sha-256": "sha256",
  sha256: "sha256",
  "sha-384": "sha384",
  sha384: "sha384",
  "sha-512": "sha512",
  sha512: "sha512",
};

async function computeSchemaDigestSri(jsonSchema: unknown, algorithm: string | null | undefined): Promise<string> {
  const alg = SRI_ALG_MAP[String(algorithm ?? "").trim().toLowerCase()] ?? "sha256";
  const parsed = typeof jsonSchema === "string" ? safeParse(jsonSchema) : jsonSchema;
  const canonical = await canonicalizeJson(parsed ?? {});
  const digest = createHash(alg).update(canonical, "utf8").digest("base64");
  return `${alg}-${digest}`;
}

function safeParse(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}

async function buildGovernanceFramework(trId: number, activeVersion: number | null | undefined) {
  if (activeVersion == null) return undefined;
  const gfv = (await knex("governance_framework_version")
    .where({ tr_id: trId, version: activeVersion })
    .first()) as { id: number; version: number; active_since?: Date | string } | undefined;
  if (!gfv) return undefined;

  const docs = (await knex("governance_framework_document")
    .where({ gfv_id: Number(gfv.id) })
    .orderBy("language", "asc")) as Array<{ language: string; url: string; digest_sri: string }>;
  if (docs.length === 0) return undefined;

  const framework: Record<string, unknown> = {
    version: Number(gfv.version),
    documents: docs.map((d) => ({ language: d.language, url: d.url, digestSri: d.digest_sri })),
  };
  const activeSince = toIso(gfv.active_since);
  if (activeSince) framework.activeSince = activeSince;
  return framework;
}

async function buildEcosystemSchemas(
  trId: number,
  includeArchived: boolean
): Promise<Array<Record<string, unknown>>> {
  const q = knex("credential_schemas").where({ tr_id: trId });
  if (!includeArchived) q.whereNull("archived");
  const rows = (await q.orderBy("id", "asc")) as Array<Record<string, unknown>>;

  const out: Array<Record<string, unknown>> = [];
  for (const row of rows) {
    const schema: Record<string, unknown> = {
      id: Number(row.id),
      type: "JsonSchema",
      digestSri: await computeSchemaDigestSri(row.json_schema, row.digest_algorithm as string | null),
      archived: row.archived != null,
    };
    const pbr = participantsByRole(row);
    if (pbr) schema.participants = pbr;
    if (row.issued != null) schema.issuedCredentials = Math.trunc(Number(row.issued)) || 0;
    if (row.verified != null) schema.verifiedCredentials = Math.trunc(Number(row.verified)) || 0;
    out.push(schema);
  }
  return out;
}

/**
 * Builds `ecosystems[]` for the ecosystems the DID controls (trust_registry
 * rows whose `did` equals the resolved DID).
 */
export async function buildEcosystems(
  did: string,
  opts: EcosystemsOptions
): Promise<Array<Record<string, unknown>>> {
  const q = knex("trust_registry").where({ did });
  if (!opts.includeArchived) q.whereNull("archived");
  const rows = (await q.orderBy("id", "asc")) as Array<Record<string, unknown>>;
  if (rows.length === 0) return [];

  const out: Array<Record<string, unknown>> = [];
  for (const row of rows) {
    const trId = Number(row.id);
    const entry: Record<string, unknown> = {
      id: trId,
      corporationId: 0,
      archived: row.archived != null,
    };
    const egf = await buildGovernanceFramework(trId, row.active_version as number | null);
    if (egf) entry.egf = egf;
    if (opts.credentialSchemas.include) {
      entry.credentialSchemas = await buildEcosystemSchemas(trId, opts.credentialSchemas.includeArchived);
    }
    const pbr = participantsByRole(row);
    if (pbr) entry.participants = pbr;
    if (row.issued != null) entry.issuedCredentials = Math.trunc(Number(row.issued)) || 0;
    if (row.verified != null) entry.verifiedCredentials = Math.trunc(Number(row.verified)) || 0;
    out.push(entry);
  }
  return out;
}

// TODO: Resolve Corporation id when supported by verana-types.
export async function resolveCorporationId(_did: string): Promise<number> {
  return 0;
}
