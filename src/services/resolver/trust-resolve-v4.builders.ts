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
 * Builds `participations[]` from the `participants` table for the resolved DID,
 * filtered to the requested participant states.
 */
export async function buildParticipations(
  did: string,
  now: Date,
  states: ParticipantState[]
): Promise<Array<Record<string, unknown>>> {
  const stateSet = new Set(states.length > 0 ? states : ["ACTIVE"]);

  const rows = (await knex("participants").where({ did }).orderBy("id", "asc")) as Array<
    Record<string, unknown>
  >;
  if (rows.length === 0) return [];

  const schemaIds = [...new Set(rows.map((r) => Number(r.schema_id)).filter((n) => Number.isFinite(n)))];
  const schemaToEcosystemId = new Map<number, number>();
  if (schemaIds.length > 0) {
    const csRows = (await knex("credential_schemas")
      .select("id", "ecosystem_id")
      .whereIn("id", schemaIds)) as Array<{ id: number; ecosystem_id: number }>;
    for (const cs of csRows) schemaToEcosystemId.set(Number(cs.id), Number(cs.ecosystem_id));
  }

  const out: Array<Record<string, unknown>> = [];
  for (const row of rows) {
    const role = normalizeRole(row.type);
    if (!role) continue;
    const state = deriveParticipantState(row, now);
    if (!stateSet.has(state)) continue;

    const schemaId = Number(row.schema_id);
    const ecosystemId = schemaToEcosystemId.get(schemaId);
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
        : row.validator_participant_id != null
          ? Number(row.validator_participant_id)
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

async function buildGovernanceFramework(ecosystemId: number, activeVersion: number | null | undefined) {
  if (activeVersion == null) return undefined;
  const gfv = (await knex("governance_framework_version")
    .where({ ecosystem_id: ecosystemId, version: activeVersion })
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
  ecosystemId: number,
  includeArchived: boolean
): Promise<Array<Record<string, unknown>>> {
  const q = knex("credential_schemas").where({ ecosystem_id: ecosystemId });
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
 * Builds `ecosystems[]` for the ecosystems the DID controls (`ecosystem`
 * rows whose `did` equals the resolved DID).
 */
export async function buildEcosystems(
  did: string,
  opts: EcosystemsOptions
): Promise<Array<Record<string, unknown>>> {
  const q = knex("ecosystem").where({ did });
  if (!opts.includeArchived) q.whereNull("archived");
  const rows = (await q.orderBy("id", "asc")) as Array<Record<string, unknown>>;
  if (rows.length === 0) return [];

  const out: Array<Record<string, unknown>> = [];
  for (const row of rows) {
    const ecosystemId = Number(row.id);
    const entry: Record<string, unknown> = {
      id: ecosystemId,
      corporationId: Number.isFinite(Number(row.corporation_id)) ? Number(row.corporation_id) : 0,
      archived: row.archived != null,
    };
    const egf = await buildGovernanceFramework(ecosystemId, row.active_version as number | null);
    if (egf) entry.egf = egf;
    if (opts.credentialSchemas.include) {
      entry.credentialSchemas = await buildEcosystemSchemas(ecosystemId, opts.credentialSchemas.includeArchived);
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

const LINKED_VP_SERVICE_TYPE = "LinkedVerifiablePresentation";

function isLinkedVpType(type: unknown): boolean {
  if (typeof type === "string") return type === LINKED_VP_SERVICE_TYPE;
  if (Array.isArray(type)) return type.includes(LINKED_VP_SERVICE_TYPE);
  return false;
}

function didDocumentServices(resolveResult: unknown): Array<Record<string, unknown>> {
  if (!resolveResult || typeof resolveResult !== "object") return [];
  const didDocument = (resolveResult as { didDocument?: unknown }).didDocument;
  if (!didDocument || typeof didDocument !== "object") return [];
  const services = (didDocument as { service?: unknown }).service;
  if (!Array.isArray(services)) return [];
  return services.filter(
    (svc): svc is Record<string, unknown> => Boolean(svc) && typeof svc === "object"
  );
}

export function buildServices(resolveResult: unknown): Array<Record<string, unknown>> {
  return didDocumentServices(resolveResult).filter((svc) => !isLinkedVpType(svc.type));
}

export type PresentationsOptions = {
  unresolvableCredentialIds: boolean;
  invalidCredentialIds: boolean;
};

export function buildPresentations(
  resolveResult: unknown,
  opts: PresentationsOptions
): Array<Record<string, unknown>> {
  const didDocument = (resolveResult as { didDocument?: { id?: unknown } } | null)?.didDocument;
  const didId = typeof didDocument?.id === "string" ? didDocument.id : "";

  const out: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();
  for (const svc of didDocumentServices(resolveResult)) {
    if (!isLinkedVpType(svc.type)) continue;
    const rawId = typeof svc.id === "string" ? svc.id : "";
    const serviceId = rawId.startsWith("#") ? `${didId}${rawId}` : rawId;
    if (!serviceId || seen.has(serviceId)) continue;
    seen.add(serviceId);

    const endpoint = svc.serviceEndpoint;
    const entry: Record<string, unknown> = {
      id: typeof endpoint === "string" ? endpoint : "",
      serviceId,
      vtcCredentials: [],
    };
    if (opts.unresolvableCredentialIds) entry.unresolvableCredentialIds = [];
    if (opts.invalidCredentialIds) entry.invalidCredentialIds = [];
    out.push(entry);
  }
  return out;
}

const ECS_SCHEMA_TITLE_BY_TYPE: Record<string, string> = {
  "ecs-service": "ServiceCredential",
  "ecs-org": "OrganizationCredential",
  "ecs-persona": "PersonaCredential",
  "ecs-user-agent": "UserAgentCredential",
};

function parseSchemaJson(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === "object") return value as Record<string, unknown>;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : null;
    } catch {
      return null;
    }
  }
  return null;
}

function ecsSchemaVersionFromId(schemaJson: unknown): string {
  const sid = parseSchemaJson(schemaJson)?.$id;
  const m = typeof sid === "string" ? sid.match(/\/cs\/(v\d+)\//) : null;
  return m ? m[1] : "";
}

function toCredentialSubject(cred: Record<string, unknown>): Record<string, unknown> {
  const { schemaType, issuer, ...subject } = cred;
  return subject;
}

type EcsSchemaLink = {
  participantId: number;
  credentialSchemaId: number;
  ecosystemId: number;
  ecsSchemaVersion: string;
};

async function resolveEcsSchemaLink(subjectDid: string, ecsSchemaTitle: string): Promise<EcsSchemaLink | null> {
  const participants = (await knex("participants")
    .where({ did: subjectDid, type: "HOLDER" })
    .select("id", "schema_id")) as Array<{ id: number; schema_id: number }>;
  if (participants.length === 0) return null;

  const schemaIds = [...new Set(participants.map((p) => Number(p.schema_id)).filter((n) => Number.isFinite(n)))];
  if (schemaIds.length === 0) return null;
  const schemas = (await knex("credential_schemas")
    .whereIn("id", schemaIds)
    .select("id", "ecosystem_id", "json_schema")) as Array<{ id: number; ecosystem_id: number; json_schema: unknown }>;

  for (const p of participants) {
    const cs = schemas.find((s) => Number(s.id) === Number(p.schema_id));
    if (cs && parseSchemaJson(cs.json_schema)?.title === ecsSchemaTitle) {
      return {
        participantId: Number(p.id) || 0,
        credentialSchemaId: Number(cs.id) || 0,
        ecosystemId: Number(cs.ecosystem_id) || 0,
        ecsSchemaVersion: ecsSchemaVersionFromId(cs.json_schema),
      };
    }
  }
  return null;
}

async function resolveIssuerParticipantId(issuerDid: string, credentialSchemaId: number): Promise<number> {
  if (!issuerDid || credentialSchemaId <= 0) return 0;
  const row = (await knex("participants")
    .where({ did: issuerDid, schema_id: credentialSchemaId, type: "ISSUER" })
    .select("id")
    .first()) as { id?: number } | undefined;
  return row?.id != null ? Number(row.id) || 0 : 0;
}

export async function buildEcsCredentials(resolveResult: unknown): Promise<Array<Record<string, unknown>>> {
  if (!resolveResult || typeof resolveResult !== "object") return [];
  const r = resolveResult as Record<string, unknown>;

  const out: Array<Record<string, unknown>> = [];
  for (const key of ["service", "serviceProvider"]) {
    const cred = r[key];
    if (!cred || typeof cred !== "object") continue;
    const c = cred as Record<string, unknown>;
    const ecsSchema = ECS_SCHEMA_TITLE_BY_TYPE[String(c.schemaType ?? "").toLowerCase()];
    if (!ecsSchema) continue;

    const subjectDid = typeof c.id === "string" ? c.id : null;
    const issuerDid = typeof c.issuer === "string" ? c.issuer : null;
    const link = subjectDid ? await resolveEcsSchemaLink(subjectDid, ecsSchema) : null;
    const credentialSchemaId = link?.credentialSchemaId ?? 0;
    const issuerParticipantId = issuerDid ? await resolveIssuerParticipantId(issuerDid, credentialSchemaId) : 0;

    const entry: Record<string, unknown> = {
      ecsSchema,
      ecsSchemaVersion: link?.ecsSchemaVersion ?? "",
      credentialSchemaId,
      issuerParticipantId,
      ecosystemId: link?.ecosystemId ?? 0,
      participantId: link?.participantId ?? 0,
      credentialSubject: toCredentialSubject(c),
    };
    if (typeof c.validFrom === "string") entry.validFrom = c.validFrom;
    if (typeof c.validUntil === "string") entry.validUntil = c.validUntil;
    out.push(entry);
  }
  return out;
}
