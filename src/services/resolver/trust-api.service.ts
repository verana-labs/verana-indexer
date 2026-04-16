import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import { ECS, PermissionType, verifyPermissions } from "@verana-labs/verre";
import { createHash } from "node:crypto";
import BaseService from "../../base/base.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { BlockCheckpoint } from "../../models";
import {
  getDeclaredDereferenceCacheTtlSeconds,
  getDeclaredPollObjectCachingRetryDays,
  getResolverRuntimeConfig,
  getTrustEvaluationTtlSeconds,
  getVerreTrustEvaluationCallOptions,
  buildTrustSummaryFromStoredRow,
  extractQ1CredentialArrays,
  getTrustResultLatestByDidAtOrBeforeHeight,
  resolveTrustForDidAtHeight,
} from "./trust-resolve";

function isDidParam(did: string): did is string {
  return did.startsWith("did:");
}

type PermissionChainLink = {
  permissionId: number;
  type: string;
  did: string | null;
  deposit: string;
  permState: string;
  effectiveFrom?: string | null;
  effectiveUntil?: string | null;
};

type Q1Credential = {
  result: "VALID" | "IGNORED" | "FAILED";
  ecsType: "ECS-SERVICE" | "ECS-ORG" | "ECS-PERSONA" | "ECS-UA" | null;
  presentedBy: string | null;
  issuedBy: string | null;
  id?: string;
  type?: string;
  format?: string;
  issuedAt?: string;
  validUntil?: string;
  digestSri?: string;
  effectiveIssuanceTime?: string;
  vtjscId?: string;
  claims?: unknown;
  schema?: unknown;
  permissionChain?: Array<Record<string, unknown>>;
  digestAlgorithm?: string;
};

const RESOLVE_RESULT_STATUS_BY_OUTCOME: Record<string, { trustStatus: string; production: boolean }> = {
  verified: { trustStatus: "TRUSTED", production: true },
  "verified-test": { trustStatus: "PARTIAL", production: false },
};

const normalizeEcsKey = (value: string): string => value.trim().toLowerCase();
const VERRE_ECS = ECS ?? {
  SERVICE: "ecs-service",
  ORG: "ecs-org",
  PERSONA: "ecs-persona",
  USER_AGENT: "ecs-user-agent",
};
const ECS_BY_NORMALIZED: Record<string, string> = {
  [normalizeEcsKey(VERRE_ECS.SERVICE)]: VERRE_ECS.SERVICE,
  [normalizeEcsKey(VERRE_ECS.ORG)]: VERRE_ECS.ORG,
  [normalizeEcsKey(VERRE_ECS.PERSONA)]: VERRE_ECS.PERSONA,
  [normalizeEcsKey(VERRE_ECS.USER_AGENT)]: VERRE_ECS.USER_AGENT,
};

function detectEcsFromVtjscId(vtjscId: string): string | null {
  const normalized = normalizeEcsKey(vtjscId);
  if (ECS_BY_NORMALIZED[normalized]) return ECS_BY_NORMALIZED[normalized];

  const tokens = normalized.split(/[^a-z0-9-]+/).filter(Boolean);
  for (const token of tokens) {
    if (ECS_BY_NORMALIZED[token]) return ECS_BY_NORMALIZED[token];
  }
  return null;
}

function computeSri(algorithm: string, canonicalJson: string): string {
  const alg = algorithm.trim().toLowerCase();
  const hashAlg = alg === "sha-256" || alg === "sha256" ? "sha256" : alg === "sha-512" || alg === "sha512" ? "sha512" : null;
  if (!hashAlg) throw new Error(`Unsupported digest algorithm: ${algorithm}`);
  const digest = createHash(hashAlg).update(canonicalJson, "utf8").digest("base64");
  return `${hashAlg}-${digest}`;
}

let canonicalizeLoader: Promise<(v: unknown) => string> | null = null;
async function canonicalizeJson(value: unknown): Promise<string> {
  if (!canonicalizeLoader) {
    canonicalizeLoader = import("canonicalize").then((mod: any) => mod?.default ?? mod);
  }
  const fn = await canonicalizeLoader;
  const out = fn(value);
  if (typeof out !== "string") throw new Error("canonicalize did not return a string");
  return out;
}

function formatDeposit(n: unknown): string {
  const v = Number(n);
  if (!Number.isFinite(v)) return "0";
  return `${v}uvna`;
}

function toPermissionChainLink(p: Record<string, unknown>): PermissionChainLink {
  const id = Number(p.id);
  const state = String(p.perm_state ?? p.permState ?? "").toUpperCase() || "UNKNOWN";
  return {
    permissionId: Number.isFinite(id) ? id : 0,
    type: String(p.type ?? ""),
    did: (p.did as string) ?? (p.grantee as string) ?? null,
    deposit: formatDeposit(p.deposit),
    permState: state,
    effectiveFrom: p.effective_from != null ? String(p.effective_from) : p.effective != null ? String(p.effective) : null,
    effectiveUntil:
      p.effective_until != null
        ? String(p.effective_until)
        : p.expiration != null
          ? String(p.expiration)
          : null,
  };
}

const IS_PG = String((knex as { client?: { config?: { client?: string } } }).client?.config?.client || "").includes("pg");

async function findCredentialSchemaIdsByVtjscUri(vtjscId: string): Promise<number[]> {
  if (!vtjscId || typeof vtjscId !== "string") return [];

  if (IS_PG) {
    const res = await knex.raw(
      `
      SELECT id FROM credential_schemas
      WHERE (is_active IS NULL OR is_active = true)
        AND archived IS NULL
        AND (
          json_schema::jsonb->>'$id' = :uri
          OR json_schema::jsonb->>'id' = :uri
          OR json_schema::jsonb->>'@id' = :uri
        )
      LIMIT 25
      `,
      { uri: vtjscId }
    );
    const rows = (res as { rows?: Array<{ id?: unknown }> }).rows ?? [];
    return rows.map((r) => Number(r.id)).filter((id) => Number.isFinite(id) && id > 0);
  }

  const rows = await knex("credential_schemas").select("id").whereNull("archived").limit(200);
  const out: number[] = [];
  const needle = vtjscId.trim();
  for (const row of rows as Array<{ id?: unknown }>) {
    const id = Number((row as { id?: unknown }).id);
    if (!Number.isFinite(id) || id <= 0) continue;
    const raw = await knex("credential_schemas").select("json_schema").where("id", id).first();
    const js = (raw as { json_schema?: unknown } | undefined)?.json_schema;
    const s = typeof js === "string" ? js : JSON.stringify(js ?? {});
    if (s.includes(needle)) out.push(id);
  }
  return out.slice(0, 25);
}

@Service({
  name: SERVICE.V1.TrustV1ApiService.key,
  version: 1,
})
export class TrustApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private vtjscToSchemaId = new Map<string, number>();
  private schemaIdToDigestAlg = new Map<number, string | null>();
  private trustedVsAtHeight = new Map<string, boolean>();

  private async ensureDidExistsOr404(ctx: Context, did: string) {
    try {
      const hitTrust = await knex("trust_results").select("did").where({ did }).first();
      if (hitTrust) return null;
    } catch {
      this.logger.warn(`Failed to query trust_results table for existence check of DID ${did}. Proceeding with additional checks.`, { did });
    }
    try {
      const hitPerm = await knex("permissions").select("id").where({ did }).first();
      if (hitPerm) return null;
    } catch {
      this.logger.warn(`Failed to query permissions table for existence check of DID ${did}. Proceeding with additional checks.`, { did });
    }

    try {
      const hitPermH = await knex("permission_history").select("id").where({ did }).first();
      if (hitPermH) return null;
    } catch {
      this.logger.warn(`Failed to query permission_history table for existence check of DID ${did}. Proceeding with additional checks.`, { did });
    }
    try {
      const hitTr = await knex("trust_registry").select("id").where({ did }).first();
      if (hitTr) return null;
    } catch {
      this.logger.warn(`Failed to query trust_registry table for existence check of DID ${did}. Proceeding with additional checks.`, { did });
    }
    try {
      const hitTrH = await knex("trust_registry_history").select("id").where({ did }).first();
      if (hitTrH) return null;
    } catch {
      this.logger.warn(`Failed to query trust_registry_history table for existence check of DID ${did}. Proceeding with additional checks.`, { did });
    }

    return null;
  }

  private async getLastProcessedTrustBlockHeight(): Promise<number> {
    const trustRow = await BlockCheckpoint.query().where("job_name", BULL_JOB_NAME.HANDLE_TRUST_RESOLVE).first();
    const h = Number(trustRow?.height ?? 0);
    return Number.isFinite(h) && h >= 0 ? Math.trunc(h) : 0;
  }

  private async clampQueryBlockHeight(requested?: number): Promise<number> {
    const lastTrust = await this.getLastProcessedTrustBlockHeight();
    if (typeof requested === "number" && Number.isInteger(requested) && requested >= 0) {
      return Math.min(requested, lastTrust);
    }
    return lastTrust;
  }

  private brokerMeta(blockHeight: number) {
    return { meta: { blockHeight } };
  }

  private parseBlockHeightQuery(atBlock?: number, atParam?: string): number | undefined {
    if (typeof atBlock === "number" && Number.isInteger(atBlock)) return atBlock;
    if (atParam !== undefined && atParam !== "") {
      const n = Number.parseInt(String(atParam), 10);
      if (Number.isInteger(n) && n >= 0) return n;
    }
    return undefined;
  }

  private isIsoDateTime(s: string): boolean {
    if (!s) return false;
    const d = new Date(s);
    return Number.isFinite(d.getTime());
  }

  private async blockHeightAtOrBeforeTime(atIso: string): Promise<number | undefined> {
    const d = new Date(atIso);
    if (!Number.isFinite(d.getTime())) return undefined;
    const row = await knex("block").select("height").where("time", "<=", d).orderBy("height", "desc").first();
    const h = Number((row as any)?.height);
    return Number.isInteger(h) && h >= 0 ? h : undefined;
  }

  private async parseAtToHeight(atParam?: string): Promise<number | undefined> {
    if (atParam === undefined || atParam === "") return undefined;
    const asHeight = this.parseBlockHeightQuery(undefined, atParam);
    if (asHeight !== undefined) return asHeight;
    if (this.isIsoDateTime(atParam)) return this.blockHeightAtOrBeforeTime(atParam);
    return undefined;
  }

  private ecsTypeFromCredential(c: Record<string, unknown>): Q1Credential["ecsType"] {
    const vtjscId =
      (typeof c.vtjscId === "string" && c.vtjscId) ||
      (typeof (c.schema as any)?.jsonSchema === "string" && (c.schema as any).jsonSchema) ||
      "";
    const ecsType = detectEcsFromVtjscId(vtjscId);
    if (ecsType === VERRE_ECS.SERVICE) return "ECS-SERVICE";
    if (ecsType === VERRE_ECS.ORG) return "ECS-ORG";
    if (ecsType === VERRE_ECS.PERSONA) return "ECS-PERSONA";
    if (ecsType === VERRE_ECS.USER_AGENT) return "ECS-UA";
    return null;
  }

  private extractDigestAlgorithmFromSchemaJson(schemaJson: unknown): string | null {
    if (!schemaJson) return null;
    try {
      const js = typeof schemaJson === "string" ? (JSON.parse(schemaJson) as any) : (schemaJson as any);
      const direct =
        (typeof js?.digest_algorithm === "string" && js.digest_algorithm) ||
        (typeof js?.digestAlgorithm === "string" && js.digestAlgorithm) ||
        (typeof js?.digest === "object" && typeof js?.digest?.algorithm === "string" && js.digest.algorithm) ||
        null;
      return direct ? String(direct) : null;
    } catch {
      return null;
    }
  }

  private extractW3cCredentialJson(raw: Record<string, unknown>): unknown | null {
    const vc =
      (raw.credential as any) ??
      (raw.verifiableCredential as any) ??
      (raw.vc as any) ??
      (raw.rawCredential as any) ??
      null;
    if (vc && typeof vc === "object") return vc;
    if (typeof vc === "string") {
      try {
        return JSON.parse(vc);
      } catch {
        return null;
      }
    }
    return null;
  }

  private async tryComputeDigestSriForCredential(vtjscId: string | undefined, rawCredential: unknown): Promise<{
    digestSri?: string;
    digestAlgorithm?: string;
  }> {
    if (!vtjscId || !rawCredential) return {};
    let schemaId = this.vtjscToSchemaId.get(vtjscId);
    if (!schemaId) {
      const schemaIds = await findCredentialSchemaIdsByVtjscUri(vtjscId);
      if (schemaIds.length === 0) return {};
      schemaId = schemaIds[0];
      this.vtjscToSchemaId.set(vtjscId, schemaId);
    }

    let alg = this.schemaIdToDigestAlg.get(schemaId);
    if (alg === undefined) {
      const row = await knex("credential_schemas").select("json_schema").where("id", schemaId).first();
      alg = this.extractDigestAlgorithmFromSchemaJson((row as any)?.json_schema) ?? null;
      this.schemaIdToDigestAlg.set(schemaId, alg);
    }
    if (!alg) return {};
    try {
      const canonical = await canonicalizeJson(rawCredential);
      return { digestSri: computeSri(alg, canonical), digestAlgorithm: alg };
    } catch {
      return { digestAlgorithm: alg };
    }
  }

  private resultFromCredential(c: Record<string, unknown>): Q1Credential["result"] {
    const r = String(c.result ?? c.status ?? "").toUpperCase();
    if (r === "VALID") return "VALID";
    if (r === "FAILED") return "FAILED";
    if (r === "IGNORED") return "IGNORED";
    return "VALID";
  }

  private normalizeQ1Credential(raw: unknown, did: string): Q1Credential | null {
    if (!raw || typeof raw !== "object") return null;
    const c = raw as Record<string, unknown>;
    const schema = (c.schema as any) ?? undefined;
    const vtjscId =
      (typeof c.vtjscId === "string" && c.vtjscId) ||
      (typeof schema?.jsonSchema === "string" && schema.jsonSchema) ||
      undefined;
    const presentedBy = (typeof c.presentedBy === "string" && c.presentedBy) || (typeof c.presented_by === "string" && (c as any).presented_by) || did;
    const issuedBy = (typeof c.issuedBy === "string" && c.issuedBy) || (typeof c.issuer === "string" && c.issuer) || (typeof c.issued_by === "string" && (c as any).issued_by) || null;
    const claims = c.claims ?? c.credentialSubject ?? c.subjectClaims ?? undefined;
    const w3c = this.extractW3cCredentialJson(c);
    return {
      result: this.resultFromCredential(c),
      ecsType: this.ecsTypeFromCredential({ ...c, vtjscId }),
      presentedBy,
      issuedBy,
      id: typeof c.id === "string" ? c.id : undefined,
      type: typeof c.type === "string" ? c.type : undefined,
      format: typeof c.format === "string" ? c.format : undefined,
      issuedAt: typeof c.issuedAt === "string" ? c.issuedAt : typeof (c as any).issuanceDate === "string" ? (c as any).issuanceDate : undefined,
      validUntil: typeof c.validUntil === "string" ? c.validUntil : typeof (c as any).expirationDate === "string" ? (c as any).expirationDate : undefined,
      digestSri: typeof c.digestSri === "string" ? c.digestSri : undefined,
      effectiveIssuanceTime: typeof c.effectiveIssuanceTime === "string" ? c.effectiveIssuanceTime : undefined,
      vtjscId,
      claims,
      schema: c.schema,
      permissionChain: Array.isArray((c as any).permissionChain) ? ((c as any).permissionChain as any[]) : undefined,
    };
  }

  private ecosystemKeyFromCredential(c: Q1Credential): string {
    const eco = (c.schema as any)?.ecosystemDid;
    return typeof eco === "string" && eco ? eco : "unknown";
  }

  private computeVsReqTrustStatus(did: string, credentials: Q1Credential[]): { trustStatus: string; production: boolean } {
    const valid = credentials.filter((c) => c.result === "VALID");
    const services = valid.filter((c) => c.ecsType === "ECS-SERVICE" && c.presentedBy === did);
    if (services.length === 0) return { trustStatus: "UNTRUSTED", production: false };

    const byEco = new Map<string, { services: Q1Credential[]; orgPersonaByDid: Map<string, Q1Credential[]> }>();
    for (const c of valid) {
      const key = this.ecosystemKeyFromCredential(c);
      const cur = byEco.get(key) ?? { services: [], orgPersonaByDid: new Map<string, Q1Credential[]>() };
      if (c.ecsType === "ECS-SERVICE" && c.presentedBy === did) cur.services.push(c);
      if ((c.ecsType === "ECS-ORG" || c.ecsType === "ECS-PERSONA") && c.presentedBy) {
        const arr = cur.orgPersonaByDid.get(c.presentedBy) ?? [];
        arr.push(c);
        cur.orgPersonaByDid.set(c.presentedBy, arr);
      }
      byEco.set(key, cur);
    }

    let ecosystemsWithService = 0;
    let ecosystemsSatisfied = 0;
    for (const cur of byEco.values()) {
      if (cur.services.length === 0) continue;
      ecosystemsWithService += 1;
      let ok = false;
      for (const svc of cur.services) {
        const issuer = svc.issuedBy;
        if (!issuer) continue;
        if (issuer === did) {
          const arr = cur.orgPersonaByDid.get(did) ?? [];
          if (arr.length === 1) ok = true;
        } else {
          const arr = cur.orgPersonaByDid.get(issuer) ?? [];
          if (arr.length === 1) ok = true;
        }
        if (ok) break;
      }
      if (ok) ecosystemsSatisfied += 1;
    }

    if (ecosystemsSatisfied <= 0) return { trustStatus: "UNTRUSTED", production: false };
    if (ecosystemsSatisfied < ecosystemsWithService) return { trustStatus: "PARTIAL", production: false };
    return { trustStatus: "TRUSTED", production: true };
  }

  private trustStatusFallbackFromResolveResult(resolveResult: unknown): { trustStatus: string; production: boolean } {
    if (!resolveResult || typeof resolveResult !== "object" || (resolveResult as any).error) {
      return { trustStatus: "UNTRUSTED", production: false };
    }
    const { verified, outcome } = resolveResult as { verified?: unknown; outcome?: unknown };
    if (!verified) return { trustStatus: "UNTRUSTED", production: false };
    if (typeof outcome === "string" && RESOLVE_RESULT_STATUS_BY_OUTCOME[outcome]) {
      return RESOLVE_RESULT_STATUS_BY_OUTCOME[outcome];
    }
    return { trustStatus: "UNTRUSTED", production: false };
  }

  private async isTrustedVsAtHeight(did: string, height: number, visited: Set<string>): Promise<boolean> {
    const key = `${did}@${height}`;
    const cached = this.trustedVsAtHeight.get(key);
    if (cached !== undefined) return cached;
    if (visited.has(key)) return true;
    visited.add(key);
    const row = await getTrustResultLatestByDidAtOrBeforeHeight(did, height);
    if (!row) {
      this.trustedVsAtHeight.set(key, false);
      return false;
    }
    const summary = buildTrustSummaryFromStoredRow({
      did,
      resolveResult: row.resolve_result,
      evaluatedAtBlock: row.height,
      createdAt: row.created_at,
      trustTtlSeconds: getTrustEvaluationTtlSeconds(),
    });
    const ok = summary.trustStatus === "TRUSTED" || summary.trustStatus === "PARTIAL";
    this.trustedVsAtHeight.set(key, ok);
    return ok;
  }

  private async enrichCredentialPermissionChain(
    cred: Q1Credential,
    blockHeight: number
  ): Promise<Array<Record<string, unknown>>> {
    const raw = cred.permissionChain;
    if (!Array.isArray(raw) || raw.length === 0) return [];
    const visited = new Set<string>();
    const out: Array<Record<string, unknown>> = [];
    for (const link of raw) {
      if (!link || typeof link !== "object") continue;
      const did = typeof (link as any).did === "string" ? (link as any).did : null;
      const didIsTrustedVS = did ? await this.isTrustedVsAtHeight(did, blockHeight, visited) : false;
      out.push({
        ...link,
        didIsTrustedVS,
      });
    }
    return out;
  }

  private async walkPermissionChainUp(leafId: number, blockHeight: number): Promise<PermissionChainLink[]> {
    const chain: PermissionChainLink[] = [];
    let id: number | null = leafId;
    const seen = new Set<number>();
    const meta = this.brokerMeta(blockHeight);
    while (id != null && !seen.has(id)) {
      seen.add(id);
      const raw = (await this.broker.call(`${SERVICE.V1.PermAPIService.path}.getPermission`, { id }, meta)) as {
        permission?: Record<string, unknown>;
      };
      const perm = raw?.permission;
      if (!perm || typeof perm !== "object") break;
      chain.push(toPermissionChainLink(perm as Record<string, unknown>));
      const typ = String(perm.type ?? "").toUpperCase();
      const parent = perm.validator_perm_id != null ? Number(perm.validator_perm_id) : null;
      if (typ === "ECOSYSTEM" || !parent || !Number.isFinite(parent)) break;
      id = parent;
    }
    return chain;
  }

  private formatUvnaAmount(n: unknown): string {
    const v = Number(n);
    if (!Number.isFinite(v)) return "0uvna";
    return `${Math.trunc(v)}uvna`;
  }

  @Action({
    rest: "GET /resolve",
    params: {
      did: { type: "string" },
      detail: { type: "string", optional: true },
      at: { type: "string", optional: true },
    },
  })
  public async resolve(ctx: Context<{ did: string; detail?: string; at?: string }>) {
    const did = ctx.params.did;
    if (!isDidParam(did)) return ApiResponder.error(ctx, 'Missing or invalid "did". Must start with "did:".', 400);

    const detailRaw = (ctx.params.detail ?? "full").toLowerCase();
    if (detailRaw !== "summary" && detailRaw !== "full") {
      return ApiResponder.error(ctx, 'Invalid "detail". Use "summary" or "full".', 400);
    }

    const requestedHeight = await this.parseAtToHeight(ctx.params.at);
    const effectiveHeight = await this.clampQueryBlockHeight(requestedHeight);
    const row = await getTrustResultLatestByDidAtOrBeforeHeight(did, effectiveHeight);
    if (!row) return ApiResponder.error(ctx, `No trust evaluation found for DID: ${did}`, 404);

    const summary = buildTrustSummaryFromStoredRow({
      did,
      resolveResult: row.resolve_result,
      evaluatedAtBlock: row.height,
      createdAt: row.created_at,
      trustTtlSeconds: getTrustEvaluationTtlSeconds(),
    });

    if (detailRaw === "full") {
      const evaluatedAtIso = row.created_at != null ? new Date(row.created_at as Date | string).toISOString() : summary.evaluatedAt;
      const { credentials: rawCreds, failedCredentials } = extractQ1CredentialArrays(row.resolve_result);
      const credentials = (rawCreds as unknown[])
        .map((c) => this.normalizeQ1Credential(c, did))
        .filter(Boolean) as Q1Credential[];
    
      const withDigest = await Promise.all(
        credentials.map(async (cred, idx) => {
          if (cred.digestSri) return cred;
          const raw = (rawCreds as unknown[])[idx];
          const rawObj = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : null;
          const w3c = rawObj ? this.extractW3cCredentialJson(rawObj) : null;
          const computed = await this.tryComputeDigestSriForCredential(cred.vtjscId, w3c);
          return { ...cred, ...computed };
        })
      );
      const enrichedCredentials = await Promise.all(
        withDigest.map(async (c) => ({
          ...c,
          permissionChain: await this.enrichCredentialPermissionChain(c, row.height),
        }))
      );
      const vsReq = (() => {
        const computed = this.computeVsReqTrustStatus(did, enrichedCredentials);
        if (computed.trustStatus === "UNTRUSTED") {
          const fb = this.trustStatusFallbackFromResolveResult(row.resolve_result);
          return fb.trustStatus !== "UNTRUSTED" ? fb : computed;
        }
        return computed;
      })();
      const body: Record<string, unknown> = {
        did: summary.did,
        trustStatus: vsReq.trustStatus,
        production: vsReq.production,
        evaluatedAt: evaluatedAtIso,
        evaluatedAtBlock: row.height,
        credentials: enrichedCredentials,
        failedCredentials,
      };
      if (summary.expiresAt !== undefined) body.expiresAt = summary.expiresAt;
      return ApiResponder.success(ctx, body, 200);
    }

    const summaryBody: Record<string, unknown> = {
      did: summary.did,
      trustStatus: summary.trustStatus,
      production: summary.production,
      evaluatedAt: summary.evaluatedAt,
      evaluatedAtBlock: row.height,
    };
    if (summary.expiresAt !== undefined) summaryBody.expiresAt = summary.expiresAt;
    return ApiResponder.success(ctx, summaryBody, 200);
  }

  private async vtjscAuthorizationResponse(
    ctx: Context,
    args: {
      did: string;
      vtjscId: string;
      permissionType: typeof PermissionType.ISSUER | typeof PermissionType.VERIFIER;
      atBlock?: number;
      atParam?: string;
      sessionId?: string;
    }
  ) {
    const isIssuer = args.permissionType === PermissionType.ISSUER;
    const evaluatedAtBlock = await this.clampQueryBlockHeight(this.parseBlockHeightQuery(args.atBlock, args.atParam));
    const meta = this.brokerMeta(evaluatedAtBlock);
    const now = new Date().toISOString();
    const { verifiablePublicRegistries } = getVerreTrustEvaluationCallOptions();

    const schemaIds = await findCredentialSchemaIdsByVtjscUri(args.vtjscId);
    if (schemaIds.length === 0) {
      return ApiResponder.error(ctx, `No credential schema registered in the indexer for VTJSC: ${args.vtjscId}`, 404);
    }

    const verre = await verifyPermissions({
      did: args.did,
      jsonSchemaCredentialId: args.vtjscId,
      issuanceDate: now,
      verifiablePublicRegistries,
      permissionType: args.permissionType,
    });
    const verreOk = Boolean(verre.verified);

    const listType = isIssuer ? "ISSUER" : "VERIFIER";
    let leaf: Record<string, unknown> | null = null;
    let resolvedSchemaId: number | null = null;
    for (const sid of schemaIds) {
      const permResp = (await this.broker.call(
        `${SERVICE.V1.PermAPIService.path}.listPermissions`,
        { did: args.did, schema_id: sid, type: listType, only_valid: true, response_max_size: 32 },
        meta
      )) as { permissions?: Array<Record<string, unknown>> };
      const hit = (permResp?.permissions ?? []).find((p) => String(p.perm_state ?? "").toUpperCase() === "ACTIVE");
      if (hit) {
        leaf = hit;
        resolvedSchemaId = sid;
        break;
      }
    }

    const base = { did: args.did, vtjscId: args.vtjscId, evaluatedAt: now, evaluatedAtBlock };

    if (!leaf || resolvedSchemaId === null) {
      const schemaHint = schemaIds.length > 0 ? String(schemaIds[0]) : "?";
      return ApiResponder.success(
        ctx,
        {
          ...base,
          authorized: false,
          reason: `No active ${listType} permission found for DID on schema ${schemaHint} (VTJSC: ${args.vtjscId})`,
          permission: null,
          fees: { required: false },
          permissionChain: [] satisfies PermissionChainLink[],
        },
        200
      );
    }

    let benPerms: Array<Record<string, unknown>> = [];
    try {
      const ben = (await this.broker.call(
        `${SERVICE.V1.PermAPIService.path}.findBeneficiaries`,
        isIssuer ? { issuer_perm_id: Number(leaf.id) } : { verifier_perm_id: Number(leaf.id) },
        meta
      )) as { permissions?: Array<Record<string, unknown>> };
      benPerms = ben?.permissions ?? [];
    } catch {
      benPerms = [];
    }

    let totalFeeUnits = 0;
    const beneficiaries = benPerms.map((p) => {
      const pid = Number(p.id);
      const typ = String(p.type ?? "");
      if (isIssuer) {
        const iss = Number(p.issuance_fees ?? 0);
        totalFeeUnits += iss;
        return { permissionId: pid, type: typ, issuanceFees: this.formatUvnaAmount(iss) };
      }
      const vf = Number(p.verification_fees ?? 0);
      totalFeeUnits += vf;
      return { permissionId: pid, type: typ, verificationFees: this.formatUvnaAmount(vf) };
    });

    const feesRequired = totalFeeUnits > 0;

    if (feesRequired && !args.sessionId) {
      const reason = isIssuer
        ? "Payment required. Issuance fees are enabled for this schema but no sessionId was provided. The issuer must create a PermissionSession (MOD-PERM-MSG-10) and re-query with the sessionId."
        : "Payment required. Verification fees are enabled for this schema but no sessionId was provided. The verifier must create a PermissionSession (MOD-PERM-MSG-10) and re-query with the sessionId.";
      return ApiResponder.success(
        ctx,
        {
          authorized: false,
          ...base,
          reason,
          fees: {
            pricingAssetType: "COIN",
            pricingAsset: "uvna",
            totalBeneficiaryFees: this.formatUvnaAmount(totalFeeUnits),
            beneficiaries,
          },
        },
        402
      );
    }

    let session: Record<string, unknown> | null = null;
    let sessionOk = !feesRequired;
    if (feesRequired && args.sessionId) {
      try {
        const raw = (await this.broker.call(`${SERVICE.V1.PermAPIService.path}.getPermissionSession`, { id: args.sessionId }, meta)) as {
          session?: Record<string, unknown>;
        };
        const sn = raw?.session;
        if (sn && typeof sn === "object") {
          sessionOk = true;
          session = {
            id: args.sessionId,
            paid: true,
            ...(isIssuer ? { issuerPermId: Number(leaf.id) } : { verifierPermId: Number(leaf.id) }),
            agentPermId: Number(sn.agent_perm_id ?? 0) || 0,
            walletAgentPermId: Number(sn.wallet_agent_perm_id ?? 0) || 0,
            created: sn.created ?? null,
          };
        }
      } catch {
        sessionOk = false;
      }
    } else if (feesRequired && !args.sessionId && !isIssuer) {
      sessionOk = false;
    }

    const permissionChain = await this.walkPermissionChainUp(Number(leaf.id), evaluatedAtBlock);
    const active = String(leaf.perm_state ?? "").toUpperCase() === "ACTIVE";
    const permissionAuthorized = Boolean(active && verreOk);
    const authorized = permissionAuthorized && (!feesRequired || sessionOk);

    let denialReason: string | undefined;
    if (!permissionAuthorized) {
      denialReason = !active
        ? `No active ${listType} permission found for DID on schema ${resolvedSchemaId} (VTJSC: ${args.vtjscId})`
        : "Verre permission verification did not succeed for this DID and VTJSC.";
    } else if (feesRequired && args.sessionId && !sessionOk) {
      denialReason = "PermissionSession not found or invalid for the given sessionId.";
    }

    const permissionObj: Record<string, unknown> = {
      id: Number(leaf.id),
      type: leaf.type,
      schemaId: resolvedSchemaId,
      did: (leaf.did as string) ?? (leaf.grantee as string),
      deposit: this.formatUvnaAmount(leaf.deposit),
      permState: String(leaf.perm_state ?? "").toUpperCase(),
      effectiveFrom: leaf.effective_from ?? leaf.effective ?? null,
      effectiveUntil: leaf.effective_until ?? leaf.expiration ?? null,
    };
    if (isIssuer) {
      permissionObj.issuanceFeeDiscount = leaf.issuance_fee_discount != null ? String(leaf.issuance_fee_discount) : "0";
    } else {
      permissionObj.verificationFeeDiscount = leaf.verification_fee_discount != null ? String(leaf.verification_fee_discount) : "0";
    }

    const feesPayload = feesRequired
      ? {
          required: true,
          pricingAssetType: "COIN",
          pricingAsset: "uvna",
          totalBeneficiaryFees: this.formatUvnaAmount(totalFeeUnits),
          beneficiaries,
          paid: sessionOk,
        }
      : isIssuer
        ? {
            required: false,
            note: "All issuance fees are zero or fully discounted (issuanceFeeDiscount=1). No PermissionSession required for fee payment.",
          }
        : {
            required: false,
            note: "All verification fees are zero or fully discounted (verificationFeeDiscount=1). No PermissionSession required for fee payment.",
          };

    return ApiResponder.success(
      ctx,
      {
        ...base,
        authorized,
        ...(denialReason ? { reason: denialReason } : {}),
        permission: permissionObj,
        fees: feesPayload,
        permissionChain,
        ...(session ? { session } : {}),
      },
      200
    );
  }

  @Action({
    rest: "GET /issuer-authorization",
    params: {
      did: { type: "string" },
      at: { type: "string", optional: true },
      vtjscId: { type: "string" },
      sessionId: { type: "string", optional: true },
    },
  })
  public async issuerAuthorization(
    ctx: Context<{
      did: string;
      vtjscId: string;
      at?: string;
      sessionId?: string;
    }>
  ) {
    const { did } = ctx.params;
    const vtjscId = ctx.params.vtjscId;
    const sessionId = ctx.params.sessionId;
    if (!isDidParam(did)) return ApiResponder.error(ctx, 'Missing or invalid "did". Must start with "did:".', 400);
    if (!vtjscId || typeof vtjscId !== "string") {
      return ApiResponder.error(ctx, 'Missing or invalid "vtjscId" (VTJSC / JSON Schema credential id).', 400);
    }
    return this.vtjscAuthorizationResponse(ctx, {
      did,
      vtjscId,
      permissionType: PermissionType.ISSUER,
      atParam: ctx.params.at,
      sessionId,
    });
  }

  @Action({
    rest: "GET /verifier-authorization",
    params: {
      did: { type: "string" },
      at: { type: "string", optional: true },
      vtjscId: { type: "string" },
      sessionId: { type: "string", optional: true },
    },
  })
  public async verifierAuthorization(
    ctx: Context<{
      did: string;
      vtjscId: string;
      at?: string;
      sessionId?: string;
    }>
  ) {
    const { did } = ctx.params;
    const vtjscId = ctx.params.vtjscId;
    const sessionId = ctx.params.sessionId;
    if (!isDidParam(did)) return ApiResponder.error(ctx, 'Missing or invalid "did". Must start with "did:".', 400);
    if (!vtjscId || typeof vtjscId !== "string") {
      return ApiResponder.error(ctx, 'Missing or invalid "vtjscId" (VTJSC / JSON Schema credential id).', 400);
    }
    return this.vtjscAuthorizationResponse(ctx, {
      did,
      vtjscId,
      permissionType: PermissionType.VERIFIER,
      atParam: ctx.params.at,
      sessionId,
    });
  }

  @Action({
    rest: "GET /ecosystem-participant",
    params: {
      did: { type: "string" },
      ecosystemDid: { type: "string" },
      at: { type: "string", optional: true },
    },
  })
  public async ecosystemParticipant(
    ctx: Context<{
      did: string;
      ecosystemDid: string;
      at?: string;
    }>
  ) {
    const did = ctx.params.did;
    const ecosystemDid = ctx.params.ecosystemDid;
    if (!isDidParam(did)) return ApiResponder.error(ctx, 'Missing or invalid "did". Must start with "did:".', 400);
    if (!ecosystemDid || !isDidParam(ecosystemDid)) {
      return ApiResponder.error(ctx, 'Missing or invalid "ecosystemDid". Must start with "did:".', 400);
    }

    const evaluatedAtBlock = await this.clampQueryBlockHeight(this.parseBlockHeightQuery(undefined, ctx.params.at));
    const meta = this.brokerMeta(evaluatedAtBlock);
    const evaluatedAt = new Date().toISOString();

    const trRow = await knex("trust_registries").select("id", "aka").where({ did: ecosystemDid }).first();
    if (!trRow) {
      return ApiResponder.success(
        ctx,
        {
          did,
          ecosystemDid,
          ecosystemAka: null,
          isParticipant: false,
          evaluatedAt,
          evaluatedAtBlock,
          permissions: [],
        },
        200
      );
    }

    const schemaRows = await knex("credential_schemas").select("id", "json_schema").where({ tr_id: Number((trRow as any).id) }).whereNull("archived");

    const permissions: Array<Record<string, unknown>> = [];
    for (const s of schemaRows as Array<{ id?: unknown; json_schema?: unknown }>) {
      const schemaId = Number(s.id);
      if (!Number.isFinite(schemaId) || schemaId <= 0) continue;

      const resp = (await this.broker.call(
        `${SERVICE.V1.PermAPIService.path}.listPermissions`,
        { did, schema_id: schemaId, only_valid: true, response_max_size: 256 },
        meta
      )) as { permissions?: Array<Record<string, unknown>> };

      const vtjscId =
        (s.json_schema && typeof s.json_schema === "object"
          ? ((s.json_schema as any).$id ?? (s.json_schema as any).id ?? (s.json_schema as any)["@id"])
          : null) ?? null;

      for (const p of resp?.permissions ?? []) {
        const permState = String(p.perm_state ?? "").toUpperCase();
        if (permState !== "ACTIVE") continue;
        permissions.push({
          permissionId: Number(p.id),
          did: (p.did as string) ?? (p.grantee as string) ?? did,
          type: String(p.type ?? ""),
          schemaId,
          vtjscId,
          deposit: this.formatUvnaAmount(p.deposit),
          permState,
          effectiveFrom: p.effective_from ?? p.effective ?? null,
          effectiveUntil: p.effective_until ?? p.expiration ?? null,
        });
      }
    }

    return ApiResponder.success(
      ctx,
      {
        did,
        ecosystemDid,
        ecosystemAka: (trRow as any).aka ?? null,
        isParticipant: permissions.length > 0,
        evaluatedAt,
        evaluatedAtBlock,
        permissions,
      },
      200
    );
  }

  @Action({
    rest: "POST /refresh",
    params: {
      did: { type: "string" },
    }
  })
  public async refresh(ctx: Context<{ did: string }>) {
    const { did } = ctx.params;
    if (!isDidParam(did)) return ApiResponder.error(ctx, 'Missing or invalid "did". Must start with "did:".', 400);

    const didErr = await this.ensureDidExistsOr404(ctx, did);
    if (didErr) return didErr;

    const indexRow = await BlockCheckpoint.query().where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION).first();
    const indexH = Number(indexRow?.height ?? 0);
    const lastTrust = await this.getLastProcessedTrustBlockHeight();
    const requested = indexH;

    if (requested > lastTrust) {
      return ApiResponder.error(ctx, `Trust resolver has not finished block ${requested} yet; lastProcessedBlock is ${lastTrust}.`, 412);
    }

    await resolveTrustForDidAtHeight(did, requested);
    return ApiResponder.success(ctx, { did, result: "ok" }, 200);
  }
}

export { TrustApiService as TrustV1ApiService };

export default TrustApiService;
