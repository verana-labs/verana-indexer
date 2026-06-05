import { Tendermint37Client } from "@cosmjs/tendermint-rpc";
import { QueryClient } from "@cosmjs/stargate";
import {
  QueryClientImpl as EcQueryClientImpl,
  QueryGetEcosystemRequest,
} from "@verana-labs/verana-types/codec/verana/ec/v1/query";
import type { EcosystemWithVersions } from "@verana-labs/verana-types/codec/verana/ec/v1/types";
import { VeranaEcosystemMessageTypes } from "../../common/verana-message-types";
import { Network } from "../../network";

export interface LedgerTrustRegistryVersion {
  id?: number | string;
  version?: number | string;
  created?: string;
  active_since?: string | null;
  activeSince?: string | null;
  documents?: Array<{
    id?: number | string;
    created?: string;
    language?: string | null;
    url?: string | null;
    digest_sri?: string | null;
    digestSri?: string | null;
  }>;
  [key: string]: unknown;
}

export interface LedgerTrustRegistry {
  id?: number | string;
  tr_id?: number | string;
  did?: string;
  controller?: string;
  created?: string;
  modified?: string;
  archived?: string | null;
  deposit?: number | string;
  aka?: string | null;
  language?: string | null;
  active_version?: number | string;
  activeVersion?: number | string;
  versions?: LedgerTrustRegistryVersion[];
  [key: string]: unknown;
}

export interface LedgerTrustRegistryResponse {
  trust_registry?: LedgerTrustRegistry;
  trustRegistry?: LedgerTrustRegistry;
  tr?: LedgerTrustRegistry;
  [key: string]: unknown;
}

export interface TrMessageLike {
  type: string;
  content?: Record<string, unknown> | null;
}

const TR_MESSAGE_TYPES = new Set<string>([
  VeranaEcosystemMessageTypes.CreateEcosystem,
  VeranaEcosystemMessageTypes.UpdateEcosystem,
  VeranaEcosystemMessageTypes.ArchiveEcosystem,
  VeranaEcosystemMessageTypes.AddGovernanceFrameworkDoc,
  VeranaEcosystemMessageTypes.IncreaseGovernanceFrameworkVersion,
  VeranaEcosystemMessageTypes.UpdateParams,
]);

function getRpcBaseUrl(): string {
  const envRpc =
    (typeof process !== "undefined" && process.env?.RPC_ENDPOINT?.trim()) || "";
  const base = envRpc || Network?.RPC || "";
  return base.replace(/\/$/, "");
}

function dateToIsoOrNull(value: unknown): string | null {
  if (!value) return null;
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString();
  }
  return typeof value === "string" ? value : null;
}

export function mapEcosystemToLedgerTrustRegistry(
  eco: EcosystemWithVersions
): LedgerTrustRegistry {
  const versions: LedgerTrustRegistryVersion[] = (eco.versions ?? []).map((v) => ({
    id: v.id,
    version: v.version,
    created: dateToIsoOrNull(v.created) ?? undefined,
    active_since: dateToIsoOrNull(v.activeSince),
    documents: (v.documents ?? []).map((d) => ({
      id: d.id,
      created: dateToIsoOrNull(d.created) ?? undefined,
      language: d.language ?? null,
      url: d.url ?? null,
      digest_sri: d.digestSri ?? null,
    })),
  }));

  return {
    id: eco.id,
    did: eco.did,
    corporation: eco.corporationId ?? null,
    corporation_id: eco.corporationId ?? null,
    created: dateToIsoOrNull(eco.created) ?? undefined,
    modified: dateToIsoOrNull(eco.modified) ?? undefined,
    archived: eco.archived ? (dateToIsoOrNull(eco.modified) ?? new Date().toISOString()) : null,
    language: eco.language ?? null,
    active_version: eco.activeVersion,
    versions,
  } as LedgerTrustRegistry;
}

export async function getTrustRegistry(
  trId: number,
  blockHeight?: number
): Promise<LedgerTrustRegistryResponse | null> {
  const rpcUrl = getRpcBaseUrl();
  if (!rpcUrl) {
    throw new Error(
      `[TR Height-Sync] Missing RPC base URL for gRPC ec query. Set RPC_ENDPOINT or Network.RPC.`
    );
  }

  const tmClient = await Tendermint37Client.connect(rpcUrl);
  try {
    const queryClient = new QueryClient(tmClient as any);
    const withHeight = typeof blockHeight === "number" && blockHeight > 0;
    const rpc = {
      request: async (
        service: string,
        method: string,
        data: Uint8Array
      ): Promise<Uint8Array> => {
        const path = `/${service}/${method}`;
        const response = await queryClient.queryAbci(
          path,
          data,
          withHeight ? blockHeight : undefined
        );
        return response.value;
      },
    };

    const ecQuery = new EcQueryClientImpl(rpc);
    const res = await ecQuery.GetEcosystem(
      QueryGetEcosystemRequest.fromPartial({
        id: trId,
        activeGfOnly: false,
        preferredLanguage: "",
      })
    );
    if (!res?.ecosystem) return null;
    return { trust_registry: mapEcosystemToLedgerTrustRegistry(res.ecosystem) };
  } finally {
    try {
      tmClient.disconnect();
    } catch {
      // ignore disconnect errors
    }
  }
}

export function isTrMessageType(type: string): boolean {
  return TR_MESSAGE_TYPES.has(type);
}

export function extractTrustRegistryIdFromContent(
  content: Record<string, unknown> | null | undefined
): number | null {
  if (!content || typeof content !== "object") return null;
  const candidates = [
    content.trust_registry_id,
    content.trustRegistryId,
    content.tr_id,
    content.trId,
    content.id,
    (content.trust_registry as Record<string, unknown> | undefined)?.id,
    (content.trust_registry as Record<string, unknown> | undefined)?.tr_id,
    (content.trust_registry as Record<string, unknown> | undefined)?.trId,
    (content.trustRegistry as Record<string, unknown> | undefined)?.id,
    (content.trustRegistry as Record<string, unknown> | undefined)?.tr_id,
    (content.trustRegistry as Record<string, unknown> | undefined)?.trId,
  ];
  for (const raw of candidates) {
    const n = Number(raw);
    if (Number.isInteger(n) && n > 0) return n;
  }
  return null;
}

export interface TxEventLike {
  type?: string;
  attributes?: Array<{ key?: string; value?: string }>;
}

function decodeAttributeValue(value: string | undefined): string {
  if (value == null || value === "") return "";
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(value) && value.length % 4 === 0) {
      return Buffer.from(value, "base64").toString("utf-8");
    }
  } catch {
    //
  }
  return value;
}

function decodeAttributeKey(key: string | undefined): string {
  if (key == null || key === "") return "";
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(key) && key.length % 4 === 0) {
      return Buffer.from(key, "base64").toString("utf-8");
    }
  } catch {
    //
  }
  return key;
}

function getDecodedEventAttributes(
  event: TxEventLike,
  decodeAttributes?: boolean
): Array<{ key: string; value: string }> {
  return (event.attributes ?? []).map((attr) => ({
    key: decodeAttributes ? decodeAttributeKey(attr.key) : (attr.key ?? ""),
    value: decodeAttributes ? decodeAttributeValue(attr.value) : (attr.value ?? ""),
  }));
}

const TR_EVENT_TYPES = new Set<string>([
  "create_trust_registry",
  "create_governance_framework_version",
  "create_governance_framework_document",
  "add_governance_framework_document",
  "increase_active_gf_version",
  "update_trust_registry",
  "archive_trust_registry",
]);

export function extractTrustRegistryIdsFromEvents(
  events: TxEventLike[],
  decodeAttributes?: boolean
): number[] {
  const ids: number[] = [];
  for (const ev of events) {
    const eventType = (ev.type ?? "").toLowerCase();
    if (!TR_EVENT_TYPES.has(eventType)) continue;

    const attrs = getDecodedEventAttributes(ev, decodeAttributes);
    if (attrs.length === 0) continue;

    for (const attr of attrs) {
      const keyLower = attr.key.toLowerCase();
      if (keyLower === "trust_registry_id") {
        const n = Number(attr.value);
        if (Number.isInteger(n) && n > 0) {
          ids.push(n);
        }
      }
    }
  }
  return [...new Set(ids)];
}

