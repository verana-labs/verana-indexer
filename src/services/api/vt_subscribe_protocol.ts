import { matchesMembership, parseSubscribeMembership } from "./api_shared";

export type VtTrustCore = {
  trusted: boolean;
  evaluatedAtTime: string;
  evaluatedAtBlock: number;
  expiresAtTime: string;
  corporationId: number | null;
};

export type CorporationChannelOptions = {
  include: boolean;
  includeDepositChanges: boolean;
};

export type ParticipationsChannelOptions = {
  include: boolean;
  includeWeightChanges: boolean;
  includeParticipantCounts: boolean;
  includeIssuedCredentials: boolean;
  includeVerifiedCredentials: boolean;
};

export type EcosystemsChannelOptions = {
  include: boolean;
  includeParticipantCounts: boolean;
  includeIssuedCredentials: boolean;
  includeVerifiedCredentials: boolean;
};

export type VtChannelOptions = {
  trust: boolean;
  ecsCredentials: boolean;
  presentations: boolean;
  services: boolean;
  corporation: CorporationChannelOptions;
  participations: ParticipationsChannelOptions;
  ecosystems: EcosystemsChannelOptions;
};

export type VtSubscribeControl = {
  action: "subscribe";
  dids: string[] | null;
  corporationId: number | null;
  channels: VtChannelOptions;
};

export type VtUnsubscribeControl = { action: "unsubscribe" };

export type VtControlMessage = VtSubscribeControl | VtUnsubscribeControl;

export type VtRawChange = {
  did: string;
  relatedDids: Set<string>;
  corporationIds: Set<number>;
  trust: VtTrustCore | null;
  corporation: { structural: boolean; deposit: boolean } | null;
  participations: {
    structural: boolean;
    weight: boolean;
    counts: boolean;
    issued: boolean;
    verified: boolean;
  } | null;
  ecosystems: { structural: boolean; counts: boolean; issued: boolean; verified: boolean } | null;
  content: boolean;
};

export type VtChange = {
  did: string;
  trust?: VtTrustCore;
  corporation?: boolean;
  participations?: boolean;
  ecsCredentials?: boolean;
  presentations?: boolean;
  services?: boolean;
  ecosystems?: boolean;
};

export type VtBlockEnvelope = {
  type: "block";
  block: number;
  blockTime: string;
  changes: VtChange[];
};

export type VtControlParseResult =
  | { ok: true; message: VtControlMessage }
  | { ok: false; error: string };

function boolOption(raw: unknown, key: string, fallback = false): boolean {
  if (raw === undefined || raw === null) return fallback;
  const v = (raw as Record<string, unknown>)[key];
  return v === true;
}

function parseCorporationChannel(raw: unknown): CorporationChannelOptions {
  if (raw === true) return { include: true, includeDepositChanges: false };
  if (raw && typeof raw === "object") {
    return { include: true, includeDepositChanges: boolOption(raw, "includeDepositChanges") };
  }
  return { include: false, includeDepositChanges: false };
}

function parseParticipationsChannel(raw: unknown): ParticipationsChannelOptions {
  const base = {
    includeWeightChanges: false,
    includeParticipantCounts: false,
    includeIssuedCredentials: false,
    includeVerifiedCredentials: false,
  };
  if (raw === true) return { include: true, ...base };
  if (raw && typeof raw === "object") {
    return {
      include: true,
      includeWeightChanges: boolOption(raw, "includeWeightChanges"),
      includeParticipantCounts: boolOption(raw, "includeParticipantCounts"),
      includeIssuedCredentials: boolOption(raw, "includeIssuedCredentials"),
      includeVerifiedCredentials: boolOption(raw, "includeVerifiedCredentials"),
    };
  }
  return { include: false, ...base };
}

function parseEcosystemsChannel(raw: unknown): EcosystemsChannelOptions {
  const base = {
    includeParticipantCounts: false,
    includeIssuedCredentials: false,
    includeVerifiedCredentials: false,
  };
  if (raw === true) return { include: true, ...base };
  if (raw && typeof raw === "object") {
    return {
      include: true,
      includeParticipantCounts: boolOption(raw, "includeParticipantCounts"),
      includeIssuedCredentials: boolOption(raw, "includeIssuedCredentials"),
      includeVerifiedCredentials: boolOption(raw, "includeVerifiedCredentials"),
    };
  }
  return { include: false, ...base };
}

function parseChannels(raw: unknown): VtChannelOptions {
  const map = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  return {
    trust: map.trust === true,
    ecsCredentials: map.ecsCredentials === true,
    presentations: map.presentations === true,
    services: map.services === true,
    corporation: parseCorporationChannel(map.corporation),
    participations: parseParticipationsChannel(map.participations),
    ecosystems: parseEcosystemsChannel(map.ecosystems),
  };
}

function hasAnyChannel(channels: VtChannelOptions): boolean {
  return (
    channels.trust ||
    channels.ecsCredentials ||
    channels.presentations ||
    channels.services ||
    channels.corporation.include ||
    channels.participations.include ||
    channels.ecosystems.include
  );
}

export function parseVtControlMessage(raw: string): VtControlParseResult {
  let json: unknown;
  try {
    json = JSON.parse(raw);
  } catch {
    return { ok: false, error: "Invalid JSON" };
  }

  if (!json || typeof json !== "object") {
    return { ok: false, error: "Control message must be an object" };
  }

  const action = (json as Record<string, unknown>).action;
  if (action !== "subscribe" && action !== "unsubscribe") {
    return { ok: false, error: "Unknown action. Expected 'subscribe' or 'unsubscribe'" };
  }

  if (action === "unsubscribe") {
    return { ok: true, message: { action: "unsubscribe" } };
  }

  const channels = parseChannels((json as Record<string, unknown>).channels);
  if (!hasAnyChannel(channels)) {
    return { ok: false, error: "'channels' must enable at least one channel" };
  }

  const base = parseSubscribeMembership(json as Record<string, unknown>);
  if (!base.ok) {
    return { ok: false, error: base.error };
  }

  return {
    ok: true,
    message: {
      action: "subscribe",
      dids: base.value.dids,
      corporationId: base.value.corporationId,
      channels,
    },
  };
}

export function projectVtChange(raw: VtRawChange, channels: VtChannelOptions): VtChange | null {
  const change: VtChange = { did: raw.did };
  let any = false;

  if (channels.trust && raw.trust) {
    change.trust = raw.trust;
    any = true;
  }

  if (channels.corporation.include) {
    const c = raw.corporation;
    const signal = Boolean(
      c && (c.structural || (channels.corporation.includeDepositChanges && c.deposit))
    );
    change.corporation = signal;
    if (signal) any = true;
  }

  if (channels.participations.include) {
    const p = raw.participations;
    const signal = Boolean(
      p &&
        (p.structural ||
          (channels.participations.includeWeightChanges && p.weight) ||
          (channels.participations.includeParticipantCounts && p.counts) ||
          (channels.participations.includeIssuedCredentials && p.issued) ||
          (channels.participations.includeVerifiedCredentials && p.verified))
    );
    change.participations = signal;
    if (signal) any = true;
  }

  if (channels.ecosystems.include) {
    const e = raw.ecosystems;
    const signal = Boolean(
      e &&
        (e.structural ||
          (channels.ecosystems.includeParticipantCounts && e.counts) ||
          (channels.ecosystems.includeIssuedCredentials && e.issued) ||
          (channels.ecosystems.includeVerifiedCredentials && e.verified))
    );
    change.ecosystems = signal;
    if (signal) any = true;
  }

  if (channels.ecsCredentials) {
    change.ecsCredentials = raw.content;
    if (raw.content) any = true;
  }
  if (channels.presentations) {
    change.presentations = raw.content;
    if (raw.content) any = true;
  }
  if (channels.services) {
    change.services = raw.content;
    if (raw.content) any = true;
  }

  return any ? change : null;
}

export function buildVtChangesEnvelope(
  block: number,
  blockTime: string,
  rawChanges: VtRawChange[],
  didFilter: Set<string> | null,
  corporationId: number | null,
  channels: VtChannelOptions
): VtBlockEnvelope {
  const changes: VtChange[] = [];
  for (const raw of rawChanges) {
    const inMembership = matchesMembership(didFilter, corporationId, {
      did: raw.did,
      relatedDids: raw.relatedDids,
      corporationIds: raw.corporationIds,
    });
    if (!inMembership) continue;
    const projected = projectVtChange(raw, channels);
    if (projected) changes.push(projected);
  }
  return { type: "block", block, blockTime, changes };
}
