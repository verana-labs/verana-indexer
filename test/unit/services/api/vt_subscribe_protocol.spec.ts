import {
  parseVtControlMessage,
  projectVtChange,
  buildVtChangesEnvelope,
  type VtChannelOptions,
  type VtRawChange,
} from "../../../../src/services/api/vt_subscribe_protocol";

function rawChange(overrides: Partial<VtRawChange> & { did: string }): VtRawChange {
  return {
    relatedDids: new Set(),
    corporationIds: new Set(),
    trust: null,
    corporation: null,
    participations: null,
    ecosystems: null,
    content: false,
    ...overrides,
  };
}

function channels(overrides: Partial<VtChannelOptions> = {}): VtChannelOptions {
  return {
    trust: false,
    ecsCredentials: false,
    presentations: false,
    services: false,
    corporation: { include: false, includeDepositChanges: false },
    participations: {
      include: false,
      includeWeightChanges: false,
      includeParticipantCounts: false,
      includeIssuedCredentials: false,
      includeVerifiedCredentials: false,
    },
    ecosystems: {
      include: false,
      includeParticipantCounts: false,
      includeIssuedCredentials: false,
      includeVerifiedCredentials: false,
    },
    ...overrides,
  };
}

describe("parseVtControlMessage", () => {
  it("parses a subscribe with boolean channels and defaults sub-options to false", () => {
    const res = parseVtControlMessage(
      JSON.stringify({ action: "subscribe", channels: { trust: true, participations: true } })
    );
    expect(res.ok).toBe(true);
    if (!res.ok || res.message.action !== "subscribe") throw new Error("expected subscribe");
    expect(res.message.dids).toBeNull();
    expect(res.message.corporationId).toBeNull();
    expect(res.message.channels.trust).toBe(true);
    expect(res.message.channels.participations).toMatchObject({
      include: true,
      includeWeightChanges: false,
      includeParticipantCounts: false,
    });
    expect(res.message.channels.ecosystems.include).toBe(false);
  });

  it("parses participations sub-options", () => {
    const res = parseVtControlMessage(
      JSON.stringify({
        action: "subscribe",
        channels: { participations: { includeWeightChanges: true, includeIssuedCredentials: true } },
      })
    );
    expect(res.ok).toBe(true);
    if (!res.ok || res.message.action !== "subscribe") throw new Error("expected subscribe");
    expect(res.message.channels.participations).toMatchObject({
      include: true,
      includeWeightChanges: true,
      includeParticipantCounts: false,
      includeIssuedCredentials: true,
      includeVerifiedCredentials: false,
    });
  });

  it("rejects a subscribe with no enabled channels", () => {
    const res = parseVtControlMessage(JSON.stringify({ action: "subscribe", channels: {} }));
    expect(res.ok).toBe(false);
  });

  it("rejects an invalid DID", () => {
    const res = parseVtControlMessage(
      JSON.stringify({ action: "subscribe", channels: { trust: true }, dids: ["not-a-did"] })
    );
    expect(res.ok).toBe(false);
  });

  it("rejects a non-positive corporationId", () => {
    const res = parseVtControlMessage(
      JSON.stringify({ action: "subscribe", channels: { trust: true }, corporationId: 0 })
    );
    expect(res.ok).toBe(false);
  });

  it("parses unsubscribe", () => {
    const res = parseVtControlMessage(JSON.stringify({ action: "unsubscribe" }));
    expect(res.ok).toBe(true);
    if (!res.ok) throw new Error("expected ok");
    expect(res.message.action).toBe("unsubscribe");
  });
});

describe("projectVtChange (sub-option gating)", () => {
  it("includes the inline trust object only when subscribed", () => {
    const core = {
      trusted: true,
      evaluatedAtTime: "2026-05-11T13:00:05Z",
      evaluatedAtBlock: 10,
      expiresAtTime: "2026-05-12T13:00:05Z",
      corporationId: 42,
    };
    const raw = rawChange({ did: "did:web:a", trust: core });
    expect(projectVtChange(raw, channels({ trust: true }))?.trust).toEqual(core);
    expect(projectVtChange(raw, channels({ ecsCredentials: true }))).toBeNull();
  });

  it("suppresses a weight-only participations change unless includeWeightChanges is set", () => {
    const raw = rawChange({
      did: "did:web:a",
      participations: { structural: false, weight: true, counts: false, issued: false, verified: false },
    });
    expect(projectVtChange(raw, channels({ participations: { include: true, includeWeightChanges: false, includeParticipantCounts: false, includeIssuedCredentials: false, includeVerifiedCredentials: false } }))).toBeNull();
    const enabled = projectVtChange(
      raw,
      channels({ participations: { include: true, includeWeightChanges: true, includeParticipantCounts: false, includeIssuedCredentials: false, includeVerifiedCredentials: false } })
    );
    expect(enabled?.participations).toBe(true);
  });

  it("always signals a structural participations change regardless of sub-options", () => {
    const raw = rawChange({
      did: "did:web:a",
      participations: { structural: true, weight: false, counts: false, issued: false, verified: false },
    });
    expect(projectVtChange(raw, channels({ participations: { include: true, includeWeightChanges: false, includeParticipantCounts: false, includeIssuedCredentials: false, includeVerifiedCredentials: false } }))?.participations).toBe(true);
  });

  it("gates corporation deposit changes behind includeDepositChanges", () => {
    const raw = rawChange({ did: "did:web:a", corporation: { structural: false, deposit: true } });
    expect(projectVtChange(raw, channels({ corporation: { include: true, includeDepositChanges: false } }))).toBeNull();
    expect(
      projectVtChange(raw, channels({ corporation: { include: true, includeDepositChanges: true } }))?.corporation
    ).toBe(true);
  });

  it("maps the content flag to ecsCredentials/presentations/services", () => {
    const raw = rawChange({ did: "did:web:a", content: true });
    const out = projectVtChange(raw, channels({ ecsCredentials: true, services: true }));
    expect(out).toMatchObject({ ecsCredentials: true, services: true });
    expect(out).not.toHaveProperty("presentations");
  });
});

describe("buildVtChangesEnvelope (membership filter)", () => {
  const core = {
    trusted: true,
    evaluatedAtTime: "2026-05-11T13:00:05Z",
    evaluatedAtBlock: 10,
    expiresAtTime: "2026-05-12T13:00:05Z",
    corporationId: 7,
  };

  it("delivers all changed DIDs to a wildcard subscriber", () => {
    const raws = [
      rawChange({ did: "did:web:a", trust: core }),
      rawChange({ did: "did:web:b", trust: core }),
    ];
    const env = buildVtChangesEnvelope(10, "t", raws, null, null, channels({ trust: true }));
    expect(env.type).toBe("block");
    expect(env.changes.map((c) => c.did).sort()).toEqual(["did:web:a", "did:web:b"]);
  });

  it("filters by DID and matches the validator one-hop relatedDids", () => {
    const raws = [
      rawChange({ did: "did:web:leaf", trust: core, relatedDids: new Set(["did:web:validator"]) }),
      rawChange({ did: "did:web:other", trust: core }),
    ];
    const env = buildVtChangesEnvelope(10, "t", raws, new Set(["did:web:validator"]), null, channels({ trust: true }));
    expect(env.changes.map((c) => c.did)).toEqual(["did:web:leaf"]);
  });

  it("filters by corporationId", () => {
    const raws = [
      rawChange({ did: "did:web:a", trust: core, corporationIds: new Set([42]) }),
      rawChange({ did: "did:web:b", trust: core, corporationIds: new Set([99]) }),
    ];
    const env = buildVtChangesEnvelope(10, "t", raws, null, 42, channels({ trust: true }));
    expect(env.changes.map((c) => c.did)).toEqual(["did:web:a"]);
  });

  it("emits an empty changes array as a heartbeat", () => {
    const env = buildVtChangesEnvelope(10, "t", [], null, null, channels({ trust: true }));
    expect(env.changes).toEqual([]);
  });
});
