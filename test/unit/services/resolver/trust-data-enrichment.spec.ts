jest.mock("../../../../src/common/utils/db_connection", () => {
  const chain: any = {};
  chain.select = jest.fn(() => chain);
  chain.whereIn = jest.fn(() => chain);
  chain.andWhere = jest.fn(() => chain);
  chain.distinctOn = jest.fn(() => chain);
  chain.orderBy = jest.fn(() => chain);
  chain.then = (resolve: any, reject: any) => Promise.resolve([]).then(resolve, reject);

  const knexMock: any = jest.fn(() => chain);
  knexMock.client = { config: { client: "pg" } };
  return knexMock;
});

jest.mock("../../../../src/services/resolver/trust-resolve", () => ({
  __esModule: true,
  getTrustEvaluationTtlSeconds: jest.fn(() => 3600),
  extractQ1CredentialArrays: jest.fn(() => ({ credentials: [], failedCredentials: [] })),
  buildTrustSummaryFromStoredRow: jest.fn((args: any) => ({
    did: args.did,
    trust_status: "UNTRUSTED",
    production: false,
    evaluated_at: "2026-01-01T00:00:00Z",
    evaluated_at_block: args.evaluatedAtBlock,
    expires_at: "2026-01-01T01:00:00Z",
  })),
}));

import { enrichTrustDataDeep, parseTrustDataMode } from "../../../../src/services/resolver/trust-data-enrichment";

describe("trust-data-enrichment", () => {
  it("parseTrustDataMode accepts null/none/summary/full and rejects invalid values", () => {
    expect(parseTrustDataMode(undefined)).toEqual({ ok: true, mode: "none" });
    expect(parseTrustDataMode(null)).toEqual({ ok: true, mode: "none" });
    expect(parseTrustDataMode("")).toEqual({ ok: true, mode: "none" });
    expect(parseTrustDataMode("null")).toEqual({ ok: true, mode: "none" });
    expect(parseTrustDataMode("none")).toEqual({ ok: true, mode: "none" });
    expect(parseTrustDataMode("summary")).toEqual({ ok: true, mode: "summary" });
    expect(parseTrustDataMode("FULL")).toEqual({ ok: true, mode: "full" });
    expect(parseTrustDataMode("wat")).toEqual({
      ok: false,
      message: 'Invalid "trust_data". Allowed values: null, summary, full',
    });
  });

  it("is a strict no-op when mode is none", async () => {
    const payload: any = { did: "did:example:test", nested: { did: "did:example:other" } };
    const out = await enrichTrustDataDeep(payload, "none");
    expect(out).toBe(payload);
    expect(payload).not.toHaveProperty("trust_data");
    expect(payload.nested).not.toHaveProperty("trust_data");
  });

  it("injects trust_dataonly for did:* strings (and never mutates input)", async () => {
    const payload: any = {
      did: "did:example:test",
      nested: { did: "not-a-did" },
      alsoNested: { did: null },
    };

    const out = await enrichTrustDataDeep(payload, "summary", 123);

    expect(out).not.toBe(payload);
    expect(payload).not.toHaveProperty("trust_data");
    expect(payload.nested).not.toHaveProperty("trust_data");
    expect(payload.alsoNested).not.toHaveProperty("trust_data");

    expect((out as any).trust_data).toBeNull();
    expect((out as any).nested).not.toHaveProperty("trust_data");
    expect((out as any).alsoNested).not.toHaveProperty("trust_data");
  });
});

