import { ServiceBroker } from "moleculer";

const TrustResolutionOutcome = {
  VERIFIED: "verified",
  VERIFIED_TEST: "verified-test",
  NOT_TRUSTED: "not-trusted",
  INVALID: "invalid",
} as const;

jest.mock(
  "@verana-labs/verre",
  () => ({
    __esModule: true,
    resolveDID: jest.fn(async () => ({ verified: true })),
    verifyPermissions: jest.fn(async () => ({ verified: true })),
    PermissionType: { ISSUER: "ISSUER", VERIFIER: "VERIFIER" },
    TrustResolutionOutcome,
  }),
  { virtual: true }
);

jest.mock("../../../../src/models", () => ({
  __esModule: true,
  BlockCheckpoint: {
    query: () => ({
      where: jest.fn().mockReturnThis(),
      first: jest.fn().mockResolvedValue({ height: 10 }),
    }),
  },
}));

jest.mock("canonicalize", () => ({ __esModule: true, default: (obj: any) => JSON.stringify(obj) }), { virtual: true });

jest.mock("../../../../src/common/utils/db_connection", () => {
  const chain: any = {};
  chain.select = jest.fn(() => chain);
  chain.where = jest.fn(() => chain);
  chain.whereNull = jest.fn(() => chain);
  chain.whereNotNull = jest.fn(() => chain);
  chain.whereIn = jest.fn(() => chain);
  chain.orderBy = jest.fn(() => chain);
  chain.limit = jest.fn(async () => []);
  chain.first = jest.fn(async () => ({ height: 9 }));

  const knexMock: any = jest.fn((table?: string) => {
    if (table === "credential_schemas") {
      const listChain: any = {};
      listChain.select = jest.fn(() => listChain);
      listChain.whereNull = jest.fn(() => listChain);
      listChain.limit = jest.fn(async () => [{ id: 1 }]);

      const schemaChain: any = {};
      schemaChain.select = jest.fn(() => schemaChain);
      schemaChain.where = jest.fn(() => schemaChain);
      schemaChain.first = jest.fn(async () => ({
        json_schema: {
          digest_algorithm: "sha256",
          $id: "https://example.com/schemas/ecs-service/v1",
        },
      }));

      return {
        select: (...cols: any[]) => {
          if (cols.length === 1 && cols[0] === "id") return listChain;
          return schemaChain;
        },
      };
    }
    if (table === "block") return chain;
    return chain;
  });
  knexMock.raw = jest.fn(async () => ({ rows: [] }));
  return knexMock;
});

describe("TrustV1ApiService GET /resolve", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(async () => {
    const { TrustV1ApiService } = await import(
      "../../../../src/services/resolver/trust-api.service"
    );
    broker = new ServiceBroker({ logger: false });
    service = broker.createService(TrustV1ApiService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("summary returns trust summary + legacy fields and queries stored row at clamped height", async () => {
    const TrustResolve = await import("../../../../src/services/resolver/trust-resolve");
    jest.spyOn(TrustResolve, "getTrustResultLatestByDidAtOrBeforeHeight").mockResolvedValue({
      did: "did:verana:test123",
      height: 10,
      resolve_result: { verified: true, outcome: TrustResolutionOutcome.VERIFIED },
      issuer_auth: { verified: true },
      verifier_auth: { verified: false },
      ecosystem_participant: { verified: true },
      created_at: "2026-01-01T00:00:00Z",
    } as any);

    const ctx: any = {
      params: { did: "did:verana:test123", detail: "summary", at: "10" },
      meta: {},
    };
    const res = await service.resolve(ctx);

    expect(TrustResolve.getTrustResultLatestByDidAtOrBeforeHeight).toHaveBeenCalledWith(
      "did:verana:test123",
      10
    );
    expect(ctx.meta.$statusCode).toBe(200);
    expect(res.trust_status).toBe("TRUSTED");
    expect(res.production).toBe(true);
    expect(res.evaluated_at_block).toBe(10);
    expect(res.resolve_result).toBeUndefined();
  });

  it("detail=full returns normative Q1 shape + credential arrays from stored Verre", async () => {
    const TrustResolve = await import("../../../../src/services/resolver/trust-resolve");
    jest.spyOn(TrustResolve, "getTrustResultLatestByDidAtOrBeforeHeight").mockResolvedValue({
      did: "did:verana:test123",
      height: 10,
      resolve_result: {
        verified: true,
        outcome: TrustResolutionOutcome.VERIFIED,
        credentials: [{ result: "VALID" }],
        failedCredentials: [{ id: "x", error: "bad" }],
      },
      issuer_auth: { verified: true },
      verifier_auth: { verified: false },
      ecosystem_participant: { verified: true },
      created_at: "2026-01-01T00:00:00Z",
    } as any);

    const ctx: any = {
      params: { did: "did:verana:test123", detail: "full", at: "10" },
      meta: {},
    };
    const res = await service.resolve(ctx);

    expect(ctx.meta.$statusCode).toBe(200);
    expect(res.did).toBe("did:verana:test123");
    expect(res.evaluated_at_block).toBe(10);
    expect(res.trust_status).toBe("TRUSTED");
    expect(res.resolve_result).toBeUndefined();
    expect(res.credentials).toEqual([
      expect.objectContaining({
        result: "VALID",
        presented_by: "did:verana:test123",
      }),
    ]);
    expect(res.failed_credentials).toEqual([{ id: "x", error: "bad" }]);
    expect(res.issuer_auth).toBeUndefined();
  });

  it("default detail is full and computes digestSri when schema digest algorithm exists", async () => {
    const TrustResolve = await import("../../../../src/services/resolver/trust-resolve");
    jest.spyOn(TrustResolve, "getTrustResultLatestByDidAtOrBeforeHeight").mockResolvedValue({
      did: "did:verana:test123",
      height: 10,
      resolve_result: {
        verified: true,
        outcome: TrustResolutionOutcome.VERIFIED,
        credentials: [
          {
            result: "VALID",
            vtjscId: "https://example.com/schemas/ecs-service/v1",
            credential: { a: 1, b: 2 },
          },
        ],
        failedCredentials: [],
      },
      created_at: "2026-01-01T00:00:00Z",
    } as any);

    const ctx: any = {
      params: { did: "did:verana:test123" }, // no detail => default full
      meta: {},
    };
    const res = await service.resolve(ctx);
    expect(ctx.meta.$statusCode).toBe(200);
    expect(res.credentials?.[0]).toEqual(
      expect.objectContaining({
        result: "VALID",
        vtjsc_id: "https://example.com/schemas/ecs-service/v1",
      })
    );
    expect(res.credentials?.[0].digest_sri).toMatch(/^sha256-/);
  });

  it("accepts at as ISO datetime by mapping to block height", async () => {
    const TrustResolve = await import("../../../../src/services/resolver/trust-resolve");
    jest.spyOn(TrustResolve, "getTrustResultLatestByDidAtOrBeforeHeight").mockResolvedValue({
      did: "did:verana:test123",
      height: 9,
      resolve_result: { verified: true, outcome: TrustResolutionOutcome.VERIFIED },
      created_at: "2026-01-01T00:00:00Z",
    } as any);

    const ctx: any = {
      params: { did: "did:verana:test123", at: "2026-01-01T00:00:00Z" },
      meta: {},
    };
    const res = await service.resolve(ctx);
    expect(ctx.meta.$statusCode).toBe(200);
    expect(res.evaluated_at_block).toBe(9);
  });
});

describe("TrustV1ApiService POST /refresh", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(async () => {
    const { TrustV1ApiService } = await import("../../../../src/services/resolver/trust-api.service");
    broker = new ServiceBroker({ logger: false });
    service = broker.createService(TrustV1ApiService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("returns { did, result: ok } when refresh is triggered", async () => {
    const TrustResolve = await import("../../../../src/services/resolver/trust-resolve");
    jest.spyOn(TrustResolve, "resolveTrustForDidAtHeight").mockResolvedValue(undefined);

    const ctx: any = { params: { did: "did:verana:test123" }, meta: {} };
    const res = await service.refresh(ctx);
    expect(ctx.meta.$statusCode).toBe(200);
    expect(res).toEqual({ did: "did:verana:test123", result: "ok" });
  });
});
