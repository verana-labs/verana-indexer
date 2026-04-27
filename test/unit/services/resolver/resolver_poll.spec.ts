import { ServiceBroker } from "moleculer";
import { SERVICE } from "../../../../src/common";

jest.mock("../../../../src/services/manager/indexer_status.manager", () => ({
  indexerStatusManager: {
    isCrawlingActive: () => true,
    getStatus: () => ({ isRunning: true, isCrawling: true }),
  },
}));

jest.mock(
  "@verana-labs/verre",
  () => ({
    __esModule: true,
    InMemoryCache: class InMemoryCache {
      constructor(_ttlMs: number) {}
    },
    resolveDID: jest.fn(async () => ({ verified: true })),
  }),
  { virtual: true }
);

jest.mock("../../../../src/config.json", () => ({
  __esModule: true,
  default: {
    httpBatchRequest: {
      batchSizeLimit: 20,
      dispatchMilisecond: 20,
    },
    resolver: {
      enabled: true,
      blocksPerCall: 1,
    },
  },
}));

const mockBlockCheckpointQuery = {
  where: jest.fn(),
  first: jest.fn(),
  insert: jest.fn(),
  onConflict: jest.fn(),
  merge: jest.fn(),
};

jest.mock("../../../../src/models", () => ({
  __esModule: true,
  BlockCheckpoint: {
    query: () => mockBlockCheckpointQuery,
  },
}));

jest.mock("../../../../src/common/utils/db_connection", () => {
  const chain: any = {};
  chain.select = jest.fn(() => chain);
  chain.where = jest.fn(() => chain);
  chain.whereNotNull = jest.fn(() => chain);
  chain.andWhere = jest.fn(() => chain);
  chain.orderBy = jest.fn(() => chain);
  chain.limit = jest.fn(() => Promise.resolve([]));
  chain.first = jest.fn(() => Promise.resolve({ height: 200 }));

  const knexMock: any = jest.fn((_table?: string) => chain);
  knexMock.raw = jest.fn(async () => ({ rows: [] }));
  return knexMock;
});

jest.mock("../../../../src/services/resolver/trust-resolve", () => {
  const actual = jest.requireActual<typeof import("../../../../src/services/resolver/trust-resolve")>(
    "../../../../src/services/resolver/trust-resolve"
  );
  return {
    ...actual,
    getResolverTuning: jest.fn(async () => ({
      isReindexing: true,
      pollIntervalMs: 100,
      blocksPerCall: 1,
      didConcurrency: 8,
      maxDidsPerBlock: 500,
    })),
    findHeightsWithTrustModuleMessages: jest.fn(async (from: number, to: number) => {
      const out: number[] = [];
      for (let h = from + 1; h <= to; h++) out.push(h);
      return out;
    }),
    trustTxPrefilterEnabled: jest.fn(() => true),
    resolveTrustForBlock: jest.fn(async (_height: number) => undefined),
  };
});

describe("ResolverPollService", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBlockCheckpointQuery.where.mockReturnValue(mockBlockCheckpointQuery);
    mockBlockCheckpointQuery.insert.mockReturnValue(mockBlockCheckpointQuery);
    mockBlockCheckpointQuery.onConflict.mockReturnValue(mockBlockCheckpointQuery);
    mockBlockCheckpointQuery.merge.mockResolvedValue(undefined);
  });

  describe("pollOnce", () => {
    it("runs pipeline for new heights, advances checkpoint, emits block-resolved", async () => {
      const { ResolverPollService } = await import("../../../../src/services/resolver/resolver-poll.service");
      const TrustResolve = await import("../../../../src/services/resolver/trust-resolve");

      mockBlockCheckpointQuery.first.mockResolvedValue({ height: 9 });

      const broker = { call: jest.fn().mockResolvedValue(undefined) } as any;

      const svc = Object.create(ResolverPollService.methods) as any;
      svc.broker = broker;
      svc.logger = { debug: jest.fn(), warn: jest.fn(), info: jest.fn() };

      await svc.pollOnce();

      expect(TrustResolve.resolveTrustForBlock).toHaveBeenCalledWith(10);
      expect(mockBlockCheckpointQuery.insert).toHaveBeenCalled();
      expect(mockBlockCheckpointQuery.merge).toHaveBeenCalled();
      expect(broker.call).toHaveBeenCalledWith(
        `${SERVICE.V1.IndexerEventsService.path}.broadcastBlockResolved`,
        expect.objectContaining({ height: 10, timestamp: expect.any(String) })
      );
    });
  });

  describe("handleTrustResolveJob", () => {
    it("invokes block resolution and emits block-resolved", async () => {
      const [{ ResolverPollService }, TrustResolve] = await Promise.all([
        import("../../../../src/services/resolver/resolver-poll.service"),
        import("../../../../src/services/resolver/trust-resolve"),
      ]);

      mockBlockCheckpointQuery.first.mockResolvedValue({ height: 1 });

      const broker = new ServiceBroker({ logger: false });
      broker.createService({
        name: SERVICE.V1.IndexerEventsService.key,
        version: 1,
        actions: {
          broadcastBlockResolved: async () => ({ ok: true }),
        },
      });
      const resolver: any = broker.createService(ResolverPollService);

      await broker.start();
      await resolver.actions.handleTrustResolveJob({ height: 77 });
      await broker.stop();

      expect(TrustResolve.resolveTrustForBlock).toHaveBeenCalledWith(77);
    });
  });
});
