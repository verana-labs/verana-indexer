import { ServiceBroker } from "moleculer";
import { Transaction } from "../../../../src/models/transaction";
import TrustDeposit from "../../../../src/models/trust_deposit";
import TrustDepositDatabaseService from "../../../../src/services/crawl-td/td_database.service";
import { SERVICE } from "../../../../src/common";

jest.mock("../../../../src/models/trust_deposit.ts");
jest.mock("../../../../src/models/transaction");
jest.mock("../../../../src/models/block_checkpoint", () => {
  const queryChain = {
    findOne: jest.fn().mockResolvedValue({
      id: "cp1",
      job_name: "crawl:trust-deposit",
      height: 0,
    }),
    insertAndFetch: jest.fn().mockResolvedValue({
      id: "cp1",
      job_name: "crawl:trust-deposit",
      height: 0,
    }),
    patch: jest.fn().mockReturnValue({ where: jest.fn().mockResolvedValue(1) }),
    where: jest.fn().mockReturnValue({}),
    orderBy: jest.fn().mockReturnValue({ first: jest.fn() }),
    first: jest.fn(),
    select: jest.fn().mockReturnValue({
      where: jest.fn().mockReturnValue({ first: jest.fn() }),
    }),
  } as any;
  return {
    BlockCheckpoint: {
      query: jest.fn(() => queryChain),
      getCheckpoint: jest
        .fn()
        .mockResolvedValue([
          0,
          0,
          { id: "cp1", job_name: "crawl:trust-deposit", height: 0 },
        ]),
    },
  };
});

jest.mock("../../../../src/common/utils/db_connection", () => {
  const mockKnex: any = jest.fn((table?: string) => {
    if (table === "trust_deposit_history") {
      const historyChain: any = {
        where: jest.fn(() => historyChain),
        first: jest.fn().mockResolvedValue(null),
        insert: jest.fn().mockResolvedValue(1),
        update: jest.fn().mockResolvedValue(1),
      };
      return historyChain;
    }
    return mockKnex;
  });

  mockKnex.transaction = jest.fn(async (fn: any) => {
    return fn(mockKnex);
  });

  mockKnex.raw = jest.fn();
  mockKnex.where = jest.fn(() => mockKnex);
  mockKnex.first = jest.fn(() => mockKnex);
  mockKnex.insert = jest.fn(() => ({
    returning: jest.fn().mockResolvedValue([
      {
        account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
        amount: 100,
        share: 10,
        claimable: 0,
      },
    ]),
  }));

  mockKnex.update = jest.fn(() => mockKnex);

  return mockKnex;
});

describe("🧪 TrustDepositDatabaseService", () => {
  const broker = new ServiceBroker({ logger: false });
  const service = broker.createService(TrustDepositDatabaseService);

  beforeAll(async () => {
    (Transaction as any).query = jest.fn(() => {
      const chain: any = {
        where: jest.fn(() => chain),
        orderBy: jest.fn(() => chain),
        limit: jest.fn().mockResolvedValue([]),
      };
      return chain;
    });

    // Default TrustDeposit.query chain
    (TrustDeposit as any).query = jest.fn(() => ({
      findOne: jest.fn().mockResolvedValue(null),
      insertAndFetch: jest.fn().mockResolvedValue({}),
      patchAndFetchById: jest.fn().mockResolvedValue({}),
    }));

    await broker.start();
  });
  afterAll(() => broker.stop());

  it("✅ should process transactions with adjust_trust_deposit event", async () => {
    // Mock TrustDeposit.query to return null (no existing record)
    (TrustDeposit.query as any).mockReturnValue({
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue({}),
    });

    const result = await broker.call(
      SERVICE.V1.TrustDepositDatabaseService.path + ".adjustTrustDeposit",
      {
        account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
        newAmount: BigInt(100),
        newShare: BigInt(10),
        newClaimable: BigInt(0),
      }
    );

    expect(result.success).toBe(true);
    expect(result.message).toBe("Inserted new trust deposit record");
    expect(TrustDeposit.query).toBeDefined();
  });

  it("✅ should update existing trust deposit", async () => {
    (TrustDeposit.query as any).mockReturnValue({
      findOne: jest.fn().mockResolvedValue({
        id: 1,
        account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
        amount: 100,
        share: 10,
        claimable: 0,
        slashed_deposit: 0,
        repaid_deposit: 0,
        slash_count: 0,
      }),
      patchAndFetchById: jest.fn().mockResolvedValue({}),
    });

    const result = await broker.call(
      SERVICE.V1.TrustDepositDatabaseService.path + ".adjustTrustDeposit",
      {
        account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
        newAmount: BigInt(50),
        newShare: BigInt(5),
        newClaimable: BigInt(0),
      }
    );

    expect(result.success).toBe(true);
    expect(result.message).toBe("Updated existing trust deposit record");
    expect(TrustDeposit.query).toBeDefined();
  });
});
