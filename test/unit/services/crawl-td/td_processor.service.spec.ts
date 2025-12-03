import { ServiceBroker } from "moleculer";
import TrustDeposit from "../../../../src/models/trust_deposit";
import TrustDepositMessageProcessorService from "../../../../src/services/crawl-td/td_message.service";
import TrustDepositDatabaseService from "../../../../src/services/crawl-td/td_database.service";
import { SERVICE } from "../../../../src/common";

jest.mock("../../../../src/models/trust_deposit");
jest.mock("../../../../src/models/modules_params");

jest.mock("../../../../src/common/utils/db_connection", () => {
  const mockKnex: any = jest.fn(() => mockKnex);

  mockKnex.transaction = jest.fn(async (fn: any) => {
    return fn(mockKnex);
  });

  mockKnex.raw = jest.fn();

  mockKnex.where = jest.fn(() => mockKnex);
  mockKnex.first = jest.fn(() => mockKnex);
  mockKnex.insert = jest.fn(() => mockKnex);
  mockKnex.update = jest.fn(() => mockKnex);
  mockKnex.returning = jest.fn(() => mockKnex);

  return mockKnex;
});

describe("🧪 TrustDepositMessageProcessorService", () => {
  const broker = new ServiceBroker({ logger: false });
  const processorService = broker.createService(TrustDepositMessageProcessorService);
  const dbService = broker.createService(TrustDepositDatabaseService);

  beforeAll(async () => {
    await broker.start();
    processorService["trustDepositParams"] = {
      params: { trust_deposit_share_value: "100" },
    };
  });
  afterAll(() => broker.stop());

  it("✅ should process empty message list gracefully", async () => {
    const res = await broker.call(
      SERVICE.V1.TrustDepositMessageProcessorService.path +
      ".handleTrustDepositMessages",
      { trustDepositList: [] }
    );
    expect(res.success).toBe(true);
  });

  it("✅ should process unknown message type", async () => {
    const warnSpy = jest
      // @ts-ignore - logger injected by moleculer
      .spyOn((processorService as any).logger, "warn")
      .mockImplementation(() => undefined as unknown as any);
    await broker.call(
      SERVICE.V1.TrustDepositMessageProcessorService.path +
      ".handleTrustDepositMessages",
      {
        trustDepositList: [
          {
            type: "UNKNOWN",
            content: {
              account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
            },
            timestamp: new Date().toISOString(),
          },
        ],
      }
    );
    expect(warnSpy).toHaveBeenCalled();
  });

  it("✅ should slash trust deposit successfully", async () => {
    (TrustDeposit.query as any).mockReturnValue({
      findOne: jest.fn().mockResolvedValue({
        id: "1",
        account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
        amount: "1000",
        share: "10",
        slashed_deposit: "0",
        slash_count: "0",
      }),
      patchAndFetchById: jest.fn().mockResolvedValue({}),
    });

    const res = await broker.call(
      SERVICE.V1.TrustDepositDatabaseService.path + ".slash_trust_deposit",
      {
        account: "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
        slashed: "100",
        lastSlashed: new Date().toISOString(),
      }
    );
    expect(res.success).toBe(true);
  });

  it("❌ should throw error if slash called with invalid params", async () => {
    await expect(
      broker.call(
        SERVICE.V1.TrustDepositDatabaseService.path +
        ".slash_trust_deposit",
        {
          slashed: "100",
        }
      )
    ).rejects.toThrow("Parameters validation error!");
  });
});
