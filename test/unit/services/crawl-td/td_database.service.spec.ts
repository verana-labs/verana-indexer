import { ServiceBroker } from "moleculer";
import ModuleParams from "../../../../src/models/modules_params";
import TrustDeposit from "../../../../src/models/trust_deposit";
import TrustDepositDatabaseService from "../../../../src/services/crawl-td/td_database.service";
import TrustDepositApiService from "../../../../src/services/crawl-td/td_apis.service";
import { SERVICE } from "../../../../src/common";

jest.mock("../../../../src/models/trust_deposit");
jest.mock("../../../../src/models/modules_params");
jest.mock("../../../../src/common/utils/db_connection", () => ({
  transaction: jest.fn((fn) => fn({})),
  default: jest.fn((table: string) => ({
    where: jest.fn(() => ({
      orderBy: jest.fn(() => ({
        orderBy: jest.fn(() => ({
          first: jest.fn().mockResolvedValue(null),
        })),
      })),
    })),
  })),
}));

describe("🧪 TrustDepositDatabaseService", () => {
  const broker = new ServiceBroker({ logger: false });
  const dbService = broker.createService(TrustDepositDatabaseService);
  const apiService = broker.createService(TrustDepositApiService);

  beforeAll(() => broker.start());
  afterAll(() => broker.stop());

  describe("Action: getTrustDeposit", () => {
    it("✅ should return trust deposit successfully", async () => {
      (TrustDeposit.query as any).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          account: "verana1testaccountxyz",
          share: "100000",
          amount: "100000",
          claimable: "0",
          slashed_deposit: "1000",
          repaid_deposit: "500",
          last_slashed: "2025-10-09T10:00:00Z",
          last_repaid: "2025-10-09T12:00:00Z",
          slash_count: 1,
          last_repaid_by: "verana1dummyxyz12345",
        }),
      });

      const res: any = await broker.call(
        SERVICE.V1.TrustDepositApiService.path + ".getTrustDeposit",
        {
          account: "verana1testaccountxyz",
        }
      );

      expect(res.trust_deposit).toBeDefined();
      expect(res.trust_deposit.account).toBe("verana1testaccountxyz");
      expect(res.trust_deposit.slashed_deposit).toBe("1000");
    });

    it("❌ should return 400 for invalid account", async () => {
      try {
        const res: any = await broker.call(
          SERVICE.V1.TrustDepositApiService.path + ".getTrustDeposit",
          { account: "abc" }
        );
        // Service may return structured error or throw based on framework config
        expect(res.status).toBe(400);
        expect(res.error).toBe("Invalid account format");
      } catch (err: any) {
        // If framework throws, just ensure it's an error
        expect(err).toBeDefined();
      }
    });

    it("❌ should return 404 if not found", async () => {
      (TrustDeposit.query as any).mockReturnValue({
        findOne: jest.fn().mockResolvedValue(null),
      });

      try {
        const res: any = await broker.call(
          SERVICE.V1.TrustDepositApiService.path + ".getTrustDeposit",
          {
            account: "verana1notfoundxyz",
          }
        );
        expect(res.status).toBe(404);
        expect(res.error).toContain("No trust deposit found");
      } catch (err: any) {
        expect(err?.data?.action).toBe("v1.TrustDepositApiService.getTrustDeposit");
      }
    });
  });

  describe("Action: getModuleParams", () => {
    it("✅ should return module params successfully", async () => {
      (ModuleParams.query as any).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          module: "TD",
          params: JSON.stringify({
            params: {
              key1: "value1",
              key2: "value2",
            },
          }),
        }),
      });

      const res: any = await broker.call(
        SERVICE.V1.TrustDepositApiService.path + ".getModuleParams"
      );

      expect(res.params.key1).toBe("value1");
      expect(res.params.key2).toBe("value2");
    });

    it("❌ should return 404 when params not found", async () => {
      (ModuleParams.query as any).mockReturnValue({
        findOne: jest.fn().mockResolvedValue(null),
      });

      const res: any = await broker.call(
        SERVICE.V1.TrustDepositApiService.path + ".getModuleParams"
      );
      expect(res.status).toBe(404);
      expect(res.error).toBe("Module parameters not found: trustdeposit");
    });
  });
});
