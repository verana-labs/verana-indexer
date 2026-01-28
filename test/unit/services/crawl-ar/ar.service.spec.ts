import { ServiceBroker } from "moleculer";
import AccountReputationService from "../../../../src/services/crawl-ar/ar_api.service";
import { SERVICE } from "../../../../src/common";

jest.mock("../../../../src/common/utils/db_connection", () => jest.fn());

describe("AccountReputationService", () => {
    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService(AccountReputationService);
    const mockKnex = require("../../../../src/common/utils/db_connection");

    beforeAll(() => broker.start());
    afterAll(() => broker.stop());

    it("returns 400 for invalid account", async () => {
        const res: any = await broker.call(
            SERVICE.V1.AccountReputationService.path + ".getAccountReputation",
            { account: "abc" }
        );
        expect(res.code).toBe(400);
        expect(res.error).toBe("Invalid account format");
    });

    it("returns 404 when account not found", async () => {
        mockKnex.mockImplementationOnce((table: string) => ({
            where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(null) })),
        }));

        const res: any = await broker.call(
            SERVICE.V1.AccountReputationService.path + ".getAccountReputation",
            { account: "verana1notfoundxx" }
        );
        expect(res.code).toBe(404);
        expect(res.error).toContain("not found");
    });

    it("returns success even when no trust or schema data exists", async () => {

        mockKnex
            .mockImplementationOnce((table: string) => ({
                where: jest.fn(() => ({
                    first: jest.fn().mockResolvedValue({
                        address: "verana1emptydataxx",
                        spendable_balances: [{ denom: "uvna", amount: "999" }],
                        first_interaction_ts: null,
                    })
                })),
            }))
            .mockImplementationOnce((table: string) => ({
                where: jest.fn(() => ({
                    first: jest.fn().mockResolvedValue({
                        amount: "0",
                        slashed_deposit: "0",
                        repaid_deposit: "0",
                        slash_count: 0,
                    })
                })),
            }))
            .mockImplementationOnce((table: string) => ({
                joinRaw: jest.fn().mockReturnThis(),
                where: jest.fn().mockReturnThis(),
                andWhere: jest.fn().mockReturnThis(),
                select: jest.fn().mockResolvedValue([]),
            }));

        const res: any = await broker.call(
            SERVICE.V1.AccountReputationService.path + ".getAccountReputation",
            { account: "verana1emptydataxx" }
        );

        expect(res.account).toBe("verana1emptydataxx");
        expect(res.balance).toBe("999");
        expect(Array.isArray(res.trust_registries)).toBe(true);
    });

    it("returns success with slash and repay details included", async () => {

        mockKnex
            .mockImplementationOnce(() => ({
                where: jest.fn(() => ({
                    first: jest.fn().mockResolvedValue({
                        address: "verana1withdetailxx",
                        balances: [{ denom: "uvna", amount: "500" }],
                        first_interaction_ts: "2025-11-01T00:00:00Z",
                    })
                })),
            }))
            .mockImplementationOnce(() => ({
                where: jest.fn(() => ({
                    first: jest.fn().mockResolvedValue({
                        amount: "50",
                        slashed_deposit: "10",
                        repaid_deposit: "5",
                        slash_count: 2,
                    })
                })),
            }))
            .mockImplementationOnce(() => ({
                where: jest.fn(() => ({
                    select: jest.fn(() => ({
                        whereNotNull: jest.fn(() => Promise.resolve([
                            { slashed_amount: "10", slashed_ts: "t1" }
                        ]))
                    }))
                })),
            }))
            .mockImplementationOnce(() => ({
                where: jest.fn(() => ({
                    select: jest.fn(() => ({
                        whereNotNull: jest.fn(() => Promise.resolve([
                            { repaid_amount: "5", repaid_ts: "t2", repaid_by: "actor2" }
                        ]))
                    }))
                })),
            }))
            .mockImplementationOnce(() => ({
                joinRaw: jest.fn().mockReturnThis(),
                where: jest.fn().mockReturnThis(),
                andWhere: jest.fn().mockReturnThis(),
                select: jest.fn().mockResolvedValue([]),
            }));

        const res: any = await broker.call(
            SERVICE.V1.AccountReputationService.path + ".getAccountReputation",
            { account: "verana1withdetailxx", include_slash_details: true }
        );

        expect(res.account).toBe("verana1withdetailxx");
        expect(res.deposit).toBe(50);
        expect(res.slashed).toBe(10);
        expect(res.repaid).toBe(5);
        expect(Array.isArray(res.slashs)).toBe(true);
        expect(Array.isArray(res.repayments)).toBe(true);
        expect(res.slashs[0].slashed_amount).toBe("10");
        expect(res.repayments[0].repaid_amount).toBe("5");
    });
});
