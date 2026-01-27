// tests/trustRegistryDatabaseService.spec.ts
import { ServiceBroker } from "moleculer";
import TrustRegistryDatabaseService from "../../../../src/services/crawl-tr/tr_database.service";
import { TrustRegistry } from "../../../../src/models/trust_registry";
import ApiResponder from "../../../../src/common/utils/apiResponse";

jest.mock("../../../../src/models/trust_registry");
jest.mock("../../../../src/common/utils/apiResponse");
jest.mock("../../../../src/services/crawl-perm/perm_state_utils", () => ({
    calculatePermState: jest.fn().mockReturnValue("ACTIVE"),
}));
jest.mock("../../../../src/services/crawl-tr/tr_stats", () => ({
    calculateTrustRegistryStats: jest.fn(),
}));

jest.mock("../../../../src/common/utils/db_connection", () => {
    const mockQuery: any = jest.fn(() => mockQuery);
    mockQuery.whereIn = jest.fn(() => mockQuery);
    mockQuery.select = jest.fn(() => mockQuery);
    mockQuery.where = jest.fn(() => mockQuery);
    mockQuery.orderBy = jest.fn(() => mockQuery);
    mockQuery.limit = jest.fn(() => mockQuery);
    mockQuery.first = jest.fn(() => mockQuery);
    return mockQuery;
});

describe("TrustRegistryDatabaseService", () => {
    let broker: ServiceBroker;
    let service: TrustRegistryDatabaseService;

    beforeAll(() => {
        broker = new ServiceBroker({ nodeID: "test-node", logger: false });
        service = new TrustRegistryDatabaseService(broker);
        broker.createService(TrustRegistryDatabaseService as any);
    });

    afterAll(() => broker.stop());
    beforeEach(() => jest.clearAllMocks());

    describe("getTrustRegistry", () => {
        it("should return error if TR not found", async () => {
            (TrustRegistry.query as any).mockReturnValueOnce({
                findById: jest.fn().mockReturnValueOnce({
                    withGraphFetched: jest.fn().mockResolvedValueOnce(undefined),
                }),
            });

            const ctx: any = { params: { tr_id: 1 } };
            await service.getTrustRegistry(ctx);
            expect(ApiResponder.error).toHaveBeenCalledWith(ctx, "TrustRegistry with id 1 not found", 404);
        });

        it("should return TR with filtered documents for preferred language", async () => {
            const mockTR = {
                toJSON: jest.fn().mockReturnValue({
                    id: 1,
                    governanceFrameworkVersions: [
                        {
                            active_since: "2025-01-01",
                            documents: [
                                { language: "en", url: "doc1" },
                                { language: "fr", url: "doc2" },
                            ],
                        },
                    ],
                }),
            };

            (TrustRegistry.query as any).mockReturnValueOnce({
                findById: jest.fn().mockReturnValueOnce({
                    withGraphFetched: jest.fn().mockResolvedValueOnce(mockTR),
                }),
            });

            const { calculateTrustRegistryStats } = require("../../../../src/services/crawl-tr/tr_stats");
            (calculateTrustRegistryStats as jest.Mock).mockResolvedValue({
                participants: 0,
                active_schemas: 0,
                archived_schemas: 0,
                weight: "0",
                issued: "0",
                verified: "0",
                ecosystem_slash_events: 0,
                ecosystem_slashed_amount: "0",
                ecosystem_slashed_amount_repaid: "0",
                network_slash_events: 0,
                network_slashed_amount: "0",
                network_slashed_amount_repaid: "0",
            });

            const ctx: any = { params: { tr_id: 1, preferred_language: "en", active_gf_only: "true" } };
            await service.getTrustRegistry(ctx);

            expect(ApiResponder.success).toHaveBeenCalled();
            let data = (ApiResponder.success as jest.Mock).mock.calls[0][1];
            data = data?.trust_registry
            expect(data.versions[0].documents).toEqual([{ language: "en", url: "doc1" }]);
        });
    });

    describe("listTrustRegistries", () => {
        it("should return error for invalid response_max_size", async () => {
            const ctx: any = { params: { response_max_size: 2000 } };
            await service.listTrustRegistries(ctx);
            expect(ApiResponder.error).toHaveBeenCalledWith(ctx, "response_max_size must be between 1 and 1024", 400);
        });

        it("should return filtered list with preferred language and active_gf_only", async () => {
            const mockTR = {
                toJSON: jest.fn().mockReturnValue({
                    id: 1,
                    governanceFrameworkVersions: [
                        {
                            active_since: "2025-01-01",
                            documents: [
                                { language: "en", url: "doc1" },
                                { language: "fr", url: "doc2" },
                            ],
                        },
                    ],
                }),
            };

            // Complete mock for chained calls
            const mockQuery: any = {
                withGraphFetched: jest.fn().mockReturnThis(),
                where: jest.fn().mockReturnThis(),
                orderBy: jest.fn().mockReturnThis(),
                limit: jest.fn().mockResolvedValue([mockTR]),
            };
            (TrustRegistry.query as any).mockReturnValue(mockQuery);

            const knex = require("../../../../src/common/utils/db_connection");
            (knex.select as jest.Mock).mockResolvedValueOnce([
                {
                    id: 1,
                    participants: 0,
                    active_schemas: 0,
                    archived_schemas: 0,
                    weight: "0",
                    issued: "0",
                    verified: "0",
                    ecosystem_slash_events: 0,
                    ecosystem_slashed_amount: "0",
                    ecosystem_slashed_amount_repaid: "0",
                    network_slash_events: 0,
                    network_slashed_amount: "0",
                    network_slashed_amount_repaid: "0",
                },
            ]);

            const { calculateTrustRegistryStats } = require("../../../../src/services/crawl-tr/tr_stats");
            (calculateTrustRegistryStats as jest.Mock).mockResolvedValue({
                participants: 0,
                active_schemas: 0,
                archived_schemas: 0,
                weight: "0",
                issued: "0",
                verified: "0",
                ecosystem_slash_events: 0,
                ecosystem_slashed_amount: "0",
                ecosystem_slashed_amount_repaid: "0",
                network_slash_events: 0,
                network_slashed_amount: "0",
                network_slashed_amount_repaid: "0",
            });

            const ctx: any = {
                params: { active_gf_only: "true", preferred_language: "fr", response_max_size: 2 },
            };
            await service.listTrustRegistries(ctx);

            expect(ApiResponder.success).toHaveBeenCalled();
            let data = (ApiResponder.success as jest.Mock).mock.calls[0][1];
            data = data?.trust_registries
            expect(data[0].versions[0].documents).toEqual([{ language: "fr", url: "doc2" }]);
        });
    });
});
