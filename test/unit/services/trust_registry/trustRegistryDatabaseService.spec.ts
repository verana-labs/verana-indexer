// tests/trustRegistryDatabaseService.spec.ts
import { ServiceBroker } from "moleculer";
import TrustRegistryDatabaseService from "../../../../src/services/trust_registry/tr_database.service";
import { TrustRegistry } from "../../../../src/models/trust_registry";
import ApiResponder from "../../../../src/common/utils/apiResponse";

jest.mock("../../../../src/models/trust_registry");
jest.mock("../../../../src/common/utils/apiResponse");

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

            const ctx: any = { params: { tr_id: 1, preferred_language: "en", active_gf_only: "true" } };
            await service.getTrustRegistry(ctx);

            expect(ApiResponder.success).toHaveBeenCalled();
            const data = (ApiResponder.success as jest.Mock).mock.calls[0][1];
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

            const ctx: any = {
                params: { active_gf_only: "true", preferred_language: "fr", response_max_size: 2 },
            };
            await service.listTrustRegistries(ctx);

            expect(ApiResponder.success).toHaveBeenCalled();
            const data = (ApiResponder.success as jest.Mock).mock.calls[0][1];
            expect(data[0].versions[0].documents).toEqual([{ language: "fr", url: "doc2" }]);
        });
    });
});
