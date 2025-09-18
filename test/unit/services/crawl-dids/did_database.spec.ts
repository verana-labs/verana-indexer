// tests/dids.service.spec.ts
import { ServiceBroker } from "moleculer";
import DidDatabaseService from "../../../../src/services/crawl-dids/dids.service";
import knex from "../../../../src/common/utils/db_connection";
import ModuleParams from "../../../../src/models/modules_params";
import ApiResponder from "../../../../src/common/utils/apiResponse";

jest.mock("../../../../src/common/utils/db_connection");
jest.mock("../../../../src/models/modules_params");
jest.mock("../../../../src/common/utils/apiResponse");

describe("DidDatabaseService", () => {
    let broker: ServiceBroker;
    let service: DidDatabaseService;

    beforeAll(() => {
        broker = new ServiceBroker({ logger: false });
        service = new DidDatabaseService(broker);
        broker.createService(DidDatabaseService as any);
    });

    afterAll(() => broker.stop());
    beforeEach(() => jest.clearAllMocks());

    describe("upsertProcessedDid", () => {
        it("should insert or merge a DID record", async () => {
            const mergeMock = jest.fn().mockResolvedValue(1);
            const onConflictMock = jest.fn(() => ({ merge: mergeMock }));
            (knex as jest.Mock).mockReturnValueOnce({
                insert: jest.fn(() => ({ onConflict: onConflictMock })),
            } as any);

            const ctx: any = { params: { did: "did:example:123" } };
            await service.upsertProcessedDid(ctx);
            expect(knex).toHaveBeenCalledWith("dids");
            expect(mergeMock).toHaveBeenCalled();
            expect(ApiResponder.success).toHaveBeenCalledWith(ctx, { success: true, result: 1 }, 200);
        });
    });

    describe("deleteDid", () => {
        it("should mark DID as deleted", async () => {
            const updateMock = jest.fn().mockResolvedValue(1);
            (knex as jest.Mock).mockReturnValueOnce({
                where: jest.fn(() => ({ update: updateMock })),
            } as any);

            const ctx: any = { params: { did: "did:example:123" } };
            await service.deleteDid(ctx);
            expect(ApiResponder.success).toHaveBeenCalledWith(ctx, { success: true }, 200);
        });

        it("should return 404 if DID not found", async () => {
            const updateMock = jest.fn().mockResolvedValue(0);
            (knex as jest.Mock).mockReturnValueOnce({
                where: jest.fn(() => ({ update: updateMock })),
            } as any);

            const ctx: any = { params: { did: "did:example:notfound" } };
            await service.deleteDid(ctx);
            expect(ApiResponder.error).toHaveBeenCalledWith(ctx, "No record found for DID: did:example:notfound", 404);
        });
    });

    describe("getDid", () => {
        it("should return DID if found", async () => {
            const record = { did: "did:example:123" };
            (knex as jest.Mock).mockReturnValueOnce({
                where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(record) })),
            } as any);

            const ctx: any = { params: { did: "did:example:123" } };
            await service.getDid(ctx);
            expect(ApiResponder.success).toHaveBeenCalledWith(ctx, record, 200);
        });

        it("should return 404 if DID not found", async () => {
            (knex as jest.Mock).mockReturnValueOnce({
                where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(undefined) })),
            } as any);

            const ctx: any = { params: { did: "did:example:notfound" } };
            await service.getDid(ctx);
            expect(ApiResponder.error).toHaveBeenCalledWith(ctx, "DID not found", 404);
        });
    });

    describe("getSingleDid", () => {
        it("should return DID if found", async () => {
            const record = { did: "did:example:123" };
            (knex as jest.Mock).mockReturnValueOnce({
                where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(record) })),
            } as any);

            const ctx: any = { params: { did: "did:example:123" } };
            await service.getSingleDid(ctx);
            expect(ApiResponder.success).toHaveBeenCalledWith(ctx, record, 200);
        });

        it("should return 404 if DID not found", async () => {
            (knex as jest.Mock).mockReturnValueOnce({
                where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(undefined) })),
            } as any);

            const ctx: any = { params: { did: "did:example:notfound" } };
            await service.getSingleDid(ctx);
            expect(ApiResponder.error).toHaveBeenCalledWith(ctx, "Not Found", 404);
        });
    });

    describe("getDidList", () => {
        it("should return list of DIDs", async () => {
            const mockQuery = {
                where: jest.fn().mockReturnThis(),
                andWhere: jest.fn().mockReturnThis(),
                andWhereRaw: jest.fn().mockReturnThis(),
                clone: jest.fn().mockReturnThis(),
                count: jest.fn().mockResolvedValue([{ count: "5" }]),
                orderBy: jest.fn().mockReturnThis(),
                limit: jest.fn().mockReturnThis(),
                offset: jest.fn().mockResolvedValue([{ did: "did:1" }, { did: "did:2" }]),
            };
            (knex as jest.Mock).mockReturnValue(mockQuery as any);

            const ctx: any = { params: { page: 1, response_max_size: 10 } };
            await service.getDidList(ctx);

            expect(ApiResponder.success).toHaveBeenCalledWith(
                ctx,
                { total: 5, page: 1, responseMaxSize: 10, items: [{ did: "did:1" }, { did: "did:2" }] },
                200
            );
        });
    });


    describe("getDidParams", () => {
        it("should return parsed module params", async () => {
            (ModuleParams.query as any).mockReturnValue({
                findOne: jest.fn().mockResolvedValue({ params: JSON.stringify({ params: { key: "value" } }) }),
            });

            const ctx: any = {};
            await service.getDidParams(ctx);
            expect(ApiResponder.success).toHaveBeenCalledWith(ctx, { key: "value" }, 200);
        });

        it("should return 404 if module params not found", async () => {
            (ModuleParams.query as any).mockReturnValue({
                findOne: jest.fn().mockResolvedValue(null),
            });

            const ctx: any = {};
            await service.getDidParams(ctx);
            expect(ApiResponder.error).toHaveBeenCalledWith(ctx, "Module parameters not found: diddirectory", 404);
        });
    });
});
