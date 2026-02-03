import { ServiceBroker } from "moleculer";
import PermAPIService from "../../../../src/services/crawl-perm/perm_apis.service";
import knex from "../../../../src/common/utils/db_connection";

jest.mock("../../../../src/common/utils/db_connection", () => {
  const mockQuery: any = jest.fn(() => mockQuery);
  mockQuery.where = jest.fn(() => mockQuery);
  mockQuery.first = jest.fn(() => mockQuery);
  mockQuery.insert = jest.fn(() => mockQuery);
  mockQuery.update = jest.fn(() => mockQuery);
  mockQuery.select = jest.fn(() => mockQuery);
  mockQuery.limit = jest.fn(() => mockQuery);
  mockQuery.join = jest.fn(() => mockQuery);
  mockQuery.as = jest.fn(() => mockQuery);
  mockQuery.then = jest.fn();
  mockQuery.orderBy = jest.fn(() => mockQuery);
  mockQuery.returning = jest.fn().mockResolvedValue([{}]);
  mockQuery.modify = jest.fn(() => mockQuery);
  return mockQuery;
});

describe("Pending Flat API", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false });
    service = broker.createService(PermAPIService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it("returns empty when no permissions found", async () => {
    (knex as any).mockImplementation(() => ({
      select: () => ({ where: () => ({ limit: () => Promise.resolve([]) }) }),
    }));

    const ctx: any = { params: { account: "acc1", response_max_size: 10 }, meta: {} };
    const res = await service.pendingFlat(ctx);
    expect(res).toEqual({ trust_registries: [] });
  });
});

