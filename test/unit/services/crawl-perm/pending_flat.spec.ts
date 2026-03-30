import { ServiceBroker } from "moleculer";
import PermAPIService from "../../../../src/services/crawl-perm/perm_apis.service";
import knex from "../../../../src/common/utils/db_connection";

function createKnexChain() {
  const c: any = {};
  c.select = jest.fn(() => c);
  c.where = jest.fn((fn: unknown) => {
    if (typeof fn === "function") {
      const qb = { where: jest.fn(() => qb), orWhereIn: jest.fn(() => qb) };
      (fn as (q: typeof qb) => void)(qb);
    }
    return c;
  });
  c.whereIn = jest.fn(() => c);
  c.orWhereIn = jest.fn(() => c);
  c.andWhere = jest.fn(() => c);
  c.whereRaw = jest.fn(() => c);
  c.limit = jest.fn(() => Promise.resolve([]));
  c.from = jest.fn(() => c);
  c.join = jest.fn(() => c);
  c.as = jest.fn(() => c);
  c.orderBy = jest.fn(() => c);
  c.distinctOn = jest.fn(() => c);
  c.whereNotIn = jest.fn(() => c);
  c.clone = jest.fn(() => c);
  c.then = jest.fn((onFulfilled?: (rows: unknown) => unknown) => {
    const p = Promise.resolve([]);
    return typeof onFulfilled === "function" ? p.then(onFulfilled) : p;
  });
  return c;
}

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
  mockQuery.whereIn = jest.fn(() => mockQuery);
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
    (knex as any).mockImplementation(() => createKnexChain());

    const ctx: any = { params: { account: "acc1", response_max_size: 10 }, meta: {} };
    const res = await service.pendingFlat(ctx);
    expect(res).toEqual({ trust_registries: [] });
  });
});
