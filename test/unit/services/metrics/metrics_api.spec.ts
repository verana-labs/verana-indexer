import { ServiceBroker } from "moleculer";
import MetricsService from "../../../../src/services/metrics/metrics_api.service";
import knex from "../../../../src/common/utils/db_connection";

jest.mock("../../../../src/common/utils/db_connection", () => {
  const mockQuery: any = jest.fn(() => mockQuery);
  mockQuery.select = jest.fn(() => mockQuery);
  mockQuery.where = jest.fn(() => mockQuery);
  mockQuery.countDistinct = jest.fn(() => mockQuery);
  mockQuery.first = jest.fn(() => mockQuery);
  mockQuery.as = jest.fn(() => mockQuery);
  mockQuery.from = jest.fn(() => mockQuery);
  mockQuery.join = jest.fn(() => mockQuery);
  mockQuery.then = jest.fn();
  mockQuery.orderBy = jest.fn(() => mockQuery);
  mockQuery.limit = jest.fn(() => mockQuery);
  mockQuery.select = jest.fn(() => mockQuery);
  mockQuery.returning = jest.fn().mockResolvedValue([{}]);
  return mockQuery;
});

describe("Metrics API", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false });
    service = broker.createService(MetricsService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it("runs latest metrics without throwing", async () => {
    (knex as any).mockImplementation(() => ({
      select: () => ({ first: async () => ({ count: '0' }) }),
      where: () => ({ select: async () => [] }),
    }));
    const ctx: any = { params: {}, meta: {} };
    await expect(service.getAll(ctx)).resolves.not.toThrow();
  });

  it("runs historical metrics without throwing", async () => {
    (knex as any).mockImplementation(() => ({
      select: () => ({ first: async () => ({ count: '0' }) }),
      where: () => ({ orderBy: () => ({ first: async () => null }) }),
      from: () => ({ select: async () => [] }),
    }));
    const ctx: any = { params: {}, meta: { $headers: { "at-block-height": "100" } } };
    await expect(service.getAll(ctx)).resolves.not.toThrow();
  });
});

