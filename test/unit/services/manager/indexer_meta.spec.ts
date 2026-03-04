import { ServiceBroker } from "moleculer";
import IndexerMetaService from "../../../../src/services/manager/indexer_meta.service";
import knex from "../../../../src/common/utils/db_connection";

type QueryState = {
  table: string;
  whereCol?: string;
  whereOp?: string;
  whereVal?: number;
  orderCol?: string;
  orderDir?: string;
  limitVal?: number;
};

const tableHeights: Record<string, number[]> = {
  did_history: [100, 105, 120, 150],
  trust_registry_history: [150],
  governance_framework_version_history: [],
  governance_framework_document_history: [],
  credential_schema_history: [],
  permission_history: [],
  permission_session_history: [],
  trust_deposit_history: [],
  module_params_history: [],
};

const queryStates: QueryState[] = [];

jest.mock("../../../../src/common/utils/db_connection", () => {
  const mockKnex: any = jest.fn((tableName: string) => {
    const state: QueryState = { table: tableName };
    queryStates.push(state);

    const qb: any = {};
    qb.select = jest.fn(() => qb);
    qb.where = jest.fn((col: string, op: string, val: number) => {
      state.whereCol = col;
      state.whereOp = op;
      state.whereVal = Number(val);
      return qb;
    });
    qb.orderBy = jest.fn((col: string, dir: string) => {
      state.orderCol = col;
      state.orderDir = dir;
      return qb;
    });
    qb.limit = jest.fn((val: number) => {
      state.limitVal = val;
      return qb;
    });
    qb.first = jest.fn(async () => {
      const rows = tableHeights[tableName] ?? [];
      const threshold = state.whereVal ?? Number.NEGATIVE_INFINITY;
      const next = rows
        .filter((h) => h > threshold)
        .sort((a, b) => a - b)[0];
      return next !== undefined ? { height: next } : undefined;
    });
    return qb;
  });

  mockKnex.raw = jest.fn((sql: string) => sql);
  return mockKnex;
});

describe("IndexerMetaService next_change_at", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false });
    service = new (IndexerMetaService as any)(broker);
  });

  beforeEach(() => {
    queryStates.length = 0;
  });

  afterAll(async () => {
    await broker.stop();
  });

  it("returns the minimum height strictly greater than block_height", async () => {
    const next = await service.getNextChangeAt(105);
    expect(next).toBe(120);

    expect(queryStates.length).toBeGreaterThan(0);
    for (const q of queryStates) {
      expect(q.whereCol).toBe("height");
      expect(q.whereOp).toBe(">");
      expect(q.whereVal).toBe(105);
      expect((q.orderDir || "").toLowerCase()).toBe("asc");
      expect(q.limitVal).toBe(1);
    }
  });

  it("returns null when no higher height exists", async () => {
    const next = await service.getNextChangeAt(1000);
    expect(next).toBeNull();
  });
});

