import { BULL_JOB_NAME, SERVICE } from "../../../../src/common";

jest.mock("../../../../src/common/utils/db_connection", () => ({
  __esModule: true,
  default: Object.assign(jest.fn(), {
    raw: jest.fn().mockResolvedValue({ rows: [] }),
    transaction: jest.fn(async (fn: any) => fn({})),
  }),
}));

jest.mock("../../../../src/common/utils/chain.registry", () => ({
  __esModule: true,
  default: jest.fn(() => ({})),
}));

const txQuery = {
  select: jest.fn(),
  where: jest.fn(),
  orderBy: jest.fn(),
  first: jest.fn(),
  max: jest.fn(),
  timeout: jest.fn(),
};

jest.mock("../../../../src/models", () => ({
  __esModule: true,
  Transaction: {
    query: () => txQuery,
  },
  BlockCheckpoint: {
    getCheckpoint: jest.fn(),
    query: jest.fn(),
  },
}));

describe("CrawlTxService (trust resolver decoupled)", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    txQuery.select.mockReturnValue(txQuery);
    txQuery.where.mockReturnValue(txQuery);
    txQuery.orderBy.mockReturnValue(txQuery);
    txQuery.max.mockReturnValue(txQuery);
    txQuery.timeout.mockReturnValue(txQuery);
  });

  it("does not enqueue trust-resolve after block-indexed (resolver polls independently)", async () => {
    const { default: CrawlTxService } = await import("../../../../src/services/crawl-tx/crawl_tx.service");
    const { BlockCheckpoint } = await import("../../../../src/models");

    (BlockCheckpoint.getCheckpoint as any).mockResolvedValue([
      0,
      100,
      { job_name: BULL_JOB_NAME.HANDLE_TRANSACTION, height: 0, updated_at: new Date() },
    ]);

    const bcQueryChain: any = {
      select: jest.fn(() => bcQueryChain),
      where: jest.fn(() => bcQueryChain),
      first: jest.fn(async () => ({ height: 10 })),
      insert: jest.fn(() => bcQueryChain),
      onConflict: jest.fn(() => bcQueryChain),
      merge: jest.fn(() => bcQueryChain),
      timeout: jest.fn(() => bcQueryChain),
      transacting: jest.fn(async () => undefined),
    };
    (BlockCheckpoint.query as any).mockReturnValue(bcQueryChain);

    txQuery.first.mockResolvedValueOnce({ id: 0 });
    txQuery.first.mockResolvedValueOnce({ max_id: 0 });

    const trustQueue = { add: jest.fn().mockResolvedValue(undefined) };

    const svc = Object.create(CrawlTxService.prototype) as any;
    svc.logger = { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() };
    svc.broker = { call: jest.fn().mockResolvedValue(undefined) };
    svc.getQueueManager = () => ({ getQueue: () => trustQueue });
    svc.applyScheduledFlipsForRange = jest.fn().mockResolvedValue(undefined);
    svc.applyScheduledFlipsForBlockHeight = jest.fn().mockResolvedValue(undefined);
    svc.processTransactionsForBlock = jest.fn();
    svc.processPayloads = jest.fn();
    svc._hasUniqueConstraintCache = true;
    svc._isFreshStart = false;

    await svc.jobHandlerCrawlTxBody();

    expect(svc.broker.call).toHaveBeenCalledWith(
      `${SERVICE.V1.IndexerEventsService.path}.broadcastBlockIndexed`,
      expect.objectContaining({ height: 10 })
    );
    expect(trustQueue.add).not.toHaveBeenCalled();
  });
});
