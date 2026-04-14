import { ServiceBroker } from "moleculer";
import PermIngestService from "../../../../src/services/crawl-perm/perm_database.service";
import knex from "../../../../src/common/utils/db_connection";
import { formatTimestamp } from "../../../../src/common/utils/date_utils";

// Mock knex
jest.mock("../../../../src/common/utils/db_connection", () => {
  const mockQuery: any = jest.fn(() => mockQuery);
  mockQuery.where = jest.fn(() => mockQuery);
  mockQuery.first = jest.fn(() => mockQuery);
  mockQuery.insert = jest.fn(() => mockQuery);
  mockQuery.update = jest.fn(() => mockQuery);
  mockQuery.transaction = jest.fn((fn) => fn(mockQuery));
  mockQuery.commit = jest.fn();
  mockQuery.rollback = jest.fn();
  mockQuery.returning = jest.fn().mockResolvedValue([{}]);
  mockQuery.raw = jest.fn().mockResolvedValue({ rowCount: 0 });
  mockQuery.schema = {
    hasColumn: jest.fn().mockResolvedValue(false),
    hasTable: jest.fn().mockResolvedValue(true),
  };
  return mockQuery;
});

jest.mock("../../../../src/common/utils/date_utils", () => ({
  formatTimestamp: jest.fn((v) => `formatted-${v}`),
}));

describe("🧪 PermIngestService Unit Tests", () => {
  let broker: ServiceBroker;
  let service: any;
  let syncPermissionFromLedger: any;
  let mapLedgerPermissionToDbRow: (row: Record<string, any>) => Record<string, any>;

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false });
    service = broker.createService(PermIngestService);
    syncPermissionFromLedger = (service as any).syncPermissionFromLedger.bind(service);
    mapLedgerPermissionToDbRow = (service as any).mapLedgerPermissionToDbRow.bind(service);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("handleMsgCreateRootPermission", () => {
    it("should insert permission with proper fields", async () => {
      (knex.insert as jest.Mock).mockResolvedValueOnce([1]);

      const msg = {
        schema_id: 99,
        did: "did:test:123",
        creator: "grantee1",
        timestamp: "2025-10-08T00:00:00Z",
        effective_from: "2025-10-09T00:00:00Z",
        effective_until: "2025-12-31T00:00:00Z",
        validation_fees: 10,
        issuance_fees: 5,
        verification_fees: 2,
        country: "PK",
      };

      await service.handleCreateRootPermission(msg);

      expect(knex.insert).toHaveBeenCalledWith(
        expect.objectContaining({
          schema_id: 99,
          type: "ECOSYSTEM",
          did: "did:test:123",
          corporation: "grantee1",
          validation_fees: 10,
          issuance_fees: 5,
          verification_fees: 2,
        })
      );
    });

    it("should skip insert if schema_id is missing", async () => {
      const msg = { creator: "grantee1" };
      await service.handleCreateRootPermission(msg as any);
      expect(knex.insert).not.toHaveBeenCalled();
    });
  });

  describe("handleMsgCreatePermission", () => {
    it("should insert new permission if root ecosystem exists", async () => {
      (knex.where as jest.Mock).mockReturnValueOnce({
        first: jest.fn().mockResolvedValue({ id: 1 }),
      });
      (knex.insert as jest.Mock).mockResolvedValueOnce([2]);

      const msg = {
        schema_id: 99,
        did: "did:test:123",
        creator: "issuer1",
        type: 1,
      };

      await service.handleCreatePermission(msg);

      expect(knex.insert).toHaveBeenCalledWith(
        expect.objectContaining({
          validator_perm_id: 1,
          schema_id: 99,
          corporation: "issuer1",
        })
      );
    });
  });

  describe("handleRevokePermission", () => {
    it("should revoke permission if caller is grantee", async () => {
      (knex.first as jest.Mock).mockResolvedValueOnce({
        id: 10,
        corporation: "user1",
        schema_id: 1,
      });
      (knex.transaction as jest.Mock).mockImplementation((fn) => fn(knex));

      const result = await service.handleRevokePermission({
        id: 10,
        creator: "user1",
        timestamp: "now",
      });

      expect(knex.update).toHaveBeenCalled();
      expect(result.success).toBe(true);
    });

    it("should return error if permission not found", async () => {
      (knex.first as jest.Mock).mockResolvedValueOnce(null);
      const result = await service.handleRevokePermission({
        id: 999,
        creator: "user1",
      });
      expect(result.success).toBe(false);
    });
  });

  describe("handleStartPermissionVP", () => {
    it("should insert new VP record", async () => {
      (knex.first as jest.Mock).mockResolvedValueOnce({
        id: 99,
        schema_id: 99,
        validation_fees: 0,
      });

      const msg = {
        validator_perm_id: 99,
        did: "did:test:abc",
        creator: "alice",
        timestamp: "t1",
      };

      await service.handleStartPermissionVP(msg);

      expect(knex.insert).toHaveBeenCalledWith(
        expect.objectContaining({
          validator_perm_id: 99,
          vp_state: "PENDING",
        })
      );
    });
  });

  describe("mapLedgerPermissionToDbRow (VPR v4 corporation)", () => {
    it("maps corporation from ledger", () => {
      const row = mapLedgerPermissionToDbRow({
        id: 1,
        schema_id: 2,
        type: "ECOSYSTEM",
        did: "did:example:x",
        corporation: "verana1corp",
      });
      expect(row.corporation).toBe("verana1corp");
    });

    it("uses empty string when corporation is missing (NOT NULL column)", () => {
      const row = mapLedgerPermissionToDbRow({
        id: 1,
        schema_id: 2,
        type: "ECOSYSTEM",
        did: "did:x",
      });
      expect(row.corporation).toBe("");
    });
  });

  describe("syncPermissionFromLedger vs legacy stats parity", () => {
    it("should route through same stats helpers for participants/weight", async () => {
      const mockTrx: any = knex;
      const updateWeightSpy = jest
        .spyOn(service as any, "updateWeight")
        .mockResolvedValue(undefined);
      const updateParticipantsSpy = jest
        .spyOn(service as any, "updateParticipants")
        .mockResolvedValue(undefined);

      (knex.first as jest.Mock).mockResolvedValueOnce(null);
      (knex.insert as jest.Mock).mockResolvedValueOnce([{
        id: 7,
        schema_id: 48,
      }]);

      const ledgerPermission = {
        id: "7",
        schema_id: "48",
        type: "ISSUER",
        did: "did:test:issuer",
        corporation: "verana1test",
        created: "2026-01-29T20:27:06.725Z",
        modified: "2026-01-29T20:27:23.422Z",
        effective_from: "2026-01-29T20:27:23.422Z",
        effective_until: null,
        vp_state: "VALIDATED",
      };

      await syncPermissionFromLedger(ledgerPermission, 1908620, "tx-hash", "/verana.perm.v1.MsgSetPermissionVPToValidated");

      expect(updateWeightSpy).toHaveBeenCalledWith(mockTrx, 7);
      expect(updateParticipantsSpy).toHaveBeenCalledWith(mockTrx, 7);
    });
  });
});
