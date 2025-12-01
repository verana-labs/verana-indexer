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
  return mockQuery;
});

jest.mock("../../../../src/common/utils/date_utils", () => ({
  formatTimestamp: jest.fn((v) => `formatted-${v}`),
}));

describe("🧪 PermIngestService Unit Tests", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false });
    service = broker.createService(PermIngestService);
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
          grantee: "grantee1",
          validation_fees: "10",
          issuance_fees: "5",
          verification_fees: "2",
          country: "PK",
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
          grantee: "issuer1",
        })
      );
    });
  });

  describe("handleExtendPermission", () => {
    it("should reject invalid ID", async () => {
      const result = await service.handleExtendPermission({
        id: "abc",
        effective_until: new Date().toISOString(),
      });
      expect(result.success).toBe(false);
      expect(result.reason).toBe("Invalid permission ID");
    });

    it("should reject invalid timestamp", async () => {
      const result = await service.handleExtendPermission({
        id: 1,
        effective_until: "INVALID",
      });
      expect(result.success).toBe(false);
      expect(result.reason).toBe("Invalid effective_until timestamp");
    });
  });

  describe("handleRevokePermission", () => {
    it("should revoke permission if caller is grantee", async () => {
      (knex.first as jest.Mock).mockResolvedValueOnce({
        id: 10,
        grantee: "user1",
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
        validation_fees: "0",
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
});
