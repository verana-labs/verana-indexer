import { ServiceBroker } from "moleculer";
import PermProcessorService from "../../../../src/services/crawl-perm/perm_processor.service";
import {
  SERVICE,
} from "../../../../src/common/constant";
import { VeranaPermissionMessageTypes as PermissionMessageTypes } from "../../../../src/common/verana-message-types";

jest.mock("../../../../src/common/utils/start_mode_detector", () => ({
  detectStartMode: jest.fn().mockResolvedValue({ isFreshStart: false })
}));

describe("🧪 PermProcessorService", () => {
  let broker: ServiceBroker;
  let oldUseHeightSyncPerm: string | undefined;
  let syncPermissionFromLedgerSpy: jest.Mock;
  let comparePermissionWithLedgerSpy: jest.Mock;

  // Keep references to spies
  let spyCreateRootPermission: jest.Mock;
  let spyCreatePermission: jest.Mock;

  beforeAll(async () => {
    oldUseHeightSyncPerm = process.env.USE_HEIGHT_SYNC_PERM;
    process.env.USE_HEIGHT_SYNC_PERM = "false";
    broker = new ServiceBroker({ logger: false });

    // ✅ Create spies BEFORE creating the service
    spyCreateRootPermission = jest.fn(() => ({ saved: true }));
    spyCreatePermission = jest.fn(() => ({ saved: true }));
    syncPermissionFromLedgerSpy = jest.fn(() => ({ success: true, schemaId: 7 }));
    comparePermissionWithLedgerSpy = jest.fn(() => ({ success: true, matches: true }));

    broker.createService({
      name: "permIngest",
      actions: {
        handleMsgCreateRootPermission: spyCreateRootPermission,
        handleMsgCreatePermission: spyCreatePermission,
        handleMsgExtendPermission: jest.fn(() => ({ saved: true })),
        handleMsgRevokePermission: jest.fn(() => ({ saved: true })),
        handleMsgStartPermissionVP: jest.fn(() => ({ saved: true })),
        handleMsgSetPermissionVPToValidated: jest.fn(() => ({ saved: true })),
        handleMsgRenewPermissionVP: jest.fn(() => ({ saved: true })),
        handleMsgCancelPermissionVPLastRequest: jest.fn(() => ({
          saved: true,
        })),
        handleMsgCreateOrUpdatePermissionSession: jest.fn(() => ({
          saved: true,
        })),
        handleMsgSlashPermissionTrustDeposit: jest.fn(() => ({ saved: true })),
        handleMsgRepayPermissionSlashedTrustDeposit: jest.fn(() => ({
          saved: true,
        })),
        syncPermissionFromLedger: syncPermissionFromLedgerSpy,
        syncPermissionSessionFromLedger: jest.fn(() => ({ success: true })),
        comparePermissionWithLedger: comparePermissionWithLedgerSpy,
        comparePermissionSessionWithLedger: jest.fn(() => ({ success: true, matches: true })),
        getPermission: jest.fn(() => ({ id: "perm-123", type: "root" })),
        listPermissions: jest.fn(() => [
          { id: "perm-1", type: "root" },
          { id: "perm-2", type: "issue" },
        ]),
      },
    });

    broker.createService({
      name: SERVICE.V1.TrustDepositDatabaseService.key,
      version: 1,
      actions: {
        syncFromLedger: jest.fn(() => ({ success: true })),
      },
    });

    broker.createService({
      name: SERVICE.V1.TrustRegistryDatabaseService.key,
      version: 1,
      actions: {
        get: jest.fn(() => ({ trust_registry: { id: 3, controller: "verana1controller" } })),
      },
    });

    broker.createService({
      name: SERVICE.V1.CredentialSchemaDatabaseService.key,
      version: 1,
      actions: {
        syncFromLedger: jest.fn(() => ({ success: true })),
      },
    });

    broker.createService(PermProcessorService);
    await broker.start();
  }, 30000);

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_PERM = oldUseHeightSyncPerm;
    await broker.stop();
  }, 30000);

  it("✅ should process permission messages and call correct handlers", async () => {
    const messages = [
      {
        type: PermissionMessageTypes.CreateRootPermission,
        content: { "@type": "someType", id: "perm1", controller: "acc1" },
        timestamp: "2025-10-08T10:00:00Z",
      },
      {
        type: PermissionMessageTypes.CreatePermission,
        content: { "@type": "someType", id: "perm2", controller: "acc2" },
        timestamp: "2025-10-08T11:00:00Z",
      },
    ];

    await broker.call(
      `v1.${SERVICE.V1.PermProcessorService.key}.handlePermissionMessages`,
      { permissionMessages: messages }
    );

    // ✅ Check first spy call
    const ctxRoot = spyCreateRootPermission.mock.calls[0][0];
    expect(ctxRoot.params.data.id).toBe("perm1");
    expect(ctxRoot.params.data.controller).toBe("acc1");
    expect(ctxRoot.params.data).toHaveProperty("timestamp");

    // ✅ Check second spy call
    const ctxPerm = spyCreatePermission.mock.calls[0][0];
    expect(ctxPerm.params.data.id).toBe("perm2");
    expect(ctxPerm.params.data.controller).toBe("acc2");
    expect(ctxPerm.params.data).toHaveProperty("timestamp");
  });

  it("✅ should return permission for getPermission", async () => {
    const res = await broker.call(
      `v1.${SERVICE.V1.PermProcessorService.key}.getPermission`,
      { schema_id: 123, grantee: "wallet123", type: "root" }
    );
    expect(res).toEqual({ id: "perm-123", type: "root" });
  });

  it("✅ should list permissions for listPermissions", async () => {
    const res = await broker.call(
      `v1.${SERVICE.V1.PermProcessorService.key}.listPermissions`,
      { schema_id: 123, grantee: "wallet123", type: "root" }
    );
    expect(Array.isArray(res)).toBe(true);
    expect(res.length).toBe(2);
    expect(res[0].id).toBe("perm-1");
  });

  it("✅ should use height-sync strategy when USE_HEIGHT_SYNC_PERM=true", async () => {
    process.env.USE_HEIGHT_SYNC_PERM = "true";
    const fetchSpy = jest.spyOn(global as any, "fetch").mockImplementation(async (url: string) => {
      const textUrl = String(url);
      if (textUrl.includes("/verana/perm/v1/get/101")) {
        return {
          ok: true,
          json: async () => ({
            permission: {
              id: 101,
              schema_id: 7,
              type: "ISSUER",
              grantee: "verana1grantee",
              created_by: "verana1creator",
              created: "2026-03-01T00:00:00Z",
              modified: "2026-03-01T00:00:00Z",
              validation_fees: 0,
              issuance_fees: 0,
              verification_fees: 0,
              deposit: 0,
              slashed_deposit: 0,
              repaid_deposit: 0,
              vp_state: "VALIDATED",
              vp_validator_deposit: 0,
              vp_current_fees: 0,
              vp_current_deposit: 0,
            },
          }),
        } as any;
      }
      if (textUrl.includes("/verana/cs/v1/get/7")) {
        return {
          ok: true,
          json: async () => ({
            schema: { id: 7, tr_id: 3 },
          }),
        } as any;
      }
      return { ok: false, json: async () => ({}) } as any;
    });

    await broker.call(
      `v1.${SERVICE.V1.PermProcessorService.key}.handlePermissionMessages`,
      {
        permissionMessages: [{
          type: PermissionMessageTypes.CreatePermission,
          content: { id: 101 },
          height: 123,
          timestamp: "2026-03-01T00:00:00Z",
          txHash: "0xabc",
          txEvents: [],
        }],
      }
    );

    expect(syncPermissionFromLedgerSpy).toHaveBeenCalled();
    expect(comparePermissionWithLedgerSpy).toHaveBeenCalled();

    fetchSpy.mockRestore();
    process.env.USE_HEIGHT_SYNC_PERM = "false";
  });
});
