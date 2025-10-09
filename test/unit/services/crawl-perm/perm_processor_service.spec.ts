import { ServiceBroker } from "moleculer";
import PermProcessorService from "../../../../src/services/crawl-perm/perm_processor.service";
import {
  PermissionMessageTypes,
  SERVICE,
} from "../../../../src/common/constant";

describe("🧪 PermProcessorService", () => {
  let broker: ServiceBroker;

  // Keep references to spies
  let spyCreateRootPermission: jest.Mock;
  let spyCreatePermission: jest.Mock;

  beforeAll(async () => {
    broker = new ServiceBroker({ logger: false });

    // ✅ Create spies BEFORE creating the service
    spyCreateRootPermission = jest.fn(() => ({ saved: true }));
    spyCreatePermission = jest.fn(() => ({ saved: true }));

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
        getPermission: jest.fn(() => ({ id: "perm-123", type: "root" })),
        listPermissions: jest.fn(() => [
          { id: "perm-1", type: "root" },
          { id: "perm-2", type: "issue" },
        ]),
      },
    });

    broker.createService(PermProcessorService);
    await broker.start();
  });

  afterAll(async () => {
    await broker.stop();
  });

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
});
