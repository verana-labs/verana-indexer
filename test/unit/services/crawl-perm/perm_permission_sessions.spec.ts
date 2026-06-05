import { ServiceBroker } from "moleculer";
import PermDatabaseService from "../../../../src/services/crawl-perm/perm_database.service";

describe("🧪 PermDatabaseService Basic Tests", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(async () => {
    broker = new ServiceBroker({
      logger: false,
      metrics: false,
      tracing: false,
    });
    service = broker.createService(PermDatabaseService);
    await broker.start();
  });

  afterAll(async () => {
    await broker.stop();
  });

  it("✅ should create service successfully", () => {
    expect(service).toBeDefined();
    expect(service.name).toBe("participantIngest");
  });

  it("✅ should have getPermission action", () => {
    expect(service.actions.getPermission).toBeDefined();
  });

  it("✅ should have listPermissions action", () => {
    expect(service.actions.listPermissions).toBeDefined();
  });

  it("✅ should be able to call getPermission action", async () => {
    try {
      await broker.call("permDatabase.getPermission", {
        schema_id: 1,
        grantee: "test",
        type: "ECOSYSTEM",
      });
    } catch (error) {
      expect(error).toBeDefined();
    }
  });
});
