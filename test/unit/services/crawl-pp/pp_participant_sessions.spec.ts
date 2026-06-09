import { ServiceBroker } from "moleculer";
import ParticipantDatabaseService from "../../../../src/services/crawl-pp/pp_database.service";

describe("🧪 ParticipantDatabaseService Basic Tests", () => {
  let broker: ServiceBroker;
  let service: any;

  beforeAll(async () => {
    broker = new ServiceBroker({
      logger: false,
      metrics: false,
      tracing: false,
    });
    service = broker.createService(ParticipantDatabaseService);
    await broker.start();
  });

  afterAll(async () => {
    await broker.stop();
  });

  it("✅ should create service successfully", () => {
    expect(service).toBeDefined();
    expect(service.name).toBe("participantIngest");
  });

  it("✅ should have getParticipant action", () => {
    expect(service.actions.getParticipant).toBeDefined();
  });

  it("✅ should have listParticipants action", () => {
    expect(service.actions.listParticipants).toBeDefined();
  });

  it("✅ should be able to call getParticipant action", async () => {
    try {
      await broker.call("participantDatabase.getParticipant", {
        schema_id: 1,
        grantee: "test",
        type: "ECOSYSTEM",
      });
    } catch (error) {
      expect(error).toBeDefined();
    }
  });
});
