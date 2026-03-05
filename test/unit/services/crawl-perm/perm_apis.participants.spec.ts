import { ServiceBroker } from "moleculer";
import PermAPIService from "../../../../src/services/crawl-perm/perm_apis.service";

describe("PermAPIService role participant filters", () => {
  let broker: ServiceBroker;
  let service: PermAPIService;

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false });
    service = new PermAPIService(broker);
  });

  afterAll(async () => {
    await broker.stop();
  });

  it("applies half-open ranges for participant role fields", () => {
    const rows = [
      { id: 1, participants_ecosystem: 0, participants_issuer: 1, participants_holder: 0 },
      { id: 2, participants_ecosystem: 1, participants_issuer: 3, participants_holder: 2 },
      { id: 3, participants_ecosystem: 2, participants_issuer: 5, participants_holder: 1 },
    ];

    const filtered = (service as any).applyMetricFiltersInMemory(rows, {
      min_participants_ecosystem: 1,
      max_participants_ecosystem: 3,
      min_participants_issuer: 2,
      max_participants_issuer: 5,
      min_participants_holder: 1,
      max_participants_holder: 3,
    });

    expect(filtered.map((r: any) => r.id)).toEqual([2]);
  });

  it("returns empty when min equals max for a role range", () => {
    const rows = [{ id: 1, participants_verifier: 2 }];
    const filtered = (service as any).applyMetricFiltersInMemory(rows, {
      min_participants_verifier: 2,
      max_participants_verifier: 2,
    });
    expect(filtered).toEqual([]);
  });
});
