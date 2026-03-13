import { describe, it, expect, beforeEach, afterEach } from "@jest/globals";
import knex from "../../../../src/common/utils/db_connection";
import TrustRegistryMessageProcessorService from "../../../../src/services/crawl-tr/tr_processor.service";
import { ServiceBroker } from "moleculer";

describe("Trust Registry History Recording", () => {
  let service: TrustRegistryMessageProcessorService;
  let broker: ServiceBroker;

  beforeEach(async () => {
    broker = new ServiceBroker({ logger: false });
    service = new TrustRegistryMessageProcessorService(broker);
    await broker.start();
  });

  afterEach(async () => {
    await broker.stop();
  });

  it("should record history when stats change", async () => {
    // Create a test TR
    const [tr] = await knex("trust_registry")
      .insert({
        did: "did:test:history",
        controller: "verana1test",
        created: new Date(),
        modified: new Date(),
        deposit: "10000000",
        language: "en",
        height: 1000,
        participants: 0,
        participants_ecosystem: 0,
        active_schemas: 0,
        archived_schemas: 0,
        weight: "0",
        issued: "0",
        verified: "0",
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: "0",
        ecosystem_slashed_amount_repaid: "0",
        network_slash_events: 0,
        network_slashed_amount: "0",
        network_slashed_amount_repaid: "0",
      })
      .returning("*");

    expect(tr).toBeDefined();
    const trId = tr.id;

    // Get initial history count
    const initialHistoryCount = await knex("trust_registry_history")
      .where("tr_id", trId)
      .count("* as count")
      .first();

    // Update stats manually to simulate stats change
    await knex("trust_registry")
      .where("id", trId)
      .update({
        participants: 1,
        participants_ecosystem: 1,
        active_schemas: 1,
        weight: "2000000",
      });

    // Call updateTRStatsAndSync to trigger history recording
    const updateMethod = (service as any).updateTRStatsAndSync.bind(service);
    await updateMethod(trId, trId, 1001);

    // Check that history was recorded
    const finalHistoryCount = await knex("trust_registry_history")
      .where("tr_id", trId)
      .count("* as count")
      .first();

    expect(Number(finalHistoryCount?.count)).toBeGreaterThan(Number(initialHistoryCount?.count || 0));

    // Check latest history record
    const latestHistory = await knex("trust_registry_history")
      .where("tr_id", trId)
      .orderBy("height", "desc")
      .first();

    expect(latestHistory).toBeDefined();
    expect(latestHistory.event_type).toBe("StatsUpdate");
    expect(Number(latestHistory.participants)).toBeGreaterThanOrEqual(0);
    expect(Number(latestHistory.active_schemas)).toBeGreaterThanOrEqual(0);

    // Cleanup
    await knex("trust_registry_history").where("tr_id", trId).delete();
    await knex("trust_registry").where("id", trId).delete();
  });

  it("should always save history with current stats values", async () => {
    // Create a test TR
    const [tr] = await knex("trust_registry")
      .insert({
        did: "did:test:history2",
        controller: "verana1test2",
        created: new Date(),
        modified: new Date(),
        deposit: "10000000",
        language: "en",
        height: 2000,
        participants: 5,
        participants_ecosystem: 3,
        active_schemas: 2,
        archived_schemas: 1,
        weight: "5000000",
        issued: "100",
        verified: "50",
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: "0",
        ecosystem_slashed_amount_repaid: "0",
        network_slash_events: 0,
        network_slashed_amount: "0",
        network_slashed_amount_repaid: "0",
      })
      .returning("*");

    const trId = tr.id;

    // Record history using recordTRHistory directly
    const recordMethod = (service as any).recordTRHistory.bind(service);
    const height = 2001;
    await knex.transaction(async (trx) => {
      await recordMethod(trx, trId, "TestUpdate", height, null, tr);
    });

    // Check history record
    const history = await knex("trust_registry_history")
      .where("tr_id", trId)
      .where("event_type", "TestUpdate")
      .first();

    expect(history).toBeDefined();
    expect(Number(history.participants)).toBe(5);
    expect(Number(history.participants_ecosystem)).toBe(3);
    expect(Number(history.active_schemas)).toBe(2);
    expect(Number(history.archived_schemas)).toBe(1);
    expect(Number(history.weight)).toBe(5000000);
    expect(Number(history.issued)).toBe(100);
    expect(Number(history.verified)).toBe(50);

    // Ensure stats in history match live calculateTrustRegistryStats result at the same height
    const { calculateTrustRegistryStats } = await import("../../../../src/services/crawl-tr/tr_stats");
    const stats = await calculateTrustRegistryStats(trId, height);

    expect(Number(history.participants)).toBe(stats.participants);
    expect(Number(history.participants_ecosystem)).toBe(stats.participants_ecosystem);
    expect(Number(history.active_schemas)).toBe(stats.active_schemas);
    expect(Number(history.archived_schemas)).toBe(stats.archived_schemas);
    expect(Number(history.weight)).toBe(Number(stats.weight));
    expect(Number(history.issued)).toBe(Number(stats.issued));
    expect(Number(history.verified)).toBe(Number(stats.verified));
    expect(Number(history.ecosystem_slash_events)).toBe(stats.ecosystem_slash_events);
    expect(Number(history.ecosystem_slashed_amount)).toBe(Number(stats.ecosystem_slashed_amount));
    expect(Number(history.ecosystem_slashed_amount_repaid)).toBe(Number(stats.ecosystem_slashed_amount_repaid));
    expect(Number(history.network_slash_events)).toBe(stats.network_slash_events);
    expect(Number(history.network_slashed_amount)).toBe(Number(stats.network_slashed_amount));
    expect(Number(history.network_slashed_amount_repaid)).toBe(Number(stats.network_slashed_amount_repaid));

    // Cleanup
    await knex("trust_registry_history").where("tr_id", trId).delete();
    await knex("trust_registry").where("id", trId).delete();
  });
});
