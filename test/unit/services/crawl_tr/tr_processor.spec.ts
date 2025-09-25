import { ServiceBroker } from "moleculer";
import { TrustRegistryMessageTypes } from "../../../common";
import knex from "../../../common/utils/db_connection";
import TrustRegistryMessageProcessorService from "../../../services/crawl_tr/tr_processor.service";

describe("TrustRegistryMessageProcessorService Tests", () => {
  const broker = new ServiceBroker({ logger: false });
  const service = broker.createService(TrustRegistryMessageProcessorService);

  afterAll(async () => {
    await broker.stop();
  });

  beforeEach(async () => {
    await knex("governance_framework_document_history")
      .del()
      .catch(() => {});
    await knex("governance_framework_version_history")
      .del()
      .catch(() => {});
    await knex("trust_registry_history")
      .del()
      .catch(() => {});
    await knex("governance_framework_document")
      .del()
      .catch(() => {});
    await knex("governance_framework_version")
      .del()
      .catch(() => {});
    await knex("trust_registry")
      .del()
      .catch(() => {});
    await knex("module_params")
      .del()
      .catch(() => {});

    await knex("module_params").insert({
      module: "trustregistry",
      params: JSON.stringify({
        params: {
          trust_registry_trust_deposit: 1000,
          trust_unit_price: 1,
        },
      }),
    });
  });

  it("should insert a Trust Registry with related Governance Framework and history", async () => {
    const timestamp = new Date();
    const height = 1;

    const dummyTR = [
      {
        type: TrustRegistryMessageTypes.Create, // CREATE
        content: {
          did: "did:example:insert",
          creator: "creator_test",
          aka: "Test TR",
          language: "en",
          height,
          doc_url: "http://example.com/doc.pdf",
          doc_digest_sri: "sha256-test",
          timestamp,
        },
      },
    ];

    await service.handleTrustRegistryMessages({
      params: { trustRegistryList: dummyTR },
    } as any);

    const tr = await knex("trust_registry")
      .where({ did: "did:example:insert" })
      .first();
    expect(tr).toBeDefined();
    expect(tr.controller).toBe("creator_test");

    const gfv = await knex("governance_framework_version")
      .where({ tr_id: tr.id })
      .first();
    expect(gfv).toBeDefined();
    expect(gfv.version).toBe(1);

    const gfd = await knex("governance_framework_document")
      .where({ gfv_id: gfv.id })
      .first();
    expect(gfd).toBeDefined();
    expect(gfd.url).toBe("http://example.com/doc.pdf");

    const trHistory = await knex("trust_registry_history")
      .where({ tr_id: tr.id })
      .first();
    expect(trHistory).toBeDefined();
    expect(trHistory.changes).toBeDefined();

    const gfvHistory = await knex("governance_framework_version_history")
      .where({ tr_id: tr.id })
      .first();
    expect(gfvHistory).toBeDefined();
    expect(gfvHistory.changes).toBeDefined();

    const gfdHistory = await knex("governance_framework_document_history")
      .where({ gfv_id: gfv.id })
      .first();
    expect(gfdHistory).toBeDefined();
    expect(gfdHistory.changes).toBeDefined();
  });

  it("should update an existing Trust Registry and record history", async () => {
    const timestamp = new Date();
    const height = 2;

    const [trId] = await knex("trust_registry")
      .insert({
        did: "did:example:update",
        controller: "creator_update",
        created: timestamp,
        modified: timestamp,
        aka: "Old TR",
        language: "en",
        height: 500,
        deposit: 1000,
        active_version: 1,
      })
      .returning("id");

    const UpdateResponse = [
      {
        type: TrustRegistryMessageTypes.Update, // UPDATE
        content: {
          trust_registry_id: trId.id,
          creator: "creator_update",
          aka: "Updated TR",
          language: "fr",
          height: 501,
          deposit: 2000,
          timestamp,
        },
      },
    ];

    await service.handleTrustRegistryMessages({
      params: { trustRegistryList: UpdateResponse },
    } as any);

    const updatedTR = await knex("trust_registry")
      .where({ id: trId.id })
      .first();
    expect(updatedTR).toBeDefined();
    expect(updatedTR.aka).toBe("Updated TR");
    expect(updatedTR.language).toBe("fr");
    expect(Number(updatedTR.deposit)).toBe(2000);
    expect(Number(updatedTR.height)).toBe(501);

    const trHistory = await knex("trust_registry_history")
      .where({ tr_id: trId.id })
      .orderBy("id", "desc")
      .first();
    expect(trHistory).toBeDefined();
    const changes =
      typeof trHistory.changes === "string"
        ? JSON.parse(trHistory.changes)
        : trHistory.changes;
    expect(changes).toHaveProperty("aka");
    expect(changes).toHaveProperty("language");
    expect(changes).toHaveProperty("deposit");
    expect(changes).toHaveProperty("height");
  });

  it("should archive and unarchive a Trust Registry with history", async () => {
    const timestamp = new Date();
    const height = 3;

    const [trId] = await knex("trust_registry")
      .insert({
        did: "did:example:archive",
        controller: "creator_archive",
        created: timestamp,
        modified: timestamp,
        aka: "TR Archive",
        language: "en",
        height: 700,
        deposit: 1000,
        active_version: 1,
      })
      .returning("id");

    // Archive
    const archiveTrustRegistry = [
      {
        type: TrustRegistryMessageTypes.Archive,
        content: {
          trust_registry_id: trId.id,
          creator: "creator_archive",
          archive: true,
          timestamp,
          height,
        },
      },
    ];
    await service.handleTrustRegistryMessages({
      params: { trustRegistryList: archiveTrustRegistry },
    } as any);

    const archivedTR = await knex("trust_registry")
      .where({ id: trId.id })
      .first();
    expect(archivedTR.archived).not.toBeNull();

    const trHistoryArchive = await knex("trust_registry_history")
      .where({ tr_id: trId.id })
      .orderBy("id", "desc")
      .first();
    expect(trHistoryArchive).toBeDefined();
    const archiveChanges =
      typeof trHistoryArchive.changes === "string"
        ? JSON.parse(trHistoryArchive.changes)
        : trHistoryArchive.changes;
    expect(archiveChanges.archived).toHaveProperty("new");
    expect(archiveChanges.archived).toHaveProperty("old");

    // Unarchive
    const unarchiveTrustRegistry = [
      {
        type: TrustRegistryMessageTypes.Archive,
        content: {
          trust_registry_id: trId.id,
          creator: "creator_archive",
          archive: false,
          timestamp,
          height,
        },
      },
    ];
    await service.handleTrustRegistryMessages({
      params: { trustRegistryList: unarchiveTrustRegistry },
    } as any);

    const unarchivedTR = await knex("trust_registry")
      .where({ id: trId.id })
      .first();
    expect(unarchivedTR.archived).toBeNull();

    const trHistoryUnarchive = await knex("trust_registry_history")
      .where({ tr_id: trId.id })
      .orderBy("id", "desc")
      .first();
    expect(trHistoryUnarchive).toBeDefined();
    const unarchiveChanges =
      typeof trHistoryUnarchive.changes === "string"
        ? JSON.parse(trHistoryUnarchive.changes)
        : trHistoryUnarchive.changes;
    expect(unarchiveChanges.archived).toHaveProperty("new");
    expect(unarchiveChanges.archived).toHaveProperty("old");
  });
});
