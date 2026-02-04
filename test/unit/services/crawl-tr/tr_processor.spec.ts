import { ServiceBroker } from "moleculer";
import {
  ModulesParamsNamesTypes,
} from "../../../../src/common";
import { VeranaTrustRegistryMessageTypes as TrustRegistryMessageTypes } from "../../../../src/common/verana-message-types";
import knex from "../../../../src/common/utils/db_connection";
import TrustRegistryMessageProcessorService from "../../../../src/services/crawl-tr/tr_processor.service";

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
      module: ModulesParamsNamesTypes.TR,
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
        type: TrustRegistryMessageTypes.CreateTrustRegistry,
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
        type: TrustRegistryMessageTypes.UpdateTrustRegistry,
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

    const archiveTrustRegistry = [
      {
        type: TrustRegistryMessageTypes.ArchiveTrustRegistry,
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
    expect(archiveChanges).toHaveProperty("archived");
    const archivedField = archiveChanges.archived;
    if (archivedField === null) {
      expect(archivedField).toBeNull();
    } else if (typeof archivedField === "object" && archivedField !== null) {
      expect(archivedField).toHaveProperty("new");
      expect(archivedField).toHaveProperty("old");
    } else {
      expect(typeof archivedField).toBe("string");
    }

    const unarchiveTrustRegistry = [
      {
        type: TrustRegistryMessageTypes.ArchiveTrustRegistry,
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
    expect(unarchiveChanges).toHaveProperty("archived");
    const unarchiveArchivedField = unarchiveChanges.archived;
    if (unarchiveArchivedField === null) {
      expect(unarchiveArchivedField).toBeNull();
    } else if (typeof unarchiveArchivedField === "object" && unarchiveArchivedField !== null) {
      expect(unarchiveArchivedField).toHaveProperty("new");
      expect(unarchiveArchivedField).toHaveProperty("old");
    } else {
      expect(typeof unarchiveArchivedField).toBe("string");
    }
  });

  it("should create multiple Trust Registries with the same DID at different heights", async () => {
    const timestamp1 = new Date("2025-11-25T02:35:00.319Z");
    const timestamp2 = new Date("2026-01-26T18:37:04.894Z");
    const height1 = 1000;
    const height2 = 2000;
    const sameDid = "did:webvh:QmdRTJUXfCky9wX2EzYFkZKyfgG1W6wqxkQvmVU22uKmBP:ecs-tr.testnet.verana.network";

    // Create first TR
    const firstTR = [
      {
        type: TrustRegistryMessageTypes.CreateTrustRegistry,
        content: {
          did: sameDid,
          creator: "creator1",
          aka: "First TR",
          language: "en",
          height: height1,
          doc_url: "https://verana.io",
          doc_digest_sri: "sha384-Qo8q7llY5uqGcQzBZMZXvagltDKrMy1ratF2O4sxaUOxVatIwUCbLvnurganAfsI",
          timestamp: timestamp1,
        },
      },
    ];

    await service.handleTrustRegistryMessages({
      params: { trustRegistryList: firstTR },
    } as any);

    const firstTRRecord = await knex("trust_registry")
      .where({ height: height1 })
      .first();
    expect(firstTRRecord).toBeDefined();
    expect(firstTRRecord.did).toBe(sameDid);
    expect(firstTRRecord.height).toBe(String(height1));

    const firstGFV = await knex("governance_framework_version")
      .where({ tr_id: firstTRRecord.id })
      .first();
    expect(firstGFV).toBeDefined();

    const firstGFD = await knex("governance_framework_document")
      .where({ gfv_id: firstGFV.id })
      .first();
    expect(firstGFD).toBeDefined();
    expect(firstGFD.url).toBe("https://verana.io");

    // Create second TR with same DID but different height
    const secondTR = [
      {
        type: TrustRegistryMessageTypes.CreateTrustRegistry,
        content: {
          did: sameDid,
          creator: "creator2",
          aka: "Second TR",
          language: "en",
          height: height2,
          doc_url: "https://verana.io/page/about/governance/",
          doc_digest_sri: "sha384-dJ2sWrmvzgdahEMXTrWxMm1l+vI5wQm6B8FGLpO2zfjkKLptsKVZ1Qq3Nqs0hwkN",
          timestamp: timestamp2,
        },
      },
    ];

    await service.handleTrustRegistryMessages({
      params: { trustRegistryList: secondTR },
    } as any);

    // Verify second TR was created (not updating the first one)
    const secondTRRecord = await knex("trust_registry")
      .where({ height: height2 })
      .first();
    expect(secondTRRecord).toBeDefined();
    expect(secondTRRecord.did).toBe(sameDid);
    expect(secondTRRecord.height).toBe(String(height2));
    expect(secondTRRecord.id).not.toBe(firstTRRecord.id); // Should be a different TR

    // Verify first TR was not modified
    const firstTRRecordAfter = await knex("trust_registry")
      .where({ id: firstTRRecord.id })
      .first();
    expect(firstTRRecordAfter.height).toBe(String(height1));
    expect(firstTRRecordAfter.aka).toBe("First TR");

    // Verify second TR has its own GFV and GFD
    const secondGFV = await knex("governance_framework_version")
      .where({ tr_id: secondTRRecord.id })
      .first();
    expect(secondGFV).toBeDefined();
    expect(secondGFV.id).not.toBe(firstGFV.id); // Should be a different GFV

    const secondGFD = await knex("governance_framework_document")
      .where({ gfv_id: secondGFV.id })
      .first();
    expect(secondGFD).toBeDefined();
    expect(secondGFD.url).toBe("https://verana.io/page/about/governance/");
    expect(secondGFD.id).not.toBe(firstGFD.id); // Should be a different GFD

    // Verify first TR's documents were not affected
    const firstGFDAfter = await knex("governance_framework_document")
      .where({ gfv_id: firstGFV.id })
      .first();
    expect(firstGFDAfter.url).toBe("https://verana.io");
    expect(firstGFDAfter.id).toBe(firstGFD.id);
  });
});
