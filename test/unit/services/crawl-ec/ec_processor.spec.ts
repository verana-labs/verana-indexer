import { ServiceBroker } from "moleculer";
import {
  ModulesParamsNamesTypes,
} from "../../../../src/common";
import { VeranaEcosystemMessageTypes as EcosystemMessageTypes } from "../../../../src/common/verana-message-types";
import knex from "../../../../src/common/utils/db_connection";
import EcosystemMessageProcessorService from "../../../../src/services/crawl-ec/ec_processor.service";

describe("EcosystemMessageProcessorService Tests", () => {
  const broker = new ServiceBroker({ logger: false });
  const service = broker.createService(EcosystemMessageProcessorService);

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
    await knex("ecosystem_history")
      .del()
      .catch(() => {});
    await knex("governance_framework_document")
      .del()
      .catch(() => {});
    await knex("governance_framework_version")
      .del()
      .catch(() => {});
    await knex("ecosystem")
      .del()
      .catch(() => {});
    await knex("module_params")
      .del()
      .catch(() => {});

    await knex("module_params").insert({
      module: ModulesParamsNamesTypes.EC,
      params: JSON.stringify({
        params: {
          ecosystem_trust_deposit: 1000,
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
        type: EcosystemMessageTypes.CreateEcosystem,
        content: {
          did: "did:example:insert",
          creator: "creator_test",
          corporation_id: 1,
          aka: "Test EC",
          language: "en",
          height,
          doc_url: "http://example.com/doc.pdf",
          doc_digest_sri: "sha256-test",
          timestamp,
        },
      },
    ];

    await service.handleEcosystemMessages({
      params: { ecosystemList: dummyTR },
    } as any);

    const ec = await knex("ecosystem")
      .where({ did: "did:example:insert" })
      .first();
    expect(ec).toBeDefined();
    expect(Number(ec.corporation_id)).toBe(1);

    const gfv = await knex("governance_framework_version")
      .where({ ecosystem_id: ec.id })
      .first();
    expect(gfv).toBeDefined();
    expect(gfv.version).toBe(1);

    const gfd = await knex("governance_framework_document")
      .where({ gfv_id: gfv.id })
      .first();
    expect(gfd).toBeDefined();
    expect(gfd.url).toBe("http://example.com/doc.pdf");

    const ecosystemHistory = await knex("ecosystem_history")
      .where({ ecosystem_id: ec.id })
      .first();
    expect(ecosystemHistory).toBeDefined();
    expect(ecosystemHistory.changes).toBeDefined();

    const gfvHistory = await knex("governance_framework_version_history")
      .where({ ecosystem_id: ec.id })
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

    const [ecosystemId] = await knex("ecosystem")
      .insert({
        did: "did:example:update",
        corporation_id: 2,
        created: timestamp,
        modified: timestamp,
        aka: "Old EC",
        language: "en",
        height: 500,
        active_version: 1,
      })
      .returning("id");

    const UpdateResponse = [
      {
        type: EcosystemMessageTypes.UpdateEcosystem,
        content: {
          ecosystem_id: ecosystemId.id,
          creator: "creator_update",
          aka: "Updated EC",
          language: "fr",
          height: 501,
          deposit: 2000,
          timestamp,
        },
      },
    ];

    await service.handleEcosystemMessages({
      params: { ecosystemList: UpdateResponse },
    } as any);

    const updatedTR = await knex("ecosystem")
      .where({ id: ecosystemId.id })
      .first();
    expect(updatedTR).toBeDefined();
    expect(updatedTR.aka).toBe("Updated EC");
    expect(updatedTR.language).toBe("fr");
    expect(updatedTR.height).toBe(501);

    const ecosystemHistory = await knex("ecosystem_history")
      .where({ ecosystem_id: ecosystemId.id })
      .orderBy("id", "desc")
      .first();
    expect(ecosystemHistory).toBeDefined();
    const changes =
      typeof ecosystemHistory.changes === "string"
        ? JSON.parse(ecosystemHistory.changes)
        : ecosystemHistory.changes;
    expect(changes).toHaveProperty("aka");
    expect(changes).toHaveProperty("language");
    expect(changes).toHaveProperty("height");
  });

  it("should archive and unarchive a Trust Registry with history", async () => {
    const timestamp = new Date();
    const height = 3;

    const [ecosystemId] = await knex("ecosystem")
      .insert({
        did: "did:example:archive",
        corporation_id: 3,
        created: timestamp,
        modified: timestamp,
        aka: "EC Archive",
        language: "en",
        height: 700,
        active_version: 1,
      })
      .returning("id");

    const archiveEcosystem = [
      {
        type: EcosystemMessageTypes.ArchiveEcosystem,
        content: {
          ecosystem_id: ecosystemId.id,
          creator: "creator_archive",
          archive: true,
          timestamp,
          height,
        },
      },
    ];
    await service.handleEcosystemMessages({
      params: { ecosystemList: archiveEcosystem },
    } as any);

    const archivedTR = await knex("ecosystem")
      .where({ id: ecosystemId.id })
      .first();
    expect(archivedTR.archived).not.toBeNull();

    const ecosystemHistoryArchive = await knex("ecosystem_history")
      .where({ ecosystem_id: ecosystemId.id })
      .orderBy("id", "desc")
      .first();
    expect(ecosystemHistoryArchive).toBeDefined();
    const archiveChanges =
      typeof ecosystemHistoryArchive.changes === "string"
        ? JSON.parse(ecosystemHistoryArchive.changes)
        : ecosystemHistoryArchive.changes;
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

    const unarchiveEcosystem = [
      {
        type: EcosystemMessageTypes.ArchiveEcosystem,
        content: {
          ecosystem_id: ecosystemId.id,
          creator: "creator_archive",
          archive: false,
          timestamp,
          height,
        },
      },
    ];
    await service.handleEcosystemMessages({
      params: { ecosystemList: unarchiveEcosystem },
    } as any);

    const unarchivedTR = await knex("ecosystem")
      .where({ id: ecosystemId.id })
      .first();
    expect(unarchivedTR.archived).toBeNull();

    const ecosystemHistoryUnarchive = await knex("ecosystem_history")
      .where({ ecosystem_id: ecosystemId.id })
      .orderBy("id", "desc")
      .first();
    expect(ecosystemHistoryUnarchive).toBeDefined();
    const unarchiveChanges =
      typeof ecosystemHistoryUnarchive.changes === "string"
        ? JSON.parse(ecosystemHistoryUnarchive.changes)
        : ecosystemHistoryUnarchive.changes;
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
    const sameDid = "did:webvh:QmdRTJUXfCky9wX2EzYFkZKyfgG1W6wqxkQvmVU22uKmBP:ecs-ec.testnet.verana.network";

    // Create first EC
    const firstTR = [
      {
        type: EcosystemMessageTypes.CreateEcosystem,
        content: {
          did: sameDid,
          creator: "creator1",
          aka: "First EC",
          language: "en",
          height: height1,
          doc_url: "https://verana.io",
          doc_digest_sri: "sha384-Qo8q7llY5uqGcQzBZMZXvagltDKrMy1ratF2O4sxaUOxVatIwUCbLvnurganAfsI",
          timestamp: timestamp1,
        },
      },
    ];

    await service.handleEcosystemMessages({
      params: { ecosystemList: firstTR },
    } as any);

    const firstTRRecord = await knex("ecosystem")
      .where({ height: height1 })
      .first();
    expect(firstTRRecord).toBeDefined();
    expect(firstTRRecord.did).toBe(sameDid);
    expect(firstTRRecord.height).toBe(height1);

    const firstGFV = await knex("governance_framework_version")
      .where({ ecosystem_id: firstTRRecord.id })
      .first();
    expect(firstGFV).toBeDefined();

    const firstGFD = await knex("governance_framework_document")
      .where({ gfv_id: firstGFV.id })
      .first();
    expect(firstGFD).toBeDefined();
    expect(firstGFD.url).toBe("https://verana.io");

    // Create second EC with same DID but different height
    const secondTR = [
      {
        type: EcosystemMessageTypes.CreateEcosystem,
        content: {
          did: sameDid,
          creator: "creator2",
          aka: "Second EC",
          language: "en",
          height: height2,
          doc_url: "https://verana.io/page/about/governance/",
          doc_digest_sri: "sha384-dJ2sWrmvzgdahEMXTrWxMm1l+vI5wQm6B8FGLpO2zfjkKLptsKVZ1Qq3Nqs0hwkN",
          timestamp: timestamp2,
        },
      },
    ];

    await service.handleEcosystemMessages({
      params: { ecosystemList: secondTR },
    } as any);

    // Verify second EC was created (not updating the first one)
    const secondTRRecord = await knex("ecosystem")
      .where({ height: height2 })
      .first();
    expect(secondTRRecord).toBeDefined();
    expect(secondTRRecord.did).toBe(sameDid);
    expect(secondTRRecord.height).toBe(height2);
    expect(secondTRRecord.id).not.toBe(firstTRRecord.id); // Should be a different EC

    // Verify first EC was not modified
    const firstTRRecordAfter = await knex("ecosystem")
      .where({ id: firstTRRecord.id })
      .first();
    expect(firstTRRecordAfter.height).toBe(height1);
    expect(firstTRRecordAfter.aka).toBe("First EC");

    // Verify second EC has its own GFV and GFD
    const secondGFV = await knex("governance_framework_version")
      .where({ ecosystem_id: secondTRRecord.id })
      .first();
    expect(secondGFV).toBeDefined();
    expect(secondGFV.id).not.toBe(firstGFV.id); // Should be a different GFV

    const secondGFD = await knex("governance_framework_document")
      .where({ gfv_id: secondGFV.id })
      .first();
    expect(secondGFD).toBeDefined();
    expect(secondGFD.url).toBe("https://verana.io/page/about/governance/");
    expect(secondGFD.id).not.toBe(firstGFD.id); // Should be a different GFD

    // Verify first EC's documents were not affected
    const firstGFDAfter = await knex("governance_framework_document")
      .where({ gfv_id: firstGFV.id })
      .first();
    expect(firstGFDAfter.url).toBe("https://verana.io");
    expect(firstGFDAfter.id).toBe(firstGFD.id);
  });
});
