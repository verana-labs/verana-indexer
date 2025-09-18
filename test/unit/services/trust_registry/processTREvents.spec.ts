import { ServiceBroker } from "moleculer";
import ProcessTREventsService from "../../../../src/services/trust_registry/trust_registory.service";
import knex from "../../../../src/common/utils/db_connection";
import { trustRegistryEvents } from "../../../../src/common";

describe("ProcessTREventsService Insert Test", () => {
    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService(ProcessTREventsService);

    afterAll(async () => {
        await broker.stop();
    });

    beforeEach(async () => {
        await knex("governance_framework_document").del().catch(() => {});
        await knex("governance_framework_version").del().catch(() => {});
        await knex("trust_registry").del().catch(() => {});
        await knex("module_params").del().catch(() => {});

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

    it("should insert a Trust Registry with related Governance Framework", async () => {
        const timestamp = new Date();

        const dummyEvent = [
            {
                type: trustRegistryEvents[0], 
                content: {
                    did: "did:example:insert",
                    creator: "creator_test",
                    aka: "Test TR",
                    language: "en",
                    height: 1000,
                    doc_url: "http://example.com/doc.pdf",
                    doc_digest_sri: "sha256-test",
                    timestamp,
                },
            },
        ];

        await service.handleTREvents({ params: { trustRegistryList: dummyEvent } } as any);

        const tr = await knex("trust_registry").where({ did: "did:example:insert" }).first();
        expect(tr).toBeDefined();
        expect(tr.controller).toBe("creator_test");

        const gfv = await knex("governance_framework_version").where({ tr_id: tr.id }).first();
        expect(gfv).toBeDefined();
        expect(gfv.version).toBe(1);

        const gfd = await knex("governance_framework_document").where({ gfv_id: gfv.id }).first();
        expect(gfd).toBeDefined();
        expect(gfd.url).toBe("http://example.com/doc.pdf");
    });
});

describe("ProcessTREventsService Update Test", () => {
    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService(ProcessTREventsService);

    afterAll(async () => {
        await broker.stop();
    });

    beforeEach(async () => {
        await knex("governance_framework_document").del().catch(() => {});
        await knex("governance_framework_version").del().catch(() => {});
        await knex("trust_registry").del().catch(() => {});
    });

    it("should update an existing Trust Registry", async () => {
        const timestamp = new Date();

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

        const updateEvent = [
            {
                type: trustRegistryEvents[2], // UPDATE
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

        await service.handleTREvents({ params: { trustRegistryList: updateEvent } } as any);

        const updatedTR = await knex("trust_registry").where({ id: trId.id }).first();
        expect(updatedTR).toBeDefined();
        expect(updatedTR.aka).toBe("Updated TR");
        expect(updatedTR.language).toBe("fr");
        expect(Number(updatedTR.deposit)).toBe(2000);
        expect(Number(updatedTR.height)).toBe(501); 
    });
});

describe("ProcessTREventsService Add Governance Framework Version Test", () => {
    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService(ProcessTREventsService);

    afterAll(async () => {
        await broker.stop();
    });

    beforeEach(async () => {
        await knex("governance_framework_document").del().catch(() => {});
        await knex("governance_framework_version").del().catch(() => {});
        await knex("trust_registry").del().catch(() => {});
    });

    it("should add a new Governance Framework Version and Document", async () => {
        const timestamp = new Date();

        const [trId] = await knex("trust_registry")
            .insert({
                did: "did:example:add-gfv",
                controller: "creator_gfv",
                created: timestamp,
                modified: timestamp,
                aka: "TR for GFV",
                language: "en",
                height: 600,
                deposit: 1000,
                active_version: 1,
            })
            .returning("id");

        const addGFVEvent = [
            {
                type: trustRegistryEvents[4], // ADD GOVERNANCE FRAMEWORK DOC
                content: {
                    trust_registry_id: trId.id,
                    creator: "creator_gfv",
                    version: 2,
                    doc_language: "en",
                    doc_url: "http://example.com/doc_v2.pdf",
                    doc_digest_sri: "sha256-v2",
                    timestamp,
                },
            },
        ];

        await service.handleTREvents({ params: { trustRegistryList: addGFVEvent } } as any);

        const gfv = await knex("governance_framework_version")
            .where({ tr_id: trId.id, version: 2 })
            .first();
        expect(gfv).toBeDefined();

        const gfd = await knex("governance_framework_document")
            .where({ gfv_id: gfv.id, url: "http://example.com/doc_v2.pdf" })
            .first();
        expect(gfd).toBeDefined();
        expect(gfd.digest_sri).toBe("sha256-v2");
    });
});

describe("ProcessTREventsService Increase Active GFV Test", () => {
    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService(ProcessTREventsService);

    beforeEach(async () => {
        await knex("governance_framework_document").del().catch(() => {});
        await knex("governance_framework_version").del().catch(() => {});
        await knex("trust_registry").del().catch(() => {});
    });

    afterAll(async () => {
        await broker.stop();
    });

    it("should increase the active Governance Framework Version", async () => {
        const timestamp = new Date();
        const uniqueHeight = Date.now(); // ensures unique height

        const [trId] = await knex("trust_registry")
            .insert({
                did: "did:example:increase-active",
                controller: "creator_active",
                created: timestamp,
                modified: timestamp,
                aka: "TR Active Version",
                language: "en",
                height: uniqueHeight,
                deposit: 1000,
                active_version: 1,
            })
            .returning("id");

        const [gfv1Id] = await knex("governance_framework_version")
            .insert({
                tr_id: trId.id,
                created: timestamp,
                version: 1,
                active_since: timestamp,
            })
            .returning("id");

        const [gfv2Id] = await knex("governance_framework_version")
            .insert({
                tr_id: trId.id,
                created: timestamp,
                version: 2,
                active_since: timestamp,
            })
            .returning("id");

        await knex("governance_framework_document").insert([
            {
                gfv_id: gfv1Id.id,
                created: timestamp,
                language: "en",
                url: "http://example.com/doc_v1.pdf",
                digest_sri: "sha256-v1",
            },
            {
                gfv_id: gfv2Id.id,
                created: timestamp,
                language: "en",
                url: "http://example.com/doc_v2.pdf",
                digest_sri: "sha256-v2",
            },
        ]);

        const increaseActiveEvent = [
            {
                type: trustRegistryEvents[5], // INCREASE ACTIVE GFV
                content: {
                    trust_registry_id: trId.id,
                    creator: "creator_active",
                    timestamp,
                },
            },
        ];

        await service.handleTREvents({ params: { trustRegistryList: increaseActiveEvent } } as any);

        const updatedTR = await knex("trust_registry").where({ id: trId.id }).first();
        expect(Number(updatedTR.active_version)).toBe(2);

        const updatedGFV = await knex("governance_framework_version")
            .where({ tr_id: trId.id, version: 2 })
            .first();
        expect(updatedGFV.active_since).not.toBeNull();
    });
}); 

describe("ProcessTREventsService Archive/Unarchive Test", () => {
    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService(ProcessTREventsService);

    beforeEach(async () => {
        await knex("trust_registry").del().catch(() => {});
    });

    afterAll(async () => {
        await broker.stop();
    });

    it("should archive a Trust Registry", async () => {
        const timestamp = new Date();

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

        const archiveEvent = [
            {
                type: trustRegistryEvents[3],
                content: {
                    trust_registry_id: trId.id,
                    creator: "creator_archive",
                    archive: true,
                    timestamp,
                },
            },
        ];

        await service.handleTREvents({ params: { trustRegistryList: archiveEvent } } as any);

        const archivedTR = await knex("trust_registry").where({ id: trId.id }).first();
        expect(archivedTR).toBeDefined();
        expect(archivedTR.archived).toBeNull();
    });

    it("should unarchive a Trust Registry", async () => {
        const timestamp = new Date();

        const [trId] = await knex("trust_registry")
            .insert({
                did: "did:example:unarchive",
                controller: "creator_unarchive",
                created: timestamp,
                modified: timestamp,
                aka: "TR Unarchive",
                language: "en",
                height: 701,
                deposit: 1000,
                active_version: 1,
                archived: timestamp, 
            })
            .returning("id");

        const unarchiveEvent = [
            {
                type: trustRegistryEvents[3],
                content: {
                    trust_registry_id: trId.id,
                    creator: "creator_unarchive",
                    archive: false,
                    timestamp,
                },
            },
        ];

        await service.handleTREvents({ params: { trustRegistryList: unarchiveEvent } } as any);

        const unarchivedTR = await knex("trust_registry").where({ id: trId.id }).first();
        expect(unarchivedTR).toBeDefined();
        expect(unarchivedTR.archived).not.toBeNull();
    });
});
