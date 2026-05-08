import knex from "../../../../src/common/utils/db_connection";
import { VeranaCredentialSchemaMessageTypes, VeranaPermissionMessageTypes, VeranaTrustRegistryMessageTypes } from "../../../../src/common/verana-message-types";
import { up as createIndexerEventsTable } from "../../../../src/migrations/20260420000000_create_indexer_events";
import { up as hardenIndexerEventsTable } from "../../../../src/migrations/20260421000000_harden_indexer_events_replay";
import { listIndexerEvents, persistIndexerEventsForBlock } from "../../../../src/services/api/indexer_events_query";

describe("indexer_events_query", () => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const baseHeight = 7_000_000 + Math.floor(Math.random() * 100_000);
  let nextId = 90_000_000 + Math.floor(Math.random() * 1_000_000);
  const did = `did:web:indexer-events-${runId}.example`;
  const otherDid = `did:web:indexer-events-other-${runId}.example`;
  const txHashes: string[] = [];
  const heights: number[] = [];
  const createdTables: string[] = [];

  beforeAll(async () => {
    await ensureBaseTables();
    await createIndexerEventsTable(knex);
    await hardenIndexerEventsTable(knex);
  });

  afterAll(async () => {
    for (const table of createdTables.reverse()) {
      await knex.schema.dropTableIfExists(table);
    }
  });

  async function ensureBaseTables(): Promise<void> {
    if (!(await knex.schema.hasTable("block"))) {
      await knex.schema.createTable("block", (table) => {
        table.bigInteger("height").primary();
        table.text("hash").notNullable();
        table.timestamp("time").notNullable();
        table.text("proposer_address").nullable();
        table.jsonb("data").nullable();
      });
      createdTables.push("block");
    }

    if (!(await knex.schema.hasTable("transaction"))) {
      await knex.schema.createTable("transaction", (table) => {
        table.bigInteger("id").primary();
        table.bigInteger("height").notNullable();
        table.text("hash").notNullable();
        table.text("codespace").nullable();
        table.integer("code").notNullable().defaultTo(0);
        table.bigInteger("gas_used").nullable();
        table.bigInteger("gas_wanted").nullable();
        table.bigInteger("gas_limit").nullable();
        table.jsonb("fee").nullable();
        table.timestamp("timestamp").notNullable();
        table.jsonb("data").nullable();
        table.integer("index").notNullable().defaultTo(0);
      });
      createdTables.push("transaction");
    }

    if (!(await knex.schema.hasTable("transaction_message"))) {
      await knex.schema.createTable("transaction_message", (table) => {
        table.bigInteger("id").primary();
        table.bigInteger("tx_id").notNullable();
        table.integer("index").notNullable().defaultTo(0);
        table.text("type").notNullable();
        table.text("sender").nullable();
        table.jsonb("content").nullable();
      });
      createdTables.push("transaction_message");
    }

    if (!(await knex.schema.hasTable("trust_registry"))) {
      await knex.schema.createTable("trust_registry", (table) => {
        table.bigInteger("id").primary();
        table.text("did").notNullable();
      });
      createdTables.push("trust_registry");
    }

    if (!(await knex.schema.hasTable("credential_schemas"))) {
      await knex.schema.createTable("credential_schemas", (table) => {
        table.bigInteger("id").primary();
        table.bigInteger("tr_id").notNullable();
      });
      createdTables.push("credential_schemas");
    }

    if (!(await knex.schema.hasTable("permissions"))) {
      await knex.schema.createTable("permissions", (table) => {
        table.bigInteger("id").primary();
        table.bigInteger("schema_id").nullable();
        table.text("did").nullable();
        table.bigInteger("validator_perm_id").nullable();
      });
      createdTables.push("permissions");
    }
  }

  async function insertBlock(height: number): Promise<void> {
    heights.push(height);
    await knex("block").insert({
      height,
      hash: `block-${runId}-${height}`,
      time: new Date("2025-01-15T10:30:00Z"),
      proposer_address: "validator",
      data: {},
    });
  }

  async function insertTxMessage(args: {
    height: number;
    txIndex: number;
    messageIndex: number;
    hash: string;
    sender?: string;
    content?: Record<string, unknown>;
    messageType?: string;
    txResponse?: any;
  }): Promise<void> {
    txHashes.push(args.hash);
    const txId = nextId++;
    const messageId = nextId++;
    const [tx] = await knex("transaction")
      .insert({
        id: txId,
        height: args.height,
        hash: args.hash,
        codespace: "",
        code: 0,
        gas_used: 1,
        gas_wanted: 1,
        gas_limit: 1,
        fee: {},
        timestamp: new Date("2025-01-15T10:30:00Z"),
        data: args.txResponse ? { tx_response: args.txResponse } : {},
        index: args.txIndex,
      })
      .returning("id");

    await knex("transaction_message").insert({
      id: messageId,
      tx_id: typeof tx === "object" ? tx.id : tx,
      index: args.messageIndex,
      type: args.messageType ?? VeranaPermissionMessageTypes.StartPermissionVP,
      sender: args.sender ?? did,
      content: args.content ?? { id: 42, applicant: did },
    });
  }

  async function insertStoredIndexerEvent(args: {
    did: string;
    relatedDids: string[];
    height: number;
    txHash: string;
    txIndex?: number;
    messageIndex?: number;
  }): Promise<void> {
    txHashes.push(args.txHash);
    await knex("indexer_events").insert({
      event_type: "StartPermissionVP",
      did: args.did,
      block_height: args.height,
      tx_hash: args.txHash,
      tx_index: args.txIndex ?? 0,
      message_index: args.messageIndex ?? 0,
      message_type: VeranaPermissionMessageTypes.StartPermissionVP,
      module: "permission",
      entity_type: "Permission",
      entity_id: "42",
      timestamp: new Date("2025-01-15T10:30:00Z"),
      payload: {
        module: "permission",
        action: "StartPermissionVP",
        message_type: VeranaPermissionMessageTypes.StartPermissionVP,
        tx_index: args.txIndex ?? 0,
        message_index: args.messageIndex ?? 0,
        sender: otherDid,
        related_dids: args.relatedDids,
        entity_type: "Permission",
        entity_id: "42",
      },
    });
  }

  afterEach(async () => {
    if (txHashes.length > 0) {
      await knex("indexer_events").whereIn("tx_hash", txHashes).delete();
      const txIds = await knex("transaction").whereIn("hash", txHashes).pluck("id");
      if (txIds.length > 0) {
        await knex("event_attribute").whereIn("tx_id", txIds).delete();
        await knex("event").whereIn("tx_id", txIds).delete();
      }
      if (txIds.length > 0) await knex("transaction_message").whereIn("tx_id", txIds).delete();
      await knex("transaction").whereIn("hash", txHashes).delete();
      txHashes.length = 0;
    }

    if (heights.length > 0) {
      await knex("block").whereIn("height", heights).delete();
      heights.length = 0;
    }
  });

  it("replays events after a block height for one DID", async () => {
    await insertBlock(baseHeight);
    await insertBlock(baseHeight + 1);
    await insertTxMessage({ height: baseHeight, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-before` });
    await insertTxMessage({ height: baseHeight + 1, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-after` });

    await persistIndexerEventsForBlock(baseHeight);
    await persistIndexerEventsForBlock(baseHeight + 1);

    const events = await listIndexerEvents({ did, afterBlockHeight: baseHeight, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      did,
      block_height: baseHeight + 1,
      tx_hash: `tx-${runId}-after`,
      type: "indexer-event",
    });
  });

  it("returns deterministic replay ordering", async () => {
    await insertBlock(baseHeight + 10);
    await insertTxMessage({ height: baseHeight + 10, txIndex: 1, messageIndex: 0, hash: `tx-${runId}-order-2` });
    await insertTxMessage({ height: baseHeight + 10, txIndex: 0, messageIndex: 1, hash: `tx-${runId}-order-1b` });
    await insertTxMessage({ height: baseHeight + 10, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-order-1a` });

    await persistIndexerEventsForBlock(baseHeight + 10);

    const events = await listIndexerEvents({ did, afterBlockHeight: baseHeight + 9, limit: 10 });
    expect(events.map((event) => event.tx_hash)).toEqual([
      `tx-${runId}-order-1a`,
      `tx-${runId}-order-1b`,
      `tx-${runId}-order-2`,
    ]);
  });

  it("persists one event with all affected DIDs and protects duplicate inserts", async () => {
    await insertBlock(baseHeight + 20);
    await insertTxMessage({
      height: baseHeight + 20,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-multi-did`,
      content: { id: 42, applicant: did, validator: otherDid, nested: { duplicate: did } },
    });

    const firstPersist = await persistIndexerEventsForBlock(baseHeight + 20);
    const secondPersist = await persistIndexerEventsForBlock(baseHeight + 20);
    const rows = await knex("indexer_events").where("tx_hash", `tx-${runId}-multi-did`).orderBy("did", "asc");

    expect(firstPersist).toHaveLength(1);
    expect(firstPersist[0].did).toBe(did);
    expect(firstPersist[0].payload.related_dids).toEqual([did, otherDid]);
    expect(secondPersist).toEqual([]);
    expect(rows).toHaveLength(1);
    expect(rows[0].did).toBe(did);
  });

  it("matches persisted events by event.did", async () => {
    const height = baseHeight + 30;
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height,
      txHash: `tx-${runId}-match-did`,
    });

    const events = await listIndexerEvents({ did, afterBlockHeight: height - 1, limit: 10 });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ did, tx_hash: `tx-${runId}-match-did` });
  });

  it("matches persisted events by payload.related_dids", async () => {
    const height = baseHeight + 40;
    const relatedDid = `did:web:indexer-events-related-${runId}.example`;
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [relatedDid],
      height,
      txHash: `tx-${runId}-related-dids`,
    });

    const events = await listIndexerEvents({ did: relatedDid, afterBlockHeight: height - 1, limit: 10 });

    expect(events).toHaveLength(1);
    expect(events[0].did).toBe(otherDid);
    expect(events[0].payload.related_dids).toContain(relatedDid);
  });

  it("normalizes URL-encoded DID input before matching", async () => {
    const height = baseHeight + 50;
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height,
      txHash: `tx-${runId}-encoded-did`,
    });

    const events = await listIndexerEvents({
      did: encodeURIComponent(` ${did} `),
      afterBlockHeight: 0,
      limit: 10,
    });

    expect(events.map((event) => event.tx_hash)).toContain(`tx-${runId}-encoded-did`);
  });

  it("after_block_height excludes old events", async () => {
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 60,
      txHash: `tx-${runId}-old-height`,
    });
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 61,
      txHash: `tx-${runId}-new-height`,
    });

    const events = await listIndexerEvents({ did, afterBlockHeight: baseHeight + 60, limit: 10 });

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-new-height`]);
  });

  it("applies limit after deterministic ordering", async () => {
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 70,
      txHash: `tx-${runId}-limit-1`,
      txIndex: 0,
    });
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 70,
      txHash: `tx-${runId}-limit-2`,
      txIndex: 1,
    });

    const events = await listIndexerEvents({ did, afterBlockHeight: baseHeight + 69, limit: 1 });

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-limit-1`]);
  });

  it("returns an empty array for an unknown DID", async () => {
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 80,
      txHash: `tx-${runId}-unknown-did-control`,
    });

    const events = await listIndexerEvents({
      did: `did:web:indexer-events-missing-${runId}.example`,
      afterBlockHeight: 0,
      limit: 10,
    });

    expect(events).toEqual([]);
  });

  it("does not reconstruct unpersisted historical transaction events in the request path", async () => {
    const height = baseHeight + 90;
    const unpersistedDid = `did:web:indexer-events-unpersisted-${runId}.example`;
    await insertBlock(height);
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-unpersisted-history`,
      sender: otherDid,
      content: { id: 42, applicant: unpersistedDid },
    });

    const events = await listIndexerEvents({ did: unpersistedDid, afterBlockHeight: height - 1, limit: 10 });

    expect(events).toEqual([]);
  });

  it("does not leak unrelated rows when after_block_height is combined with related_dids matching", async () => {
    const height = baseHeight + 100;
    const relatedDid = `did:web:indexer-events-grouping-${runId}.example`;
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [relatedDid],
      height,
      txHash: `tx-${runId}-grouping-match`,
    });
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [relatedDid],
      height: height - 50,
      txHash: `tx-${runId}-grouping-too-old`,
    });
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [`did:web:indexer-events-unrelated-${runId}.example`],
      height: height + 1,
      txHash: `tx-${runId}-grouping-unrelated`,
    });

    const events = await listIndexerEvents({ did: relatedDid, afterBlockHeight: height - 1, limit: 10 });

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-grouping-match`]);
  });

  it("StartPermissionVP returns entity_id from direct content.id", async () => {
    await insertBlock(baseHeight + 30);
    await insertTxMessage({
      height: baseHeight + 30,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-start-direct-id`,
      content: { id: 101, did }, 
      messageType: VeranaPermissionMessageTypes.StartPermissionVP,
      txResponse: {
        events: [
          { type: "start_permission_vp", attributes: [{ key: "msg_index", value: "0" }, { key: "permission_id", value: "202" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(baseHeight + 30);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 30, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBe("202");
  });

  it("StartPermissionVP leaves entity_id empty when tx-local data has none", async () => {
    await insertBlock(baseHeight + 40);
    await insertTxMessage({
      height: baseHeight + 40,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-start-no-id`,
      content: { did, validator_perm_id: 555 },
      messageType: VeranaPermissionMessageTypes.StartPermissionVP,
    });

    await persistIndexerEventsForBlock(baseHeight + 40);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 40, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBeUndefined();
  });

  it("StartPermissionVP does not use validator_perm_id as entity_id", async () => {
    await insertBlock(baseHeight + 41);
    await insertTxMessage({
      height: baseHeight + 41,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-start-vp-validator-only`,
      content: { did, validator_perm_id: 555 }, // ignored
      messageType: VeranaPermissionMessageTypes.StartPermissionVP,
      txResponse: {
        events: [
          { type: "start_permission_vp", attributes: [{ key: "msg_index", value: "0" }, { key: "validator_perm_id", value: "555" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(baseHeight + 41);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 41, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBeUndefined();
  });

  it("Missing domain event leaves entity_id empty (even if content has id)", async () => {
    await insertBlock(baseHeight + 42);
    await insertTxMessage({
      height: baseHeight + 42,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-missing-domain-event`,
      content: { did, id: 999 },
      messageType: VeranaTrustRegistryMessageTypes.CreateTrustRegistry,
      txResponse: {
        events: [
          { type: "message", attributes: [{ key: "msg_index", value: "0" }, { key: "trust_registry_id", value: "123" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(baseHeight + 42);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 42, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBeUndefined();
  });

  it("CreateTrustRegistry resolves entity_id from scoped tx event attributes", async () => {
    await insertBlock(baseHeight + 50);
    const hash = `tx-${runId}-create-tr-scoped`;
    await insertTxMessage({
      height: baseHeight + 50,
      txIndex: 0,
      messageIndex: 0,
      hash,
      content: { did, id: 999 }, 
      messageType: VeranaTrustRegistryMessageTypes.CreateTrustRegistry,
      txResponse: {
        events: [
          { type: "create_trust_registry", attributes: [{ key: "msg_index", value: "0" }, { key: "trust_registry_id", value: "123" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(baseHeight + 50);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 50, limit: 10 });
    expect(events[0].payload.entity_id).toBe("123");
  });

  it("CreateCredentialSchema resolves entity_id from scoped tx event attributes", async () => {
    await insertBlock(baseHeight + 60);
    const hash = `tx-${runId}-create-cs-scoped`;
    await insertTxMessage({
      height: baseHeight + 60,
      txIndex: 0,
      messageIndex: 0,
      hash,
      content: { did, id: 999 },
      messageType: VeranaCredentialSchemaMessageTypes.CreateCredentialSchema,
      txResponse: {
        events: [
          { type: "create_credential_schema", attributes: [{ key: "msg_index", value: "0" }, { key: "credential_schema_id", value: "777" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(baseHeight + 60);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 60, limit: 10 });
    expect(events[0].payload.entity_id).toBe("777");
  });

  it("CreateRootPermission resolves entity_id from scoped tx event attributes", async () => {
    await insertBlock(baseHeight + 70);
    const hash = `tx-${runId}-create-root-perm-scoped`;
    await insertTxMessage({
      height: baseHeight + 70,
      txIndex: 0,
      messageIndex: 0,
      hash,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.CreateRootPermission,
      txResponse: {
        events: [
          { type: "create_root_permission", attributes: [{ key: "msg_index", value: "0" }, { key: "root_permission_id", value: "888" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(baseHeight + 70);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 70, limit: 10 });
    expect(events[0].payload.entity_id).toBe("888");
  });

  it("UpdateTrustRegistry resolves entity_id from update_trust_registry.trust_registry_id", async () => {
    await insertBlock(baseHeight + 80);
    await insertTxMessage({
      height: baseHeight + 80,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-update-tr`,
      content: { did, id: 999 },
      messageType: VeranaTrustRegistryMessageTypes.UpdateTrustRegistry,
      txResponse: {
        events: [
          { type: "update_trust_registry", attributes: [{ key: "msg_index", value: "0" }, { key: "trust_registry_id", value: "321" }] },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 80);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 80, limit: 10 });
    expect(events[0].payload.entity_id).toBe("321");
  });

  it("ArchiveTrustRegistry resolves entity_id from archive_trust_registry.trust_registry_id", async () => {
    await insertBlock(baseHeight + 81);
    await insertTxMessage({
      height: baseHeight + 81,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-archive-tr`,
      content: { did, id: 999 },
      messageType: VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry,
      txResponse: {
        events: [
          { type: "archive_trust_registry", attributes: [{ key: "msg_index", value: "0" }, { key: "trust_registry_id", value: "322" }] },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 81);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 81, limit: 10 });
    expect(events[0].payload.entity_id).toBe("322");
  });

  it("UpdateCredentialSchema resolves entity_id from update_credential_schema.credential_schema_id", async () => {
    await insertBlock(baseHeight + 82);
    await insertTxMessage({
      height: baseHeight + 82,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-update-cs`,
      content: { did, id: 999 },
      messageType: VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema,
      txResponse: {
        events: [
          { type: "update_credential_schema", attributes: [{ key: "msg_index", value: "0" }, { key: "credential_schema_id", value: "778" }] },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 82);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 82, limit: 10 });
    expect(events[0].payload.entity_id).toBe("778");
  });

  it("ArchiveCredentialSchema resolves entity_id from archive_credential_schema.credential_schema_id", async () => {
    await insertBlock(baseHeight + 83);
    await insertTxMessage({
      height: baseHeight + 83,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-archive-cs`,
      content: { did, id: 999 },
      messageType: VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema,
      txResponse: {
        events: [
          { type: "archive_credential_schema", attributes: [{ key: "msg_index", value: "0" }, { key: "credential_schema_id", value: "779" }] },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 83);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 83, limit: 10 });
    expect(events[0].payload.entity_id).toBe("779");
  });

  it("SetPermissionVPToValidated resolves entity_id from set_permission_vp_to_validated.permission_id", async () => {
    await insertBlock(baseHeight + 84);
    await insertTxMessage({
      height: baseHeight + 84,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-perm-validated`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SetPermissionVPToValidated,
      txResponse: {
        events: [
          { type: "set_permission_vp_to_validated", attributes: [{ key: "msg_index", value: "0" }, { key: "permission_id", value: "900" }] },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 84);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 84, limit: 10 });
    expect(events[0].payload.entity_id).toBe("900");
  });

  it("self_create_permission uses permission_id/self_permission_id/perm_id/id preference set (same emitted id ok)", async () => {
    await insertBlock(baseHeight + 85);
    await insertTxMessage({
      height: baseHeight + 85,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          {
            type: "self_create_permission",
            attributes: [
              { key: "msg_index", value: "0" },
              { key: "self_permission_id", value: "901" },
              { key: "permission_id", value: "901" },
            ],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 85);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 85, limit: 10 });
    expect(events[0].payload.entity_id).toBe("901");
  });

  it("SelfCreatePermission resolves from self_create_permission.permission_id", async () => {
    await insertBlock(baseHeight + 90);
    await insertTxMessage({
      height: baseHeight + 90,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm-permission-id`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          {
            type: "self_create_permission",
            attributes: [{ key: "msg_index", value: "0" }, { key: "permission_id", value: "910" }],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 90);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 90, limit: 10 });
    expect(events[0].payload.entity_id).toBe("910");
  });

  it("SelfCreatePermission resolves from self_create_permission.self_permission_id", async () => {
    await insertBlock(baseHeight + 91);
    await insertTxMessage({
      height: baseHeight + 91,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm-self-permission-id`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          {
            type: "self_create_permission",
            attributes: [{ key: "msg_index", value: "0" }, { key: "self_permission_id", value: "911" }],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 91);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 91, limit: 10 });
    expect(events[0].payload.entity_id).toBe("911");
  });

  it("SelfCreatePermission resolves from self_create_permission.perm_id", async () => {
    await insertBlock(baseHeight + 92);
    await insertTxMessage({
      height: baseHeight + 92,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm-perm-id`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          {
            type: "self_create_permission",
            attributes: [{ key: "msg_index", value: "0" }, { key: "perm_id", value: "912" }],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 92);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 92, limit: 10 });
    expect(events[0].payload.entity_id).toBe("912");
  });

  it("SelfCreatePermission resolves from self_create_permission.id", async () => {
    await insertBlock(baseHeight + 93);
    await insertTxMessage({
      height: baseHeight + 93,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm-id`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          {
            type: "self_create_permission",
            attributes: [{ key: "msg_index", value: "0" }, { key: "id", value: "913" }],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 93);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 93, limit: 10 });
    expect(events[0].payload.entity_id).toBe("913");
  });

  it("SelfCreatePermission resolves when event name differs but same msg_index has one allowed ID key", async () => {
    await insertBlock(baseHeight + 94);
    await insertTxMessage({
      height: baseHeight + 94,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm-alt-event`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          // not in preferred eventTypes, but same msg_index and has allowed id key
          {
            type: "permission_created",
            attributes: [{ key: "msg_index", value: "0" }, { key: "permission_id", value: "914" }],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 94);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 94, limit: 10 });
    expect(events[0].payload.entity_id).toBe("914");
  });

  it("SelfCreatePermission does not use schema_id / trust_registry_id / tr_id / validator_perm_id", async () => {
    await insertBlock(baseHeight + 95);
    await insertTxMessage({
      height: baseHeight + 95,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-self-create-perm-unrelated-ids`,
      content: { did, id: 999 },
      messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
      txResponse: {
        events: [
          {
            type: "self_create_permission",
            attributes: [
              { key: "msg_index", value: "0" },
              { key: "schema_id", value: "1" },
              { key: "trust_registry_id", value: "2" },
              { key: "tr_id", value: "3" },
              { key: "validator_perm_id", value: "4" },
            ],
          },
        ],
      },
    });
    await persistIndexerEventsForBlock(baseHeight + 95);
    const events = await listIndexerEvents({ did, blockHeight: baseHeight + 95, limit: 10 });
    expect(events[0].payload.entity_id).toBeUndefined();
  });

  it("conflicting emitted IDs warns and leaves entity_id empty", async () => {
    const prevLogger = (global as any).logger;
    try {
      (global as any).logger = { warn: jest.fn() };
      await insertBlock(baseHeight + 86);
      await insertTxMessage({
        height: baseHeight + 86,
        txIndex: 0,
        messageIndex: 0,
        hash: `tx-${runId}-conflict`,
        content: { did, id: 999 },
        messageType: VeranaPermissionMessageTypes.SelfCreatePermission,
        txResponse: {
          events: [
            {
              type: "self_create_permission",
              attributes: [
                { key: "msg_index", value: "0" },
                { key: "permission_id", value: "902" },
                { key: "self_permission_id", value: "903" },
              ],
            },
          ],
        },
      });
      await persistIndexerEventsForBlock(baseHeight + 86);
      const events = await listIndexerEvents({ did, blockHeight: baseHeight + 86, limit: 10 });
      expect(events[0].payload.entity_id).toBeUndefined();
      expect((global as any).logger.warn).toHaveBeenCalled();
    } finally {
      (global as any).logger = prevLogger;
    }
  });

  it("msg.content.id is ignored when tx_response.events has the real ID", async () => {
    const height = baseHeight + 106;
    await insertBlock(height);
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-content-id-ignored`,
      content: { did, id: 999 },
      messageType: VeranaTrustRegistryMessageTypes.CreateTrustRegistry,
      txResponse: {
        events: [
          { type: "create_trust_registry", attributes: [{ key: "msg_index", value: "0" }, { key: "trust_registry_id", value: "123" }] },
        ],
      },
    });

    await persistIndexerEventsForBlock(height);
    const events = await listIndexerEvents({ did, blockHeight: height, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBe("123");
  });

  it("AddGovernanceFrameworkDocument does not wrongly use tr_id as entity_id", async () => {
    const height = baseHeight + 107;
    await insertBlock(height);
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-gf-doc-tr-only`,
      content: { did, tr_id: 55 },
      messageType: VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc,
      txResponse: {
        events: [
          {
            type: "add_governance_framework_document",
            attributes: [{ key: "msg_index", value: "0" }, { key: "tr_id", value: "55" }],
          },
        ],
      },
    });

    await persistIndexerEventsForBlock(height);
    const events = await listIndexerEvents({ did, blockHeight: height, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBe("55");
    expect(events[0].payload.tr_id).toBe("55");
  });

  it("AddGovernanceFrameworkDocument resolves entity_id from document-specific event keys", async () => {
    const height = baseHeight + 108;
    await insertBlock(height);
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-gf-doc-with-id`,
      content: { did, tr_id: 55 },
      messageType: VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc,
      txResponse: {
        events: [
          {
            type: "governance_framework_document_added",
            attributes: [
              { key: "msg_index", value: "0" },
              { key: "document_id", value: "700" },
              { key: "tr_id", value: "55" },
            ],
          },
        ],
      },
    });

    await persistIndexerEventsForBlock(height);
    const events = await listIndexerEvents({ did, blockHeight: height, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBe("700");
    expect(events[0].payload.tr_id).toBe("55");
  });

  it("IncreaseActiveGFVersion does not wrongly use tr_id as entity_id", async () => {
    const height = baseHeight + 109;
    await insertBlock(height);
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-gf-version-tr-only`,
      content: { did, tr_id: 55 },
      messageType: VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion,
      txResponse: {
        events: [
          {
            type: "increase_active_governance_framework_version",
            attributes: [{ key: "msg_index", value: "0" }, { key: "tr_id", value: "55" }],
          },
        ],
      },
    });

    await persistIndexerEventsForBlock(height);
    const events = await listIndexerEvents({ did, blockHeight: height, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBe("55");
    expect(events[0].payload.tr_id).toBe("55");
  });

  it("IncreaseActiveGFVersion resolves entity_id from version-specific event keys", async () => {
    const height = baseHeight + 110;
    await insertBlock(height);
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-gf-version-with-id`,
      content: { did, tr_id: 55 },
      messageType: VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion,
      txResponse: {
        events: [
          {
            type: "governance_framework_version_created",
            attributes: [
              { key: "msg_index", value: "0" },
              { key: "active_version", value: "701" },
              { key: "tr_id", value: "55" },
            ],
          },
        ],
      },
    });

    await persistIndexerEventsForBlock(height);
    const events = await listIndexerEvents({ did, blockHeight: height, limit: 10 });
    expect(events).toHaveLength(1);
    expect(events[0].payload.entity_id).toBe("701");
    expect(events[0].payload.tr_id).toBe("55");
  });
});
