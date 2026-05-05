import knex from "../../../../src/common/utils/db_connection";
import { VeranaPermissionMessageTypes } from "../../../../src/common/verana-message-types";
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
        data: {},
        index: args.txIndex,
      })
      .returning("id");

    await knex("transaction_message").insert({
      id: messageId,
      tx_id: typeof tx === "object" ? tx.id : tx,
      index: args.messageIndex,
      type: VeranaPermissionMessageTypes.StartPermissionVP,
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
});
