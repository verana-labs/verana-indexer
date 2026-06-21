import { ServiceBroker } from "moleculer";
import knex from "../../../../src/common/utils/db_connection";
import IndexerSnapshotService, { getDidSnapshotAtHeight } from "../../../../src/services/api/snapshot.service";

describe("IndexerSnapshotService snapshot endpoint", () => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const baseHeight = 7_500_000 + Math.floor(Math.random() * 10_000);
  const didA = `did:web:snapshot-a-${runId}.example`;
  const didB = `did:web:snapshot-b-${runId}.example`;
  const otherDid = `did:web:snapshot-other-${runId}.example`;

  const createdAt = new Date("2026-01-15T10:30:00Z");

  const inserted = {
    ecosystemIds: [] as number[],
    credentialSchemaIds: [] as number[],
    participantIds: [] as number[],
  };

  const createdTables: string[] = [];
  const columnInfoCache = new Map<string, Promise<Record<string, any>>>();

  async function getColumnInfo(table: string): Promise<Record<string, any>> {
    const cached = columnInfoCache.get(table);
    if (cached) return cached;
    const p = knex(table).columnInfo();
    columnInfoCache.set(table, p);
    return p;
  }

  async function insertRow(table: string, row: Record<string, any>): Promise<void> {
    const info = await getColumnInfo(table);
    const filtered: Record<string, any> = {};
    for (const [k, v] of Object.entries(row)) {
      if (Object.prototype.hasOwnProperty.call(info, k)) filtered[k] = v;
    }
    await knex(table).insert(filtered);
  }

  beforeAll(async () => {
    const hasTr = await knex.schema.hasTable("ecosystem");
    if (!hasTr) {
      await knex.schema.createTable("ecosystem", (table) => {
        table.increments("id").primary();
        table.string("did").notNullable();
        table.bigInteger("corporation_id").notNullable().defaultTo(0);
        table.timestamp("created").notNullable();
        table.timestamp("modified").notNullable();
        table.timestamp("archived").nullable();
        table.string("aka").nullable();
        table.string("language").notNullable();
        table.integer("active_version").nullable();
        table.bigInteger("participants").notNullable().defaultTo(0);
        table.bigInteger("active_schemas").notNullable().defaultTo(0);
        table.bigInteger("archived_schemas").notNullable().defaultTo(0);
        table.bigInteger("weight").notNullable().defaultTo(0);
        table.bigInteger("issued").notNullable().defaultTo(0);
        table.bigInteger("verified").notNullable().defaultTo(0);
        table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
        table.bigInteger("ecosystem_slashed_amount").notNullable().defaultTo(0);
        table.bigInteger("ecosystem_slashed_amount_repaid").notNullable().defaultTo(0);
        table.bigInteger("network_slash_events").notNullable().defaultTo(0);
        table.bigInteger("network_slashed_amount").notNullable().defaultTo(0);
        table.bigInteger("network_slashed_amount_repaid").notNullable().defaultTo(0);
        table.bigInteger("height").notNullable().defaultTo(0);
      });
      createdTables.push("ecosystem");
    }

    const hasCs = await knex.schema.hasTable("credential_schemas");
    if (!hasCs) {
      await knex.schema.createTable("credential_schemas", (table) => {
        table.increments("id").primary();
        table.integer("ecosystem_id").notNullable();
        table.text("json_schema").nullable();
        table.boolean("is_active").notNullable().defaultTo(true);
      });
      createdTables.push("credential_schemas");
    }

    const hasParticipants = await knex.schema.hasTable("participants");
    if (!hasParticipants) {
      await knex.schema.createTable("participants", (table) => {
        table.increments("id").primary();
        table.integer("schema_id").notNullable();
        table.string("role").nullable();
        table.string("did").nullable();
        table.bigInteger("corporation_id").notNullable().defaultTo(0);
      });
      createdTables.push("participants");
    }
  });

  afterAll(async () => {
    for (const table of createdTables.reverse()) {
      // eslint-disable-next-line no-await-in-loop
      await knex.schema.dropTableIfExists(table);
    }
  });

  async function insertEcosystem(args: { did: string; corporationId: number; height: number }): Promise<number> {
    const [idRow] = await knex("ecosystem")
      .insert({
      did: args.did,
      corporation_id: args.corporationId,
      created: createdAt,
      modified: createdAt,
      archived: null,
      aka: null,
      language: "en",
      active_version: null,
      participants: 0,
      active_schemas: 0,
      archived_schemas: 0,
      weight: 0,
      issued: 0,
      verified: 0,
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: 0,
      ecosystem_slashed_amount_repaid: 0,
      network_slash_events: 0,
      network_slashed_amount: 0,
      network_slashed_amount_repaid: 0,
      height: args.height,
    })
      .returning("id");
    const ecosystemId = Number(typeof idRow === "object" ? (idRow as any).id : idRow);
    inserted.ecosystemIds.push(ecosystemId);
    return ecosystemId;
  }

  async function insertCredentialSchema(args: { ecosystemId: number; schemaId: number }): Promise<number> {
    const [idRow] = await knex("credential_schemas")
      .insert(
        await (async () => {
          const base = {
            ecosystem_id: args.ecosystemId,
            json_schema: JSON.stringify({ $id: `schema-${args.schemaId}` }),
            is_active: true,

            issuer_grantor_validation_validity_period: 365,
            verifier_grantor_validation_validity_period: 365,
            issuer_validation_validity_period: 365,
            verifier_validation_validity_period: 365,
            holder_validation_validity_period: 365,

            issuer_onboarding_mode: "OPEN",
            verifier_onboarding_mode: "OPEN",
            holder_onboarding_mode: "PARTICIPANTLESS",
          };

          const info = await getColumnInfo("credential_schemas");
          const filtered: Record<string, any> = {};
          for (const [k, v] of Object.entries(base)) {
            if (Object.prototype.hasOwnProperty.call(info, k)) filtered[k] = v;
          }
          return filtered;
        })()
      )
      .returning("id");
    const schemaRowId = Number(typeof idRow === "object" ? (idRow as any).id : idRow);
    inserted.credentialSchemaIds.push(schemaRowId);
    return schemaRowId;
  }

  async function insertParticipant(args: { schemaId: number; did?: string | null; corporationId: number }): Promise<number> {
    const [idRow] = await knex("participants")
      .insert({
        schema_id: args.schemaId,
        role: "ISSUER",
        did: args.did ?? null,
        corporation_id: args.corporationId,
      })
      .returning("id");
    const participantRowId = Number(typeof idRow === "object" ? (idRow as any).id : idRow);
    inserted.participantIds.push(participantRowId);
    return participantRowId;
  }

  afterEach(async () => {
    if (inserted.participantIds.length > 0) {
      await knex("participants").whereIn("id", inserted.participantIds).delete();
      inserted.participantIds.length = 0;
    }
    if (inserted.credentialSchemaIds.length > 0) {
      await knex("credential_schemas").whereIn("id", inserted.credentialSchemaIds).delete();
      inserted.credentialSchemaIds.length = 0;
    }
    if (inserted.ecosystemIds.length > 0) {
      await knex("ecosystem").whereIn("id", inserted.ecosystemIds).delete();
      inserted.ecosystemIds.length = 0;
    }
  });

  it("returns 400 for missing did", async () => {
    const broker = new ServiceBroker({ logger: false });
    const svc = new IndexerSnapshotService(broker as any);
    (svc as any).logger = { error: () => {} };

    const ctx: any = { params: { block_height: 0 }, meta: {} };
    const res = await svc.getSnapshot(ctx);
    expect(res).toMatchObject({ code: 400 });
    expect(String(res.error)).toContain("Missing did");
  });

  it("returns 400 for invalid did", async () => {
    const broker = new ServiceBroker({ logger: false });
    const svc = new IndexerSnapshotService(broker as any);
    (svc as any).logger = { error: () => {} };

    const ctx: any = { params: { did: "not-a-did", block_height: 0 }, meta: {} };
    const res = await svc.getSnapshot(ctx);
    expect(res).toMatchObject({ code: 400 });
    expect(String(res.error)).toContain("Invalid did");
  });

  it("uses the block height from the At-Block-Height header (ctx.meta.blockHeight)", async () => {
    const broker = new ServiceBroker({ logger: false });
    const svc = new IndexerSnapshotService(broker as any);
    (svc as any).logger = { error: () => {} };

    await insertEcosystem({ did: didA, corporationId: 1, height: baseHeight });

    const ctx: any = { params: { did: didA }, meta: { blockHeight: baseHeight } };
    const res: any = await svc.getSnapshot(ctx);
    expect(res).toMatchObject({ did: didA, block_height: baseHeight });
    expect(res.count.ecosystems).toBe(1);
  });

  it("defaults to the latest indexed block when At-Block-Height header is omitted", async () => {
    const broker = new ServiceBroker({ logger: false });
    const svc = new IndexerSnapshotService(broker as any);
    (svc as any).logger = { error: () => {} };

    const ctx: any = { params: { did: didA }, meta: {} };
    const res: any = await svc.getSnapshot(ctx);
    expect(res.code).toBeUndefined();
    expect(res.did).toBe(didA);
    expect(Number.isInteger(res.block_height)).toBe(true);
    expect(res.block_height).toBeGreaterThanOrEqual(0);
  });

  it("returns empty arrays for unknown DID", async () => {
    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight });
    expect(snap).toMatchObject({
      did: didA,
      block_height: baseHeight,
      count: { ecosystems: 0, schemas: 0, participants: 0 },
    });
    expect(snap.ecosystems).toEqual([]);
    expect(snap.schemas).toEqual([]);
    expect(snap.participants).toEqual([]);
  });

  it("returns DID-linked snapshot objects from current tables", async () => {
    const schemaIdSeed = 88000000 + Math.floor(Math.random() * 100000);
    const ecosystemId = await insertEcosystem({ did: didA, corporationId: 1, height: baseHeight });
    const schemaRowId = await insertCredentialSchema({ schemaId: schemaIdSeed, ecosystemId });
    await insertParticipant({ schemaId: schemaRowId + 100000, did: didA, corporationId: 999 });

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight });
    expect(snap.count.ecosystems).toBe(1);
    expect(snap.count.schemas).toBe(1);
    expect(snap.count.participants).toBe(1);
    expect(snap.ecosystems[0]?.did).toBe(didA);
    expect(snap.participants[0]?.did).toBe(didA);
  });

  it("returns schema-linked participants", async () => {
    const schemaIdSeed = 88000000 + Math.floor(Math.random() * 100000);
    const ecosystemId = await insertEcosystem({ did: didA, corporationId: 1, height: baseHeight });
    const schemaRowId = await insertCredentialSchema({ schemaId: schemaIdSeed, ecosystemId });
    await insertParticipant({ schemaId: schemaRowId, did: otherDid, corporationId: 999 });

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight });
    expect(snap.count.participants).toBe(1);
    expect(snap.participants[0]?.schema_id).toBe(schemaRowId);
    expect(snap.participants[0]?.did).toBe(otherDid);
  });

  it("returns corporation-linked participants from derived corporation_id", async () => {
    const schemaIdSeed = 88000000 + Math.floor(Math.random() * 100000);
    const corporationId = 4242;
    const ecosystemId = await insertEcosystem({ did: didA, corporationId, height: baseHeight });
    const schemaRowId = await insertCredentialSchema({ schemaId: schemaIdSeed, ecosystemId });
    await insertParticipant({ schemaId: schemaRowId + 100000, did: null, corporationId });

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight });
    expect(snap.count.participants).toBe(1);
    expect(Number(snap.participants[0]?.corporation_id)).toBe(corporationId);
    expect(snap.participants[0]?.schema_id).not.toBe(schemaRowId);
  });
});

