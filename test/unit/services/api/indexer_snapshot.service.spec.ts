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
    trustRegistryIds: [] as number[],
    credentialSchemaIds: [] as number[],
    permissionIds: [] as number[],
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
    const hasTr = await knex.schema.hasTable("trust_registry");
    if (!hasTr) {
      await knex.schema.createTable("trust_registry", (table) => {
        table.increments("id").primary();
        table.string("did").notNullable();
        table.string("corporation").notNullable();
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
      createdTables.push("trust_registry");
    }

    const hasCs = await knex.schema.hasTable("credential_schemas");
    if (!hasCs) {
      await knex.schema.createTable("credential_schemas", (table) => {
        table.increments("id").primary();
        table.integer("tr_id").notNullable();
        table.text("json_schema").nullable();
        table.boolean("is_active").notNullable().defaultTo(true);
      });
      createdTables.push("credential_schemas");
    }

    const hasPerms = await knex.schema.hasTable("permissions");
    if (!hasPerms) {
      await knex.schema.createTable("permissions", (table) => {
        table.increments("id").primary();
        table.integer("schema_id").notNullable();
        table.string("type").nullable();
        table.string("did").nullable();
        table.string("corporation").nullable();
      });
      createdTables.push("permissions");
    }
  });

  afterAll(async () => {
    for (const table of createdTables.reverse()) {
      // eslint-disable-next-line no-await-in-loop
      await knex.schema.dropTableIfExists(table);
    }
  });

  async function insertTrustRegistry(args: { did: string; corporation: string; height: number }): Promise<number> {
    const [idRow] = await knex("trust_registry")
      .insert({
      did: args.did,
      corporation: args.corporation,
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
    const trId = Number(typeof idRow === "object" ? (idRow as any).id : idRow);
    inserted.trustRegistryIds.push(trId);
    return trId;
  }

  async function insertCredentialSchema(args: { trId: number; schemaId: number }): Promise<number> {
    const [idRow] = await knex("credential_schemas")
      .insert(
        await (async () => {
          const base = {
            tr_id: args.trId,
            json_schema: JSON.stringify({ $id: `schema-${args.schemaId}` }),
            is_active: true,

            issuer_grantor_validation_validity_period: 365,
            verifier_grantor_validation_validity_period: 365,
            issuer_validation_validity_period: 365,
            verifier_validation_validity_period: 365,
            holder_validation_validity_period: 365,

            issuer_onboarding_mode: "OPEN",
            verifier_onboarding_mode: "OPEN",
            holder_onboarding_mode: "PERMISSIONLESS",
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

  async function insertPermission(args: { schemaId: number; did?: string | null; corporation: string }): Promise<number> {
    const [idRow] = await knex("permissions")
      .insert({
        schema_id: args.schemaId,
        type: "ISSUER",
        did: args.did ?? null,
        corporation: args.corporation,
      })
      .returning("id");
    const permRowId = Number(typeof idRow === "object" ? (idRow as any).id : idRow);
    inserted.permissionIds.push(permRowId);
    return permRowId;
  }

  afterEach(async () => {
    if (inserted.permissionIds.length > 0) {
      await knex("permissions").whereIn("id", inserted.permissionIds).delete();
      inserted.permissionIds.length = 0;
    }
    if (inserted.credentialSchemaIds.length > 0) {
      await knex("credential_schemas").whereIn("id", inserted.credentialSchemaIds).delete();
      inserted.credentialSchemaIds.length = 0;
    }
    if (inserted.trustRegistryIds.length > 0) {
      await knex("trust_registry").whereIn("id", inserted.trustRegistryIds).delete();
      inserted.trustRegistryIds.length = 0;
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

  it("returns 400 for missing block_height", async () => {
    const broker = new ServiceBroker({ logger: false });
    const svc = new IndexerSnapshotService(broker as any);
    (svc as any).logger = { error: () => {} };

    const ctx: any = { params: { did: didA }, meta: {} };
    const res = await svc.getSnapshot(ctx);
    expect(res).toMatchObject({ code: 400 });
    expect(String(res.error)).toContain("Missing block_height");
  });

  it("returns 400 for invalid block_height", async () => {
    const broker = new ServiceBroker({ logger: false });
    const svc = new IndexerSnapshotService(broker as any);
    (svc as any).logger = { error: () => {} };

    const ctx: any = { params: { did: didA, block_height: -1 }, meta: {} };
    const res = await svc.getSnapshot(ctx);
    expect(res).toMatchObject({ code: 400 });
    expect(String(res.error)).toContain("Invalid block_height");
  });

  it("returns empty arrays for unknown DID", async () => {
    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight });
    expect(snap).toMatchObject({
      did: didA,
      block_height: baseHeight,
      count: { trust_registries: 0, schemas: 0, permissions: 0 },
    });
    expect(snap.trust_registries).toEqual([]);
    expect(snap.schemas).toEqual([]);
    expect(snap.permissions).toEqual([]);
  });

  it("returns DID-linked snapshot objects from current tables (did + corporation linking)", async () => {
    const schemaIdSeed = 88000000 + Math.floor(Math.random() * 100000);
    const trId = await insertTrustRegistry({ did: didA, corporation: "cosmos1corp", height: baseHeight });
    const schemaRowId = await insertCredentialSchema({ schemaId: schemaIdSeed, trId });
    await insertPermission({ schemaId: schemaRowId, did: otherDid, corporation: "cosmos1corp" });

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight });
    expect(snap.count.trust_registries).toBe(1);
    expect(snap.count.schemas).toBe(1);
    expect(snap.count.permissions).toBe(1);
    expect(snap.trust_registries[0]?.did).toBe(didA);
  });
});

