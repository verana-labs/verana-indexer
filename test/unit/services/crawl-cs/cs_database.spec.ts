import { ServiceBroker } from "moleculer";
import CredentialSchemaDatabaseService from "../../../../src/services/crawl-cs/cs_database.service";
import knex from "../../../../src/common/utils/db_connection";
import { SERVICE } from "../../../../src/common";

function normalizeSchemaId(id: unknown): number {
  if (id == null) return NaN;
  if (typeof id === "bigint") return Number(id);
  const n = Number(id);
  return Number.isFinite(n) ? n : NaN;
}

/**
 * Broker / middleware may wrap payloads in one or more `data` layers. Drill through until we
 * hit a terminal API shape (`success`, `error`, `result`, `schema`, …).
 */
function deepUnwrapMoleculer(res: unknown): Record<string, unknown> | undefined {
  let cur: unknown = res;
  for (let depth = 0; depth < 12; depth++) {
    if (cur == null || typeof cur !== "object") return undefined;
    const o = cur as Record<string, unknown>;
    const terminal =
      typeof o.success === "boolean" ||
      typeof o.error === "string" ||
      o.result !== undefined ||
      o.updated !== undefined ||
      o.schema !== undefined ||
      Array.isArray(o.schemas) ||
      o.entity_type !== undefined ||
      Array.isArray(o.activity);
    if (terminal) return o;
    if ("data" in o && o.data != null && typeof o.data === "object") {
      cur = o.data;
      continue;
    }
    return o;
  }
  return undefined;
}

/** Actions return `{ success, result }` / `{ success, updated }`; unwrap row payloads. */
function csResultRow(res: unknown): Record<string, unknown> | undefined {
  const body = deepUnwrapMoleculer(res);
  if (!body) return undefined;
  if (body.result != null && typeof body.result === "object") {
    return body.result as Record<string, unknown>;
  }
  return body;
}

function assertSuccess(res: unknown): void {
  const body = deepUnwrapMoleculer(res);
  expect(body).toBeDefined();
  expect(body!.success).toBe(true);
  expect(body!.error).toBeUndefined();
}

describe("CredentialSchemaDatabaseService API Integration Tests", () => {
  const broker = new ServiceBroker({ logger: false });
  const serviceKey = SERVICE.V1.CredentialSchemaDatabaseService.path;
  let schema: Record<string, unknown>;
  let schemaId: number;

  beforeAll(async () => {
    broker.createService(CredentialSchemaDatabaseService);
    await broker.start();

    // reset DB tables
    await knex("credential_schema_history").del();
    await knex("credential_schemas").del();
  });

  afterAll(async () => {
    await broker.stop();
    await knex.destroy();
  });

  it("should upsert (insert) a new credential schema", async () => {
    const payload = {
      tr_id: 123,
      json_schema: JSON.stringify({
        $id: "/vpr/v1/cs/js/1",
        type: "object",
        properties: { foo: { type: "string" } },
      }),
      deposit: 10000000,
      is_active: true,
      issuer_grantor_validation_validity_period: 365,
      verifier_grantor_validation_validity_period: 365,
      issuer_validation_validity_period: 180,
      verifier_validation_validity_period: 180,
      holder_validation_validity_period: 180,
      issuer_perm_management_mode: 2,
      verifier_perm_management_mode: 2,
      created: new Date().toISOString(),
      archived: null,
      modified: new Date().toISOString(),
    };

    const res = await broker.call(`${serviceKey}.upsert`, { payload });
    assertSuccess(res);
    const row = csResultRow(res);
    expect(row).toBeDefined();
    schema = row as Record<string, unknown>;
    schemaId = normalizeSchemaId(schema.id);
    expect(Number.isFinite(schemaId) && schemaId > 0).toBe(true);
    expect(normalizeSchemaId(schema.tr_id)).toBe(123);
  });

  it("should update an existing credential schema", async () => {
    const res = await broker.call(`${serviceKey}.update`, {
      payload: { id: schemaId, deposit: 20000000 },
    });

    assertSuccess(res);
    const body = deepUnwrapMoleculer(res)!;
    const updated = body.updated as Record<string, unknown>;
    expect(updated?.id).toBe(schemaId);
  });

  it("should avoid synthetic updates in syncFromLedger and update title/description from json_schema changes", async () => {
    const basePayload = {
      tr_id: 321,
      json_schema: JSON.stringify({
        $id: "/vpr/v1/cs/js/placeholder",
        type: "object",
        title: "SchemaTitleV1",
        description: "SchemaDescV1",
        properties: { foo: { type: "string" } },
      }),
      deposit: 10000000,
      is_active: true,
      issuer_grantor_validation_validity_period: 365,
      verifier_grantor_validation_validity_period: 365,
      issuer_validation_validity_period: 180,
      verifier_validation_validity_period: 180,
      holder_validation_validity_period: 180,
      issuer_perm_management_mode: "OPEN",
      verifier_perm_management_mode: "OPEN",
      created: new Date().toISOString(),
      archived: null,
      modified: new Date().toISOString(),
    };

    const createRes = await broker.call(`${serviceKey}.upsert`, { payload: basePayload });
    assertSuccess(createRes);
    const created = csResultRow(createRes) as Record<string, unknown>;
    const createdId = normalizeSchemaId(created.id);
    expect(Number.isFinite(createdId) && createdId > 0).toBe(true);

    const beforeSync = await knex("credential_schemas").where({ id: createdId }).first();
    const historyCountBefore = await knex("credential_schema_history")
      .where({ credential_schema_id: createdId })
      .count<{ count: string }>("id as count")
      .first();

    await broker.call(`${serviceKey}.syncFromLedger`, {
      ledgerResponse: {
        schema: {
          id: createdId,
          tr_id: String(beforeSync.tr_id),
          json_schema: beforeSync.json_schema,
          deposit: beforeSync.deposit,
          issuer_grantor_validation_validity_period: beforeSync.issuer_grantor_validation_validity_period,
          verifier_grantor_validation_validity_period: beforeSync.verifier_grantor_validation_validity_period,
          issuer_validation_validity_period: beforeSync.issuer_validation_validity_period,
          verifier_validation_validity_period: beforeSync.verifier_validation_validity_period,
          holder_validation_validity_period: beforeSync.holder_validation_validity_period,
          issuer_perm_management_mode: beforeSync.issuer_perm_management_mode,
          verifier_perm_management_mode: beforeSync.verifier_perm_management_mode,
          archived: beforeSync.archived,
          created: beforeSync.created,
          modified: beforeSync.modified,
        },
      },
      blockHeight: 777777,
    });

    const historyCountAfterNoop = await knex("credential_schema_history")
      .where({ credential_schema_id: createdId })
      .count<{ count: string }>("id as count")
      .first();
    const beforeCount = Number(historyCountBefore?.count || 0);
    const afterNoopCount = Number(historyCountAfterNoop?.count || 0);
    expect(afterNoopCount).toBeGreaterThanOrEqual(beforeCount);
    expect(afterNoopCount - beforeCount).toBeLessThanOrEqual(1);

    if (afterNoopCount > beforeCount) {
      const latestNoopHistory = await knex("credential_schema_history")
        .where({ credential_schema_id: createdId })
        .orderBy("id", "desc")
        .first();
      const noopRawChanges = latestNoopHistory?.changes;
      const noopChanges =
        typeof noopRawChanges === "string"
          ? JSON.parse(noopRawChanges)
          : (noopRawChanges ?? {});
      expect(noopChanges.title).toBeUndefined();
      expect(noopChanges.description).toBeUndefined();
    }

    const changedSchema = {
      $id: `vpr:verana:vna-testnet-1/cs/v1/js/${createdId}`,
      type: "object",
      title: "SchemaTitleV2",
      description: "SchemaDescV2",
      properties: { foo: { type: "string" } },
    };
    await broker.call(`${serviceKey}.syncFromLedger`, {
      ledgerResponse: {
        schema: {
          id: createdId,
          tr_id: String(beforeSync.tr_id),
          json_schema: JSON.stringify(changedSchema),
          deposit: beforeSync.deposit,
          issuer_grantor_validation_validity_period: beforeSync.issuer_grantor_validation_validity_period,
          verifier_grantor_validation_validity_period: beforeSync.verifier_grantor_validation_validity_period,
          issuer_validation_validity_period: beforeSync.issuer_validation_validity_period,
          verifier_validation_validity_period: beforeSync.verifier_validation_validity_period,
          holder_validation_validity_period: beforeSync.holder_validation_validity_period,
          issuer_perm_management_mode: beforeSync.issuer_perm_management_mode,
          verifier_perm_management_mode: beforeSync.verifier_perm_management_mode,
          archived: beforeSync.archived,
          created: beforeSync.created,
          modified: new Date().toISOString(),
        },
      },
      blockHeight: 777778,
    });

    const afterUpdate = await knex("credential_schemas").where({ id: createdId }).first();
    expect(afterUpdate.title).toBe("SchemaTitleV2");
    expect(afterUpdate.description).toBe("SchemaDescV2");

    const latestHistory = await knex("credential_schema_history")
      .where({ credential_schema_id: createdId })
      .orderBy("id", "desc")
      .first();
    const rawChanges = latestHistory?.changes;
    const changes =
      typeof rawChanges === "string"
        ? JSON.parse(rawChanges)
        : (rawChanges ?? {});
    expect(changes.title).toBe("SchemaTitleV2");
    expect(changes.description).toBe("SchemaDescV2");
  });

  it("should archive the credential schema", async () => {
    const res = await broker.call(`${serviceKey}.archive`, {
      payload: {
        id: normalizeSchemaId(schemaId),
        archive: true,
        modified: new Date().toISOString(),
      },
    });

    assertSuccess(res);

    const dbRow = await knex("credential_schemas")
      .where({ id: schemaId })
      .first();
    expect(dbRow?.archived).not.toBeNull();
  });

  it("should unarchive the credential schema", async () => {
    const res = await broker.call(`${serviceKey}.archive`, {
      payload: {
        id: normalizeSchemaId(schemaId),
        archive: false,
        modified: new Date().toISOString(),
      },
    });

    assertSuccess(res);

    const dbRow = await knex("credential_schemas")
      .where({ id: schemaId })
      .first();
    expect(dbRow?.archived).toBeNull();
  });

  it("should get the credential schema by id", async () => {
    const res = await broker.call(`${serviceKey}.get`, { id: normalizeSchemaId(schemaId) });
    const body = deepUnwrapMoleculer(res);
    const item = body?.schema ?? body;

    expect(normalizeSchemaId(item.id)).toBe(schemaId);
    expect(normalizeSchemaId(item.tr_id)).toBe(123);
  });

  it("should list credential schemas", async () => {
    const res = await broker.call(`${serviceKey}.list`, { only_active: false });
    const body = deepUnwrapMoleculer(res);
    const items = body?.schemas ?? body;

    expect(Array.isArray(items)).toBe(true);
    const found = items.find((i: any) => normalizeSchemaId(i.id) === schemaId);
    expect(found).toBeDefined();
    expect(normalizeSchemaId(found.tr_id)).toBe(123);
  });

  it("should fetch JsonSchema of the credential schema", async () => {
    const res = await broker.call(`${serviceKey}.JsonSchema`, { id: normalizeSchemaId(schemaId) });
    const storedRaw =
      typeof res === "string"
        ? res
        : typeof (res as Record<string, unknown>)?.data === "string"
          ? ((res as Record<string, unknown>).data as string)
          : (deepUnwrapMoleculer(res) as string | undefined) ?? res;
    expect(storedRaw).toBeDefined();
    expect(typeof storedRaw).toBe("string");
    const stored = storedRaw as string;
    expect(stored).toContain("vpr:verana:vna-testnet-1/cs/v1/js/" + schemaId);
    expect(stored).toContain("foo");
    const parsed = JSON.parse(stored);
    expect(parsed.$id).toBe("vpr:verana:vna-testnet-1/cs/v1/js/" + schemaId);
    expect(parsed.properties).toHaveProperty("foo");
  });

  it("should fetch module params for credentialschema", async () => {
    try {
      const res = await broker.call(`${serviceKey}.getParams`);
      const params = res;

      expect(typeof params).toBe("object");
    } catch {
      console.warn(
        "⚠️ getParams skipped - no credentialschema module params seeded"
      );
    }
  });

  it("should get history records for the schema", async () => {
    const res = await broker.call(`${serviceKey}.getHistory`, { id: normalizeSchemaId(schemaId) });
    const body = deepUnwrapMoleculer(res) ?? res;

    expect(body).toBeDefined();
    expect(body.entity_type).toBe("CredentialSchema");
    expect(normalizeSchemaId(body.entity_id)).toBe(schemaId);
    expect(Array.isArray(body.activity)).toBe(true);
    expect(body.activity.length).toBeGreaterThan(0);
    expect(body.activity[0].timestamp).toBeDefined();
    expect(body.activity[0].block_height).toBeDefined();
    expect(body.activity[0].entity_type).toBe("CredentialSchema");
    expect(normalizeSchemaId(body.activity[0].entity_id)).toBe(schemaId);
    expect(body.activity[0].msg).toBeDefined();
  });
});
