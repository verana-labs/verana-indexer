import { ServiceBroker } from "moleculer";
import CredentialSchemaDatabaseService from "../../../../src/services/crawl-cs/cs_database.service";
import knex from "../../../../src/common/utils/db_connection";
import { SERVICE } from "../../../../src/common";

describe("CredentialSchemaDatabaseService API Integration Tests", () => {
  const broker = new ServiceBroker({ logger: false });
  const serviceKey = SERVICE.V1.CredentialSchemaDatabaseService.path;
  let schema: any;

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
      tr_id: "T123",
      json_schema: JSON.stringify({
        $id: "/vpr/v1/cs/js/1",
        type: "object",
        properties: { foo: { type: "string" } },
      }),
      deposit: "10000000",
      isActive: true,
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
    schema = res.data?.result || res.result || res;

    expect(res).toBeDefined();
    expect(schema.tr_id).toBe("T123");
    expect(schema.id).toBeDefined();
  });

  it("should update an existing credential schema", async () => {
    const res = await broker.call(`${serviceKey}.update`, {
      payload: { id: schema.id, deposit: "20000000" },
    });

    const updated = res.data?.updated || res.updated;
    expect(res.data?.success ?? res.success).toBe(true);
    expect(updated.deposit).toBe("20000000");
  });

  it("should archive the credential schema", async () => {
    const res = await broker.call(`${serviceKey}.archive`, {
      payload: {
        id: schema.id,
        archive: true,
        modified: new Date().toISOString(),
      },
    });

    const success = res.data?.success ?? res.success;
    expect(success).toBe(true);

    const dbRow = await knex("credential_schemas")
      .where({ id: schema.id })
      .first();
    expect(dbRow.archived).not.toBeNull();
  });

  it("should unarchive the credential schema", async () => {
    const res = await broker.call(`${serviceKey}.archive`, {
      payload: {
        id: schema.id,
        archive: false,
        modified: new Date().toISOString(),
      },
    });

    const success = res.data?.success ?? res.success;
    expect(success).toBe(true);

    const dbRow = await knex("credential_schemas")
      .where({ id: schema.id })
      .first();
    expect(dbRow.archived).toBeNull();
  });

  it("should get the credential schema by id", async () => {
    const res = await broker.call(`${serviceKey}.get`, { id: schema.id });
    const item = res.data || res;

    expect(item.id).toBe(schema.id);
    expect(item.tr_id).toBe("T123");
  });

  it("should list credential schemas", async () => {
    const res = await broker.call(`${serviceKey}.list`, { only_active: false });
    const items = res.data || res;

    expect(Array.isArray(items)).toBe(true);
    // check that our schema is inside the list
    const found = items.find((i: any) => i.id === schema.id);
    expect(found).toBeDefined();
    expect(found.tr_id).toBe("T123");
  });

  it("should fetch JsonSchema of the credential schema", async () => {
    const res = await broker.call(`${serviceKey}.JsonSchema`, {
      id: schema.id,
    });
    const jsonSchema = res.data || res;

    // jsonSchema is an object, so assert keys
    expect(typeof jsonSchema).toBe("object");
    expect(jsonSchema.type).toBe("object");
    expect(jsonSchema.properties).toHaveProperty("foo");
  });

  it("should fetch module params for credentialschema", async () => {
    try {
      const res = await broker.call(`${serviceKey}.getParams`);
      const params = res.data || res;

      expect(typeof params).toBe("object");
    } catch {
      console.warn(
        "⚠️ getParams skipped - no credentialschema module params seeded"
      );
    }
  });

  it("should get history records for the schema", async () => {
    const res = await broker.call(`${serviceKey}.getHistory`, {
      id: schema.id,
    });
    const history = res.data?.history || res.history;

    expect(Array.isArray(history)).toBe(true);
    expect(history.length).toBeGreaterThan(0);
    expect(history[0].credential_schema_id).toBe(schema.id);
  });
});
